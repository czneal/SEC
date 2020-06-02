import json
import pandas as pd

from typing import Dict, List, Set, cast, Optional, Any

import recon.isc as ic
import logs

from settings import Settings
from xbrlxml.xbrlchapter import CalcChapter, Node

from mysqlio.readers import MySQLReports
from mysqlio.writers import MySQLWriter
from mysqlio.basicio import MySQLTable
from mysqlio.indio import MySQLIndicatorFeeder, Facts
from abstractions import Worker
from algos.scheme import enum
from algos.calc import calc_chapter, Validator
from algos.xbrljson import dumps, loads
from utils import ProgressBar


class NewStrcturesWriter(MySQLWriter):
    def __init__(self):
        super().__init__()

        self.mg_structures = MySQLTable('mg_structures', self.con)

    def write(self, obj: Any):
        self.write_to_table(self.mg_structures, obj)


class NewStrcturesWorker(Worker):
    def __init__(self, sheet: str):
        assert(sheet in {'is', 'bs', 'cf'})

        self.sheet = sheet
        self.b = ChapterBulder()

    def feed(self, obj: Any) -> Dict[str, Any]:
        try:
            cik = int(obj[0])
            adsh = str(obj[1])
        except (ValueError, IndexError, TypeError):
            raise AttributeError('obj must be Tuple[int, str]')

        chapter = self.build_chapter(adsh)

        return self.prepare_chapter(cik, adsh, self.sheet, chapter)

    def flush(self):
        pass

    def build_chapter(self, adsh: str) -> CalcChapter:
        self.b.read_tags(adsh)
        chapter = self.b.build_chapter()

        return chapter

    def prepare_chapter(self,
                        cik: int,
                        adsh: str,
                        sheet: str,
                        chapter: CalcChapter) -> Dict[str, Any]:

        return {
            'cik': cik,
            'adsh': adsh,
            'sheet': sheet,
            'structure': dumps({sheet: chapter})
        }


def calc_tree(df: pd.DataFrame) -> pd.DataFrame:
    offsets = sorted(df['offset'].unique())

    df.set_index('index', inplace=True)
    df.sort_index(inplace=True)

    for off in offsets[:-1]:
        f = df[df['offset'] == off]
        index = list(f.index)
        for i in range(len(index)):
            if i == len(index) - 1:
                sl = df.loc[index[i]:]
            else:
                sl = df.loc[index[i]: index[i + 1] - 1]
            if sl.shape[0] <= 1:
                continue

            summ = (sl['weight'] * sl[f'level_{off+1}']).sum()
            df.loc[index[i], f'level_{off + 1}'] = summ

    return df


def get_tree(chapter: CalcChapter, facts: Facts) -> pd.DataFrame:
    columns = ['parent', 'name', 'weight', 'offset']

    df = pd.DataFrame(enum(chapter, outpattern='pcwo'),
                      columns=columns).reset_index()

    f = pd.DataFrame(data=facts.items(), columns=['name', 'value'])

    df = df.merge(f, how='left', left_on='name', right_on='name')

    return df


def make_tree_structure(tree: pd.DataFrame) -> pd.DataFrame:
    for offset in tree['offset'].unique():
        tree[f'level_{offset}'] = tree[tree['offset'] == offset]['value']

    tree['name'] = tree.apply(axis=1, func=lambda row: "'" + '--'.join(
        ['' for e in range(int(row['offset']) + 1)]) + row['name'])

    return tree


class ChapterBulder():
    def __init__(self):
        self.reverse_tags: Dict[str, str] = {}
        self.is_builder = ic.income_st_builder(Settings.models_dir())
        if self.is_builder.error_message:
            raise AttributeError(self.is_builder.error_message)

        self.all_tags: List[str] = []
        self.adsh = ''

    def read_tags(self, adsh: str) -> None:
        self.reverse_tags = {"NetIncomeLoss": "us-gaap:NetIncomeLoss"}
        self.all_tags = []
        self.adsh = adsh

        r = MySQLReports()
        chapters = r.fetch_chapters(adsh)
        r.close()

        if 'is' not in chapters:
            return

        names: Set[str] = set()

        for p, c in enum(chapters['is'], outpattern='pc'):
            if '/' not in p:
                names.add(p)
            names.add(c)

        tags: Set[str] = set()
        for name in names:
            version, tag = name.split(':')
            n = self.reverse_tags.get(tag, '')
            if n == '':
                self.reverse_tags[tag] = name
            elif version == 'us-gaap':
                self.reverse_tags[tag] = name

            tags.add(tag)

        self.all_tags = list(tags)

    def build_chapter(self) -> CalcChapter:
        chapter = CalcChapter('', label='Income Statement')

        try:
            trees_out, nodes_out = self.is_builder.al_build_tree(self.all_tags)
            trees_out_weights = self.is_builder.al_add_weights(trees_out)
        except Exception as e:
            print(e)
            return chapter

        for tree in trees_out_weights:
            for c_tag, p_tag, w in tree:
                c_name = self.reverse_tags[c_tag]
                p_name = self.reverse_tags[p_tag]

                if c_name not in chapter.nodes:
                    c_node = Node(*c_name.split(':'))
                    c_node.arc['weight'] = w

                    chapter.nodes[c_name] = c_node
                else:
                    c_node = chapter.nodes[c_name]

                if p_name not in chapter.nodes:
                    p_node = Node(*p_name.split(':'))
                    chapter.nodes[p_name] = p_node
                else:
                    p_node = chapter.nodes[p_name]

                c_node.parent = p_node
                p_node.children[c_name] = c_node

        if nodes_out:
            logger = logs.get_logger(__name__)
            logger.set_state(state={'state': self.adsh})
            logger.warning('unused nodes', extra={'nodes': nodes_out})
            logger.revoke_state()

        return chapter


def validate_facts(facts: Facts, new_facts: Facts) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []

    for f, v in new_facts.items():
        if v != facts.get(f, None):
            messages.append({'msg': 'facts doesnt match',
                             'tag': f,
                             'value': facts[f],
                             'new_value': v})

    with open('outputs/is_calc_err.json', 'w') as f:
        f.write(json.dumps(messages, indent=2))

    return messages


def validate_chapter(chapter: CalcChapter,
                     facts: Facts) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []

    valid = Validator(threshold=0.02, none_sum_err=True, none_val_err=True)

    calc_chapter(chapter, facts, err=valid)

    with open('outputs/is_valid_err.json', 'w') as f:
        f.write(json.dumps(valid.errors, indent=2))

    return messages


def calc_middle_facts(chapter: CalcChapter, facts: Facts) -> Facts:
    leaf_facts: Dict[str, float] = {}

    for [c] in enum(chapter, outpattern='c', leaf=True):
        if c in facts:
            leaf_facts[c] = facts[c]

    calc_chapter(chapter, leaf_facts, repair=True)

    return leaf_facts


def load_chapter() -> CalcChapter:
    with open('outputs/is_chapter.json') as f:
        return cast(CalcChapter, loads(f.read())['is'])


def calculate():
    logs.configure('file', level=logs.logging.WARNING)

    r = MySQLIndicatorFeeder()
    ciks_adshs = r.fetch_snp500_ciks_adshs(newer_than=2016)
    r.close()

    worker = NewStrcturesWorker('is')
    writer = NewStrcturesWriter()

    pb = ProgressBar()
    pb.start(len(ciks_adshs))

    for cik, adsh in ciks_adshs:
        row = worker.feed((cik, adsh))
        writer.write(row)
        writer.flush()

        pb.measure()
        print('\r' + pb.message(), end='')

    print()


def main():
    adsh = '0000072971-20-000217'

    # chapter = buld_chapter(adsh=adsh)
    chapter = load_chapter()

    r = MySQLIndicatorFeeder()
    facts = r.fetch_facts(adsh=adsh)
    r.close()

    new_facts = calc_middle_facts(chapter, facts)
    messages = validate_facts(facts, new_facts)
    m = validate_chapter(chapter, new_facts)

    df = get_tree(chapter, new_facts)
    df = make_tree_structure(df)
    df = calc_tree(df)

    df.to_csv('outputs/is_table.csv', index=False)


if __name__ == '__main__':
    calculate()
