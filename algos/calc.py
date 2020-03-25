import pandas as pd

from typing import Dict, List, Optional, Container, Union, cast, Callable

from xbrlxml.xbrlchapter import Node, CalcChapter, DimChapter
from algos.scheme import enum
from xbrlxml.xbrlfileparser import TContextAsDictDim

TErrors = Dict[str, List[Union[str, float, None]]]


class Validator():
    def __init__(self,
                 threshold: float,
                 none_sum_err: bool,
                 none_val_err: bool):
        self.thres = threshold
        self.none_sum_err = none_sum_err
        self.none_val_err = none_val_err
        self.reset()

    def reset(self) -> None:
        self.errors: TErrors = {'name': [],
                                'value': [],
                                'value_sum': [],
                                'diff': []}

    def _loggin(self, tag: str,
                value: Optional[float],
                value_sum: Optional[float],
                diff: Optional[float]) -> None:
        self.errors['name'].append(tag)
        self.errors['value'].append(value)
        self.errors['value_sum'].append(value_sum)
        self.errors['diff'].append(diff)

    def check(self,
              value: Optional[float],
              value_sum: Optional[float],
              tag: str) -> bool:
        if value is None:
            if self.none_val_err:
                self._loggin(tag, None, None, None)
                return False
            else:
                return True

        if value_sum is None:
            if self.none_sum_err:
                self._loggin(tag, value, value_sum, None)
                return False
            else:
                return True

        if value == value_sum:
            return True

        diff = abs(value - value_sum)
        mean = (abs(value) + abs(value_sum)) / 2
        if (mean > 0 and diff / mean > self.thres):
            self._loggin(tag, value, value_sum, diff)
            return False

        return True


def calc_fact(node: Node,
              facts: Dict[str, float],
              err: Optional[Validator] = None,
              repair: bool = False) -> Optional[float]:

    value = facts.get(node.name, None)
    s = []
    for child in node.children.values():
        v = calc_fact(child, facts, err, repair)
        if v is not None:
            s.append(v * child.getweight())
    if s:
        value_sum = sum(s)
    elif not node.children:
        value_sum = value
    else:
        value_sum = None

    if err is not None:
        err.check(value, value_sum, node.name)

    if value is None and repair:
        if value_sum is not None:
            facts[node.name] = value_sum
            return value_sum

    return value


def calc_chapter(chapter: CalcChapter,
                 facts: Dict[str, float],
                 err: Optional[Validator] = None,
                 repair: bool = False) -> Dict[str, float]:
    values = {}
    for child in chapter.nodes.values():
        if child.parent is not None:
            continue
        v = calc_fact(child, facts, err, repair)
        if v is not None:
            values[child.name] = v

    return values


def calc_from_dim(name: str,
                  context: str,
                  dfacts: pd.DataFrame,
                  contexts,
                  pres: DimChapter) -> pd.DataFrame:
    "unittested"

    chapter_dims = set([(d, m) for [d, m] in pres.dimmembers()])
    context_dims = set([(d, m) for d, m in
                        zip(contexts[context].dim,
                            contexts[context].member)])
    dims = chapter_dims.difference(context_dims)
    dims = pd.DataFrame(dims, columns=['d', 'm'])

    df = dfacts[dfacts['name'] == name]
    fact_contexts = list(df['context'].unique())

    cntx: TContextAsDictDim = []
    for c in contexts.values():
        if ((c.contextid in fact_contexts) and
                (c.axises() == contexts[context].axises() + 1)):
            cntx.extend(c.asdictdim())
    cntx = pd.DataFrame(cntx)

    columns = ['name', 'tag', 'version', 'sdate', 'edate', 'uom']

    if cntx.shape[0] == 0:
        return pd.DataFrame(columns=columns + ['value'])

    cntx = pd.merge(df, cntx,
                    left_on='context', right_on='context',
                    suffixes=('', '_y'))
    cntx = pd.merge(cntx, dims,
                    left_on=['dim', 'member'], right_on=['d', 'm'])
    cntx.fillna(value={'sdate': -1}, inplace=True)

    g = cntx.groupby(columns + ['dim'])['value'].sum().reset_index()
    g['sdate'] = g['sdate'].apply(lambda x: None if x == -1 else x)
    return g[columns + ['value']]


def facts_to_dict(dfacts: pd.DataFrame,
                  context: str,
                  names: Container[str]) -> Dict[str, float]:
    df = dfacts[(dfacts['context'] == context) &
                (dfacts['name'].isin(names))]
    return cast(Dict[str, float], df.set_index('name')['value'].to_dict())


def find_missing(chapter: CalcChapter,
                 facts: Dict[str, float],
                 err: Validator) -> List[str]:
    retval: List[str] = []
    for name in set(cast(List[str], err.errors['name'])):
        children = set([c for [c] in enum(chapter.nodes[name],
                                          outpattern='c')])
        missing = children.difference(facts.keys())
        retval.extend(missing)

    return retval


def calc_indicator(
        node: Node,
        nums: Dict[str, float]) -> Optional[float]:

    value: Optional[float] = None

    for child in node.children.values():
        child_value = calc_indicator(child, nums)
        child_weight = child.getweight()
        if child_value is None:
            child_value = nums.get(child.name, None)
        if child_value is not None:
            if value:
                value += child_value * child_weight
            else:
                value = child_value * child_weight
    if value is None:
        value = nums.get(node.name, None)
    return value
