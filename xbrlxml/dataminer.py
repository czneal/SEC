# -*- coding: utf-8 -*-

import datetime
import re
import json
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Tuple, Union, Optional, cast

import pandas as pd

import algos.xbrljson
import algos.calc as c
import logs
from algos.scheme import enum
from utils import remove_root_dir
from xbrlxml.pickup import ChapterChooser, ChapterExtender, ContextChooser
from xbrlxml.xbrlexceptions import XbrlException
from xbrlxml.xbrlfile import XbrlFile
from xbrlxml.xbrlfileparser import Context
from xbrlxml.xbrlzip import XBRLZipPacket
from xbrlxml.xbrlrss import FileRecord
from xbrlxml.xbrlchapter import CalcChapter


class SharesFilter():
    share_types = re.compile(r'[A-Z,a-z,\:,\-,_]+Class[A-Z]{1}Member')

    @staticmethod
    def filter_shares_context(context: Context) -> bool:
        if len(context.dim) == 1:
            return True
        if (len(context.dim) == 2):
                # and SharesFilter.share_types.match(
                    # cast(str, context.member[1]))):
            return True
        return False


class DataMiner(metaclass=ABCMeta):
    chapter_sheets = ('bs', 'is', 'cf', 'se')
    calculated_sheets = ('bs', 'is', 'cf')

    def __init__(self):
        self.xbrlfile = XbrlFile()
        self.xbrlzip = XBRLZipPacket()

        self.sheets = ChapterChooser(self.xbrlfile)
        self.cntx = ContextChooser(self.xbrlfile)
        self.extender = ChapterExtender(self.xbrlfile)

        self.cik: int = 0
        self.adsh: str = ''
        self.zip_filename: str = ''

        self.extentions: List[Dict[str, str]] = []  # try to deprecate
        self.sheet_context: Dict[str, str] = {}  # {roleuri: context}
        self.numeric_facts: Optional[pd.DataFrame] = None
        self.shares_facts: Optional[pd.DataFrame] = None

    def _prerequisites(self, func: str) -> None:
        logger = logs.get_logger(__name__)
        if func == {'_mine_bs_parent_for_shares',
                    '_mine_se_for_shares'}:
            if self.xbrlfile.dfacts.shape[0] == 0:
                det = 'call {0} after xbrlfile.read_units_facts_fn'.format(
                    func)
                logger.error(msg='logic error', extra={'details': det})
                raise XbrlException('logic error')

    def _choose_main_sheets(self):
        self.sheets.choose()
        warning_info = {}
        if not self.sheets.mschapters:
            warning_info['details'] = "couldn't find any main sheet"
        else:
            for sheet in DataMiner.chapter_sheets:
                if sheet not in self.sheets.mschapters:
                    warning_info['details'] = "couldn't find main sheet"
                    warning_info[sheet] = 'chapter not found'
        if warning_info:
            logs.get_logger(__name__).warning(msg='parse main sheets',
                                              extra=warning_info)

    def _extend_calc(self):
        logger = logs.get_logger(__name__)
        for roleuri in self.sheets.mschapters.values():
            warnings = self.extender.find_extentions(roleuri)
            logger.warning(msg='exdender', extra={'warnings': warnings})

            self.extender.extend()
            self.extentions.extend(self.extender.extentions)

    def _find_main_sheet_contexts(self):
        self.sheet_context = {}
        self.extentions = []

        warning_info = {}
        for sheet in self.chapter_sheets:
            if sheet not in self.sheets.mschapters:
                continue
            roleuri = self.sheets.mschapters[sheet]
            context = self.cntx.choose(roleuri)
            if context is None:
                warning_info['details'] = 'context not found'
                warning_info[sheet] = roleuri
            else:
                self.extentions.append({'roleuri': roleuri,
                                        'context': context})
                self.sheet_context[roleuri] = context

        if warning_info:
            logs.get_logger(__name__).warning(msg='parse contexts',
                                              extra=warning_info)

    def _calculate(self):
        validator = c.Validator(threshold=0.02,
                                none_sum_err=True,
                                none_val_err=True)
        logger = logs.get_logger(__name__)
        new_facts_frames = []

        for sheet in DataMiner.calculated_sheets:
            if sheet not in self.sheets.mschapters:
                continue
            roleuri = self.sheets.mschapters[sheet]
            if roleuri not in self.sheet_context:
                continue

            context = self.sheet_context[roleuri]

            calc = self.xbrlfile.calc.get(roleuri, None)
            pres = self.xbrlfile.pres.get(roleuri, None)

            if calc is None or pres is None:
                logger.warning(msg='calculation failed',
                               extra={'details': 'unable calculate chapter without ' +
                                      'presentation or calculation scheme',
                                      sheet: roleuri})
                continue

            names = calc.gettags().union(pres.gettags())

            facts = c.facts_to_dict(dfacts=self.xbrlfile.dfacts,
                                    context=context,
                                    names=names)
            err = c.Validator(threshold=0.0,
                              none_sum_err=True,
                              none_val_err=True)
            c.calc_chapter(chapter=calc, facts=facts, err=err, repair=False)
            missing = c.find_missing(chapter=calc, facts=facts, err=err)

            for name in missing:
                df = c.calc_from_dim(name=name,
                                     context=context,
                                     contexts=self.xbrlfile.contexts,
                                     dfacts=self.xbrlfile.dfacts,
                                     pres=pres)
                if df.shape[0] == 1:
                    new_facts_frames.append(df)
                    facts[name] = df.iloc[0]['value']
                if df.shape[0] == 0:
                    extra = {'details': 'calc_from_dim returns none',
                             'fact': name,
                             'sheet': sheet,
                             'roleuri': roleuri}
                    logger.warning(msg='calculation failed',
                                   extra=extra)
                if df.shape[0] > 1:
                    extra = {'details': 'calc_from_dim returns more than ' +
                             'one value',
                             'fact': name,
                             'sheet': sheet,
                             'roleuri': roleuri}
                    logger.warning(msg='calculation failed',
                                   extra=extra)

            c.calc_chapter(chapter=calc, facts=facts, err=validator)

        if new_facts_frames:
            self.new_facts = pd.concat(new_facts_frames)

        return validator

    def _gather_numeric_facts(self):
        logger = logs.get_logger(__name__)

        frames = []

        for e in self.extentions:
            pres = self.xbrlfile.pres.get(e['roleuri'])
            calc = self.xbrlfile.calc.get(e['roleuri'], pres)
            if 'label' in e:
                tags = set()
                if e['label'] in pres.nodes:
                    tags.update([e for [e] in
                                 enum(structure=pres.nodes[e['label']],
                                      outpattern='c')])
                if e['label'] in calc.nodes:
                    tags.update([e for [e] in
                                 enum(structure=calc.nodes[e['label']],
                                      outpattern='c')])
            else:
                tags = set(pres.gettags()).union(set(calc.gettags()))

            frame = self.xbrlfile.dfacts
            frame = frame[(frame['name'].isin(tags)) &
                          (frame['context'] == e['context'])]
            frames.append(frame)

        if self.new_facts is not None and self.new_facts.shape[0] > 0:
            frames.append(self.new_facts)

        if not frames:
            if self.sheets.mschapters and self.xbrlfile.any_gaap_fact():
                logger.error(msg="couldnt find any facts to write")
                raise XbrlException("couldnt find any facts to write")
            else:
                logger.warning(msg="couldn't find any us-gaap fact")
                self.numeric_facts = None
                return

        self.numeric_facts = pd.concat(frames, ignore_index=True, sort=False)
        self.numeric_facts = self.numeric_facts[
            pd.notna(self.numeric_facts['value'])]

        # values first came in order self.chapter_sheets
        self.numeric_facts.drop_duplicates('name', keep='first', inplace=True)

        self.numeric_facts['adsh'] = self.adsh
        self.numeric_facts['fy'] = self.xbrlfile.fy
        self.numeric_facts['type'] = (
            self.numeric_facts['sdate'].apply(
                lambda x: 'I' if pd.isna(x) else 'D')
        )
        self.numeric_facts.rename(columns={'edate': 'ddate'}, inplace=True)

        if self.numeric_facts.shape[0] == 0:
            logger.warning(msg='only null facts found')
            self.numeric_facts = None

    def _mine_dei_for_shares(
            self) -> List[Tuple[str, str, int,
                                Optional[datetime.date],
                                datetime.date, str]]:
        shares = self.xbrlfile.dei.shares
        if not shares:
            logs.get_logger(__name__).warning('dei shares not found')
            return []

        data: List[Tuple[str, str, int,
                         Optional[datetime.date],
                         datetime.date, str]] = []

        shares_tag = 'EntityCommonStockSharesOutstanding'
        for shares_count, context_name in shares:
            context = self.xbrlfile.contexts[context_name]
            if SharesFilter.filter_shares_context(context):
                member = context.member[-1]
                if member is None:
                    member = ''
                data.append((self.adsh,
                             'dei:' + shares_tag,
                             shares_count,
                             context.sdate,
                             context.edate,
                             member))
        if not data:
            logs.get_logger(__name__).warning(
                'dei shares not found after filter')
            return data

        max_date = max(data, key=lambda x: x[4])[4]
        data = [d for d in filter(lambda x: x[4] == max_date, data)]
        return data

    def _mine_se_for_shares(self) -> List[List[Any]]:
        self._prerequisites("_mine_se_for_shares")

        logger = logs.get_logger(__name__)
        data: List[List[Any]] = []

        roleuri = self.sheets.mschapters.get('se', None)
        if roleuri is None:
            return data

        pres = self.xbrlfile.pres.get(roleuri, None)
        if pres is None:
            logger.warning(msg='se shares not found')
            return data

        context_name = self.cntx.choose(roleuri)
        if context_name is None:
            logger.warning(msg='se shares not found',
                           extra={'details': 'context for se not found'})
            return data

        tags = pres.gettags()
        context = self.xbrlfile.contexts[context_name]
        shares = self.xbrlfile.dfacts[
            self.xbrlfile.dfacts['name'].isin(tags)]
        shares = shares[shares['uom'] == 'shares']
        shares = shares[(shares['edate'] == context.edate) &
                        (shares['sdate'] == context.sdate)]

        for index, row in shares.iterrows():
            context = self.xbrlfile.contexts[row['context']]
            if SharesFilter.filter_shares_context(context):
                data.append([self.adsh,
                             row['name'],
                             row['value'],
                             row['sdate'],
                             row['edate'],
                             context.member[-1]])
        if not data:
            logger.warning(
                msg='se shares not found', extra={
                    'details': "se doesn't contains any shares data"})
        return data

    def _mine_bs_parent_for_shares(self) -> List[Tuple[str, str, int,
                                                       Optional[datetime.date],
                                                       datetime.date,
                                                       str]]:
        self._prerequisites("_mine_bs_parent_for_shares")

        logger = logs.get_logger(__name__)
        data: List[Tuple[str, str, int,
                         Optional[datetime.date],
                         datetime.date,
                         str]] = []

        bs_roleuri = self.sheets.mschapters.get('bs', None)
        if bs_roleuri is None:
            logger.warning(msg='bs parentical shares not found')
            return data

        xsd = self.xbrlfile.xsd
        bs_label = xsd[bs_roleuri].label[0:-2]

        for roleuri, chapter in xsd.items():
            if ('parent' in chapter.label.lower() and
                    bs_label in chapter.label):
                break
        else:
            logger.warning(msg='bs parentical shares not found')
            return data

        pres = self.xbrlfile.pres.get(roleuri, None)
        if pres is None:
            logger.warning(msg='bs parentical shares not found')
            return data

        tags = pres.gettags()

        shares = self.xbrlfile.dfacts[self.xbrlfile.dfacts['name'].isin(tags)]
        shares = shares[shares['uom'] == 'shares']
        shares = shares[shares['edate'] == self.xbrlfile.period]

        for index, row in shares.iterrows():
            context = self.xbrlfile.contexts[row['context']]
            if SharesFilter.filter_shares_context(context):
                member = context.member[-1]
                if member is None:
                    member = ''
                data.append((self.adsh,
                             row['name'],
                             row['value'],
                             row['sdate'],
                             row['edate'],
                             member))
        if not data:
            logger.warning(msg='bs parentical shares not found', extra={
                           'details': "bs doesn't contains any shares data"})
        return data

    def _gather_shares_facts(self) -> None:
        data = self._mine_dei_for_shares()  # + self._mine_bs_parent_for_shares()

        self.shares_facts = pd.DataFrame(
            data,
            columns=[
                'adsh',
                'name',
                'value',
                'sdate',
                'edate',
                'member'])

    def _read_text_blocks(self):
        raise NotImplementedError('_read_text_blocks is not implemented')

    def _find_proper_company_name_cik(self, record: FileRecord) -> None:
        """
        some filers file they report under child company name
        read proper cik from xbrl file, then find company name
        change cik and company_name fields in record
        if fails throw XbrlException()
        """
        if self.cik == self.xbrlfile.cik:
            return

        try:
            (cik, context) = [(int(e[0]), e[1]) for e in filter(
                lambda x: int(x[0]) == self.xbrlfile.cik, self.xbrlfile.dei.cik)][0]
            company_name = [
                e
                for e in filter(
                    lambda x: x[1] == context, self.xbrlfile.dei.company_name)][0][0]
            self.cik = cik
            record.cik = cik
            record.company_name = company_name

        except (IndexError, ValueError):
            logger = logs.get_logger(name=__name__)
            logger.error("couldn't find proper company name and CIK")
            raise XbrlException('')

    def _prepare(self, record: FileRecord, zip_filename: str):
        self.extentions = []
        self.numeric_facts = None
        self.shares_facts = None
        self.new_facts = None
        self.cik = record.cik
        self.adsh = record.adsh
        self.zip_filename = zip_filename

        logger = logs.get_logger(__name__)

    @abstractmethod
    def do_job(self):
        pass

    def feed(self, record: FileRecord, zip_filename: str):
        logger = logs.get_logger(__name__)
        self._prepare(record, zip_filename)

        try:
            good = False
            self.xbrlzip.open_packet(zip_filename)
            self.xbrlfile.prepare(self.xbrlzip, record)
            self._find_proper_company_name_cik(record)
            self.do_job()
            good = True

            logger.info('has been read')
        except XbrlException:
            pass
        except Exception:
            logger.error(msg='unknown error', exc_info=True)

        return good


class NumericDataMiner(DataMiner):
    def do_job(self):
        self.xbrlfile.read_units_facts_fn()

        DataMiner._choose_main_sheets(self)
        DataMiner._find_main_sheet_contexts(self)
        DataMiner._calculate(self)

        DataMiner._gather_numeric_facts(self)
        DataMiner._gather_shares_facts(self)


class SharesDataMiner(DataMiner):
    def do_job(self):
        self.xbrlfile.read_units_facts_fn()
        self._gather_shares_facts()


class ChapterNamesMiner(DataMiner):
    def do_job(self):
        self.xbrlfile.read_units_facts_fn()

        self._choose_shares_sheets()
        self._gather_shares_facts()


def prepare_report(miner: DataMiner, record: FileRecord) -> Dict[str, Any]:
    file_link = remove_root_dir(miner.zip_filename)
    report = {'adsh': record.adsh,
              'cik': record.cik,
              'period': miner.xbrlfile.period,
              'period_end': miner.xbrlfile.fye,
              'fin_year': miner.xbrlfile.fy,
              'taxonomy': miner.xbrlfile.dei.us_gaap,
              'form': record.form_type,
              'quarter': 0,
              'file_date': record.file_date,
              'file_link': file_link,
              'trusted': 1,
              'structure': _dump_structure(miner),
              'contexts': _dump_contexts(miner)
              }
    return report


def prepare_nums(miner: DataMiner) -> Optional[pd.DataFrame]:
    return miner.numeric_facts


def prepare_shares(miner: DataMiner) -> Optional[pd.DataFrame]:
    return miner.shares_facts


def prepare_company(miner: DataMiner, record: FileRecord) -> Dict[str, Any]:
    company = {'company_name': record.company_name,
               'sic': record.sic,
               'cik': record.cik,
               'updated': record.file_date}

    return company


def _dump_structure(miner: DataMiner) -> str:
    structure = {}
    for sheet, roleuri in miner.sheets.mschapters.items():
        xsd_chapter = miner.xbrlfile.xsd.get(roleuri, None)
        if not xsd_chapter:
            continue

        calc_chapter = (miner
                        .xbrlfile
                        .calc
                        .get(roleuri, None))
        if calc_chapter is None:
            calc_chapter = CalcChapter(roleuri=roleuri,
                                       label=xsd_chapter.label)
        else:
            calc_chapter.label = xsd_chapter.label
        structure[sheet] = calc_chapter

    return algos.xbrljson.dumps(structure)


def _dump_contexts(miner: DataMiner) -> str:
    return json.dumps(miner.extentions)
