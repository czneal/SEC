# -*- coding: utf-8 -*-

import datetime as dt
from collections import namedtuple
from typing import Dict, Optional, Set, Iterable

import pandas as pd  # type: ignore

import logs
import utils
import xbrlxml.truevalues as tv
import xbrlxml.xbrlfileparser as xbrlfp
from xbrlxml.xbrlchapter import Chapter, ReferenceParser
from xbrlxml.xbrlexceptions import XbrlException
from xbrlxml.xbrlrss import FileRecord
from xbrlxml.xbrlzip import XBRLZipPacket
from xbrlxml.xsdfile import XSDChapter, XSDFile

XbrlFiles = namedtuple('XbrlFiles', ['xbrl', 'xsd', 'pres', 'defi', 'calc'])


class XbrlFile():
    def __init__(self):
        self.calc: Dict[str, Chapter] = {}
        self.defi: Dict[str, Chapter] = {}
        self.pres: Dict[str, Chapter] = {}
        self.xsd: Dict[str, XSDChapter] = {}

        self.dei = xbrlfp.DEI()
        self.period: dt.date = dt.date.today()
        self.fy: int = 0
        self.fye: str = ''
        self.contexts: xbrlfp.Contexts = {}
        self.root = None
        self.cik: int = 0
        self.adsh: str = ''

        self.facts: xbrlfp.Facts = {}
        self.units: xbrlfp.Units = {}
        self.footnotes: xbrlfp.FootNotes = {}

        self.dfacts: pd.DataFrame = pd.DataFrame()

        self.text_blocks: xbrlfp.TextBlocks = {}

    def prepare(self,
                files: XBRLZipPacket,
                record: FileRecord):
        """Read calculation, presentation, definition and xsd schemes
        read dei and contexts section of xbrl file

        setup schemes property
        setup contexts property
        setup dei property
        setup period property
        setup root property

        if failed raise XbrlException()
        """
        XbrlFile.__init__(self)
        self.adsh = record.adsh

        # read xbrl file into xmltree object
        root = utils.opensmallxmlfile(files.xbrl)
        if root is None:
            root = utils.openbigxmlfile(files.xbrl)
        if root is None:
            msg = 'unable open xbrl file'
            logs.get_logger(__name__).error(msg=msg)
            raise XbrlException(msg)

        # read data and setup properties
        self.root = root
        self.__read_scheme_files(files)
        self.__read_dei_contexts()
        self.__find_period(record)

    def __read_dei_contexts(self):
        """Parse dei section and contexts section of xbrl file
        setup dei property
        setup contexts property
        setup cik property

        if failed raise XbrlException()
        """
        parser = xbrlfp.XbrlParser()
        self.contexts = parser.parse_contexts(self.root)
        units = parser.parse_units(self.root)
        self.dei = parser.parse_dei(self.root, units)
        self.cik = parser.parse_cik(self.root)
        if self.cik == 0:
            msg = "couldn't find CIK in report"
            logs.get_logger(__name__).error(msg=msg)
            raise XbrlException(msg)

    def __read_scheme_files(self, files: XBRLZipPacket):
        """Read scheme files
        setup shemes property

        if failed raise XbrlException()
        """
        "unittested"

        logger = logs.get_logger(__name__)
        try:
            rparser = ReferenceParser('presentation')
            self.pres = rparser.parse(files.pres)
        except BaseException:
            msg = "couldn't read xbrl report without presentation scheme"
            logger.error(msg=msg)
            raise XbrlException(msg)
        try:
            self.xsd = XSDFile().read(files.xsd)
        except BaseException:
            msg = "couldn't read xbrl report without xsd scheme"
            logger.error(msg=msg)
            raise XbrlException(msg)

        try:
            rparser.setreftype('definition')
            self.defi = rparser.parse(files.defi)
        except BaseException:
            logger.warning('missing definition scheme')

        try:
            rparser.setreftype('calculation')
            self.calc = rparser.parse(files.calc)
        except BaseException:
            logger.warning('missing calculation scheme')

    def __find_period(self, record: FileRecord):
        """Find period date
        setup period property
        setup fy propery
        setup fye property

        if failed raise XbrlException()
        """

        period = tv.TRUE_VALUES.get_true_period(self.adsh)
        period_dei: Optional[dt.date] = None
        period_rss = record.period

        if (period is None and period_rss is not None):
            data = [(p, context, len(self.contexts[context].dim))
                    for [p, context] in self.dei.period]
            data = sorted(data, key=lambda x: x[2])

            if data:
                period_dei = data[0][0]

            if period_dei == period_rss:
                period = period_dei
        if period is None:
            msg = 'period match failed'
            extra = {'period_dei': period_dei,
                     'period_rss': period_rss}
            logs.get_logger(__name__).error(msg=msg, extra=extra)
            raise XbrlException(msg)

        self.period = period
        self.fy, self.fye = utils.calculate_fy_fye(period)

    def read_units_facts_fn(self):
        """
        parse units, facts, footnotes
        make them clean
        setup numfacts property
        setup dfacts prperty
        """
        parser = xbrlfp.XbrlParser()
        units = parser.parse_units(self.root)
        footnotes = parser.parse_footnotes(self.root)
        facts = parser.parse_facts(self.root)

        facts = xbrlfp.xbrltrans.transform_facts(facts)
        footnotes = xbrlfp.xbrltrans.transform_fn(facts, footnotes)

        xclean = xbrlfp.XbrlCleanUp()
        (facts, units, _, footnotes) = xclean.cleanup(
            facts, units, self.contexts, footnotes)
        self.facts = facts
        self.units = units
        self.footnotes = footnotes

        # transform facts and contexts into DataFrame object
        dfacts = pd.DataFrame([f.asdict() for f in facts.values()])
        dcontexts = pd.DataFrame([c.asdict()
                                  for c in self.contexts.values()])
        dcontexts = dcontexts[dcontexts['edate'] == self.period]
        dunits = pd.DataFrame(
            [[unitid, str(u)] for unitid, u in units.items()],
            columns=['unitid', 'uom'])
        self.dfacts = (dfacts.merge(dcontexts,
                                    left_on='context',
                                    right_on='context')
                       .merge(dunits,
                              left_on='unitid',
                              right_on='unitid'))

    def read_text_blocks(self, text_blocks: Iterable[str]) -> None:
        parser = xbrlfp.XbrlParser()
        self.text_blocks = parser.parse_textblocks(self.root, text_blocks)

    def any_gaap_fact(self):
        if self.dfacts.shape[0] == 0:
            return False

        return 'us-gaap' in self.dfacts['version'].unique()
