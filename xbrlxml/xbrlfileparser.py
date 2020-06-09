# -*- coding: utf-8 -*-
"""
Created on Wed May 29 12:18:19 2019

@author: Asus
"""

import re
import datetime
import contextlib

from typing import Optional, List, Dict, Union, cast, Tuple, Any, Set, Iterable

import utils
import logs
from xbrlxml.xbrlexceptions import XbrlException

TContextAsDictDim = List[Dict[str, Union[Optional[str],
                                         Optional[datetime.date],
                                         List[Optional[str]]]]]

default_decimals = 19


class Fact():
    def __init__(self,
                 version: str = '',
                 tag: str = '',
                 value: float = None,
                 context: str = '',
                 unitid: str = '',
                 decimals: int = default_decimals,
                 factid: str = ''):
        self.tag = tag
        self.version = version
        self.value: Optional[float] = value
        self.context = context
        self.unitid = unitid
        self.decimals = decimals
        self.factid = factid

    def update(self, fact):
        # type: (Fact) -> None
        "unittested"
        if (fact.value is None):
            return

        if self.decimals > fact.decimals:
            self.value = fact.value
            self.decimals = fact.decimals

    def name(self) -> str:
        "unittested"
        return '{0}:{1}'.format(self.version, self.tag)

    def key(self) -> Tuple[str, str]:
        "unittested"
        return (self.name(), self.context)

    def asdict(self) -> Dict[str, Any]:
        "unittested"
        return {'name': self.name(),
                'tag': self.tag,
                'version': self.version,
                'value': self.value,
                'context': self.context,
                'unitid': self.unitid,
                'decimals': self.decimals,
                'factid': self.factid}

    def __eq__(self, f: object) -> bool:
        "unittested"
        if not isinstance(f, Fact):
            raise NotImplementedError()

        return bool(f.name() == self.name() and f.context == self.context)


class Context():
    def __init__(self):
        self.contextid: str = ''
        self.sdate: Optional[datetime.date] = None
        self.edate: datetime.date = datetime.date.today()
        self.entity: int = 0
        self.dim: List[Optional[str]] = [None]
        self.member: List[Optional[str]] = [None]

    def axises(self) -> int:
        return len(self.dim) - 1

    def isinstant(self) -> bool:
        "unittested"
        return bool(self.sdate is None)

    def asdictdim(self) -> TContextAsDictDim:
        "unittested"
        retval: TContextAsDictDim = []
        for d, m in zip(self.dim, self.member):
            retval.append({'context': self.contextid,
                           'sdate': self.sdate,
                           'edate': self.edate,
                           'dim': d,
                           'member': m})
        return retval

    def asdict(self) -> Dict[str, Any]:
        "unittested"
        return {'context': self.contextid,
                'sdate': self.sdate,
                'edate': self.edate}

    def isdimentional(self) -> bool:
        "unittested"
        return (len(self.dim) > 1)

    def issuccessor(self) -> bool:
        "unittested"
        for m in self.member:
            if m is None:
                continue
            if re.match('.*successor.*', m, flags=re.I):
                return True

        return False

    def isparent(self) -> bool:
        "unittested"
        for m in self.member:
            if m is None:
                continue
            if re.match('.*parent.*', m, flags=re.I):
                return True

        return False


class Unit():
    def __init__(self, unitid: str = '',
                 nom: str = '',
                 denom: Optional[str] = None):
        self.unitid: str = unitid
        self.nom: str = nom
        self.denom: Optional[str] = denom

    def unitstr(self):
        "unittested"
        ret = self.nom
        if self.denom is not None:
            ret += '/' + self.denom
        return ret

    def __str__(self):
        "unittested"
        return self.unitstr()

    def __repr__(self):
        return str(self)


class FootNote():
    def __init__(self):
        self.footnote: Optional[str] = None
        self.footnoteid: str = ''


def to_float(value: Optional[str]) -> Optional[float]:
    if value is None or value == '':
        return None

    try:
        return float(value)
    except ValueError:
        return None


def to_decimals(value: Optional[str]) -> int:
    if value in {'INF', 'inf'}:
        return 0

    v = to_float(value)
    if v is None:
        return default_decimals
    return abs(int(v))


class DEI():
    def __init__(self):
        self.period: List[Tuple[datetime.date, str]] = []
        self.fye: List[Tuple[str, str]] = []
        self.shares: List[Tuple[float, str]] = []
        self.fy: List[Tuple[int, str]] = []
        self.cik: List[Tuple[int, str]] = []
        self.company_name: List[Tuple[str, str]] = []
        self.us_gaap: datetime.date.today()


Facts = Dict[Tuple[str, str], Fact]
FootNotes = Dict[Tuple[str, str], FootNote]
Units = Dict[str, Unit]
Contexts = Dict[str, Context]
TextBlocks = Dict[Tuple[str, str], str]


class XbrlParser():
    def __init__(self):
        self.nsmapi = None

    def parse_fact(self, factelem) -> Fact:
        """
        factelem - Element from lxml.etree
        return Fact class
        """

        f = Fact()
        f.tag = re.sub('{.*}', '', factelem.tag)
        f.version = self.nsmapi[re.sub(
            '{|}', '', factelem.tag.replace(f.tag, ''))]

        for attr, value in factelem.attrib.items():
            attr = re.sub('{.*}', '', attr)
            value = cast(str, value.strip())
            if attr == 'contextRef':
                f.context = value
            if attr == 'id':
                f.factid = value
            if attr == 'decimals':
                f.decimals = to_decimals(value)
            if attr == 'unitRef':
                f.unitid = value

        f.value = to_float(factelem.text)

        return f

    def parse_context(self, contextelem) -> Context:
        """
        contextelem - Element from lxml.etree
        return Context class
        """

        c = Context()
        c.contextid = contextelem.attrib['id'].strip()

        for i in contextelem.iter('{*}identifier'):
            c.entity = int(i.text.strip())

        for d in contextelem.iter('{*}startDate'):
            c.sdate = utils.str2date(d.text.strip())

        for d in contextelem.iter('{*}endDate'):
            edate = utils.str2date(d.text.strip())
            if edate is None:
                raise ValueError(f'context {c.contextid} has no end date')
            c.edate = edate

        for d in contextelem.iter('{*}instant'):
            edate = utils.str2date(d.text.strip())
            if edate is None:
                raise ValueError(f'context {c.contextid} has no end date')

            c.edate = edate

        for ex in contextelem.iter('{*}explicitMember'):
            c.dim.append(ex.attrib['dimension'])
            c.member.append(ex.text.strip())

        return c

    def parse_footnote(self, footelem) -> FootNote:
        """
        contextelem - Element from lxml.etree
        return FootNote class
        """

        fn = FootNote()
        if footelem.text is None:
            fn.footnote = ''
        else:
            fn.footnote = footelem.text.strip()

        fn.footnoteid = footelem.attrib['{%s}label' % footelem.nsmap['xlink']]

        return fn

    def parse_unit(self, unitelem) -> Unit:
        """
        unitelem - Element from lxml.etree
        return Unit class
        """
        u = Unit()

        u.unitid = unitelem.attrib['id'].strip()
        div = unitelem.find('{*}divide')
        if div is not None:
            nom = div.find('{*}unitNumerator').find('{*}measure')
            denom = div.find('{*}unitDenominator').find('{*}measure')
            u.nom = re.sub('.*:', '', nom.text).lower()
            u.denom = re.sub('.*:', '', denom.text).lower()
        else:
            m = unitelem.find('{*}measure')
            u.nom = re.sub('.*:', '', m.text).lower()

        return u

    def parse_units(self, root) -> Units:
        """
        return dict {unitid:Unit()}
        """
        units = {}
        for unitelem in root.iter('{*}unit'):
            u = self.parse_unit(unitelem)
            units[u.unitid] = u

        return units

    def parse_facts(self, root, ignore_textblocks=True) -> List[Fact]:
        """
        return list of Fact
        """
        self.nsmapi = {v: k for k, v in root.nsmap.items()}
        facts = []

        for factelem in root.findall('.//*[@unitRef]'):
            if (ignore_textblocks and
                    factelem.tag.lower().endswith('textblock')):
                continue

            f = self.parse_fact(factelem)
            facts.append(f)

        return facts

    def parse_contexts(self, root) -> Contexts:
        """
        return dict: {contextid: Context()}
        """
        contexts = {}
        for contextelem in root.iter('{*}context'):
            c = self.parse_context(contextelem)
            contexts[c.contextid] = c

        return contexts

    def parse_footnotes(self, root) -> FootNotes:
        """
        return dict: {factid: FootNote()}
        """
        fnlink = root.find('{*}footnoteLink')
        if fnlink is None:
            return {}

        footnotes = {}
        for footelem in fnlink.findall('{*}footnote'):
            fn = self.parse_footnote(footelem)
            footnotes[fn.footnoteid] = fn

        factids = {}
        for loc in fnlink.findall('{*}loc'):
            href = re.sub('.*#', '', loc.attrib
                          ['{%s}href' % loc.nsmap['xlink']])
            label = loc.attrib['{%s}label' % loc.nsmap['xlink']]
            factids[label] = href

        arcs = {}
        for arc in fnlink.findall('{*}footnoteArc'):
            fr = arc.attrib['{%s}from' % root.nsmap['xlink']]
            to = arc.attrib['{%s}to' % root.nsmap['xlink']]
            arcs[factids[fr]] = footnotes[to]

        return arcs

    def parse_cik(self, root) -> int:
        ciks: Dict[int, int] = {}
        for e in root.iter('{*}identifier'):
            if e.text is None:
                continue
            with contextlib.suppress(ValueError):
                cik = int(e.text.strip())
                ciks[cik] = ciks.get(cik, 0) + 1

        if len(ciks) == 0:
            return 0

        return sorted(ciks.items(), key=lambda x: x[1], reverse=True)[0][0]

    def parse_dei(self, root, units: Units) -> DEI:
        """return DEI object"""
        "unittested"

        dei = DEI()

        if 'dei' not in root.nsmap:
            msg = 'no dei section in xbrl file'
            logs.get_logger(__name__).error(msg=msg)
            raise XbrlException(msg)
        if 'us-gaap' not in root.nsmap:
            msg = "taxonomy doesn't definded"
            logs.get_logger(__name__).error(msg=msg)
            raise XbrlException(msg)

        dei.us_gaap = root.nsmap['us-gaap'].split('/')[-1]

        for e in root.iter('{%s}*' % root.nsmap['dei']):
            if e.text is None:
                continue

            tag = re.sub('{.*}', '', e.tag.strip())
            text = e.text.strip()
            unit = e.attrib.get('unitRef', None)
            context = e.attrib.get('contextRef', None)
            if context is None:
                continue

            if unit is not None and units[unit].unitstr() == 'shares':
                shares = to_float(text)
                if shares and shares > 0:
                    dei.shares.append((shares, context))
                continue

            try:
                if tag == 'DocumentFiscalYearFocus':
                    dei.fy.append((int(text), context))
                if tag == 'CurrentFiscalYearEndDate':
                    dei.fye.append((text, context))
                if tag == 'DocumentPeriodEndDate':
                    period = utils.str2date(text)
                    if period:
                        dei.period.append((period, context))
                if tag == 'EntityCentralIndexKey':
                    dei.cik.append((int(text), context))
                if tag == 'EntityRegistrantName':
                    dei.company_name.append((text, context))
            except ValueError:
                pass

        return dei

    def parse_textblocks(self, root,
                         text_blocks: Iterable[str]) -> TextBlocks:
        """return Dict[(name, context), text]
        """
        "unittested"

        data: TextBlocks = {}
        for name in text_blocks:
            for e in root.iter('{*}' + name):
                data[(name, e.attrib['contextRef'])] = (e.text.strip()
                                                        .replace('\r', '')
                                                        .replace('\n', ''))

        return data


class xbrltrans():
    @staticmethod
    def transform_facts(facts: List[Fact]) -> Facts:
        tfacts: Facts = {}
        for f in facts:
            key = f.key()
            if key in tfacts:
                tfacts[key].update(f)
            else:
                tfacts[key] = f
        return tfacts

    @staticmethod
    def transform_fn(facts: Facts,
                     footnotes: Dict[str,
                                     FootNote]) -> FootNotes:
        fn: FootNotes = {}
        for key, f in facts.items():
            if f.factid in footnotes:
                fn[key] = footnotes[f.factid]

        return fn


class XbrlCleanUp():
    def __init__(self):
        self.currencies = [None,
                           'usd', 'eur', 'cad', 'aud',
                           'shares', 'pure']

    def cleanup(self, facts: Facts,
                units: Units,
                contexts: Contexts,
                footnotes: FootNotes):
        units = self.cleanupunits(units)
        facts = self.cleanupfacts(facts, units)
        # pure contexts needed when parse TextBlocks and other
        # nonnumeric facts
        # contexts = self.cleanupcontexts(contexts, facts)
        footnotes = self.cleanupfn(footnotes, facts)

        return facts, units, contexts, footnotes

    def cleanupfacts(self, facts: Facts, units: Units) -> Facts:
        facts_cu: Facts = {}
        for key, f in facts.items():
            if f.unitid not in units or f.value is None:
                continue

            facts_cu[key] = f

        return facts_cu

    def cleanupunits(self, units: Units) -> Units:
        units_cu: Units = {}
        for u in units.values():
            if (u.nom in self.currencies and u.denom in self.currencies):
                units_cu[u.unitid] = u

        return units_cu

    def cleanupcontexts(self, contexts: Contexts, facts: Facts) -> Contexts:
        fact_contexts = set([f.context for f in facts.values()])
        contexts_cu: Contexts = {}
        for c in contexts.values():
            if c.contextid in fact_contexts:
                contexts_cu[c.contextid] = c

        return contexts_cu

    def cleanupfn(self, footnotes: FootNotes, facts: Facts) -> FootNotes:
        footnotes_cu: FootNotes = {}
        for key, f in facts.items():
            fn = footnotes.get(key, None)
            if fn:
                footnotes_cu[f.key()] = fn

        return footnotes_cu


if __name__ == '__main__':
    pass
