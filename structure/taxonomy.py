# -*- coding: utf-8 -*-
"""
Created on Fri May  3 11:01:42 2019

@author: Asus
"""
import re
import os
import zipfile
import pandas as pd
import lxml

from typing import Optional, Dict, cast

import urltools
from xbrlxml.xbrlchapter import ReferenceParser, CalcChapter
import algos.xbrljson
import algos.scheme
from utils import add_root_dir, make_absolute


class DefaultTaxonomy():
    def __init__(self):
        with open(make_absolute('def_tax.json', __file__)) as f:
            self.chapters: Dict[str, CalcChapter] = cast(
                Dict[str, CalcChapter], algos.xbrljson.loads(f.read()))

    def calcscheme(self, sheet: str, year: int) -> CalcChapter:
        assert sheet in {'bs', 'cf', 'is'}

        return self.chapters[sheet]


class Taxonomy(object):
    gaap_link = "http://xbrl.fasb.org/us-gaap/"

    def __init__(self, gaap_id: str):
        self.gaap_dir: str = add_root_dir('us-gaap/')
        self.gaap_file: str = 'us-gaap-' + gaap_id + '.zip'
        self.gaap_id: str = gaap_id
        self.taxonomy: Optional[pd.DataFrame] = None

    def read(self):
        if not self.download():
            return False
        try:
            xsd = SchemeXSD()
            parser = ReferenceParser('calculation')
            data = []
            with zipfile.ZipFile(
                    self.gaap_dir + self.gaap_file) as zfile:
                for xsd_filename in [f for f in zfile.namelist()
                                     if f.find('/stm/') >= 0 and
                                     f.endswith('xsd')]:
                    xsd.read(zfile.open(xsd_filename))
                    for row in xsd.asdict():
                        cal_file = zfile.open(
                            os.path.dirname(xsd_filename) + '/' +
                            row['cal_filename'])
                        chapters = parser.parse(cal_file)
                        for roleuri, ch in chapters.items():
                            row['structure'] = json.dumps(
                                ch,
                                cls=algos.xbrljson.ForDBJsonEncoder)
                            data.append(row)
            self.taxonomy = pd.DataFrame(data)
        except Exception:
            return False

        return True

    def download(self) -> bool:
        if not os.path.exists(self.gaap_dir):
            os.mkdir(self.gaap_dir)

        if not os.path.exists(self.gaap_dir + self.gaap_file):
            if not urltools.fetch_urlfile(self.gaap_link +
                                          self.gaap_id[0:4] +
                                          '/' +
                                          self.gaap_file, self.gaap_dir +
                                          self.gaap_file):
                return False

        return True


class SchemeXSD(object):
    def __init__(self):
        self.cal_filenames = []
        self.doc = []
        self.roles = []
        self.sheets = []
        self.types = []

    def read(self, filename):
        self.__init__()

        etree = lxml.etree.parse(filename)
        root = etree.getroot()

        xlink = root.nsmap['xlink']
        for link in list(root.iter('{*}linkbaseRef')):
            href = link.attrib.get('{%s}href' % xlink)
            role = link.attrib.get('{%s}role' % xlink)
            if role is None:
                continue

            if role.split('/')[-1].lower() == 'calculationLinkbaseRef'.lower():
                self.cal_filenames.append(href)

        doc = root.find('.//xs:documentation', root.nsmap)
        if doc is None:
            return
        ms = re.compile(r'.*\s+-\s+Statement\s+-\s+.*(calculation).*')
        doc = [
            re.sub(
                r'\s*\(calculation\).*',
                '',
                re.sub(
                    r'\d*\s+-\sStatement\s+-\s*',
                    '',
                    d.strip(),
                    flags=re.I),
                flags=re.I) for d in doc.text.strip().split('\n') if ms.match(d)]

        sfp = re.compile('.*Statement.*Financial.*Position.*', flags=re.I)
        scf = re.compile('.*Statement.*Cash.*Flow.*', flags=re.I)
        soi = re.compile(r'.*Statement\s+of\s+Income*.', flags=re.I)
        soc = re.compile(r'.*Statement.*Comprehensive\s+Income.*', flags=re.I)
        sfpre = {
            'dbo': re.compile(
                '.*Deposit.*Based.*Operations.*',
                flags=re.I),
            'dir': re.compile(
                '.*Direc.*Method.*Operating.*',
                flags=re.I)}
        for d in doc:
            if sfp.match(d):
                self.sheets.append('sfp')
                for tp, r in sfpre.items():
                    if r.match(d):
                        self.types.append(tp)
                        break
                break

        if len(doc) > 0:
            self.doc = doc

    def aslist(self):
        return [(f.split('-')[3], f.split('-')[4], f, doc)
                for (f, doc) in zip(self.cal_filenames, self.doc)]

    def asdict(self):
        return [{'sheet': f.split('-')[-6],
                 'type':f.split('-')[-5],
                 'cal_filename':f,
                 'doc':doc} for (f, doc) in zip(self.cal_filenames, self.doc)]


if __name__ == '__main__':
    tax = Taxonomy('2018-01-31')
    tax.read()
    df = tax.taxonomy
