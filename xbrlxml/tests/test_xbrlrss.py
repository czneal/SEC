# -*- coding: utf-8 -*-

import datetime
import os
import unittest

import lxml

import xbrlxml.xbrlrss


def make_absolute(path: str) -> str:
    dir_name = os.path.dirname(__file__)
    return os.path.join(dir_name, path)


class TestFilingRecord(unittest.TestCase):
    def test_read(self):
        root = lxml.etree.parse(make_absolute('res/rss-2018-01.xml')).getroot()
        items = root.findall(".//item")
        record = xbrlxml.xbrlrss.record_from_xbrl(items[86])
        dei = record.__dict__

        jr = {
            'company_name': 'LANCASTER COLONY CORP',
            'form_type': '10-Q',
            'cik': 57515,
            'adsh': '0000057515-18-000005',
            'period': datetime.date(
                2017,
                12,
                31),
            'file_date': datetime.date(
                2018,
                1,
                30),
            'fye': '0630',
            'fy': 2017,
            'sic': 2030}

        self.assertDictEqual(jr, dei)


class TestFilingRss(unittest.TestCase):
    def make_records(self):
        r1 = {'company_name': 'Loop Industries, Inc.',
              'form_type': '10-K/A',
              'cik': 1504678,
              'adsh': '0001477932-18-000204',
              'period': datetime.date(2017, 2, 28),
              'file_date': datetime.date(2018, 1, 12),
              'fye': '0228',
              'fy': 2016,
              'sic': 4813}
        r2 = {
            'company_name': 'ADOBE SYSTEMS INC',
            'form_type': '10-K',
            'cik': 796343,
            'adsh': '0000796343-18-000015',
            'period': datetime.date(
                2017,
                12,
                1),
            'file_date': datetime.date(
                2018,
                1,
                22),
            'fye': '1202',
            'fy': 2017,
            'sic': 7372}

        return r1, r2

    def test_filing_records(self):
        rss = xbrlxml.xbrlrss.FilingRSS()
        rss.open_file(make_absolute('res/rss-2018-01.xml'))
        records = [r for r in rss.filing_records() if (
            r.adsh == '0001477932-18-000204' or r.adsh == '0000796343-18-000015')]
        r1, r2 = self.make_records()

        self.assertEqual(len(records), 2)
        self.assertDictEqual(records[1].__dict__, r1)
        self.assertDictEqual(records[0].__dict__, r2)


if __name__ == '__main__':
    unittest.main()
