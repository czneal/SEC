# -*- coding: utf-8 -*-

import unittest
import datetime
import lxml

from xbrlxml.xbrlrss import FilingRSS, FilingRecord

class TestFilingRecord(unittest.TestCase):
   
    def test_read(self):
        record = FilingRecord()
        root = lxml.etree.parse('resources/rss-2018-01.xml').getroot()
        items = root.findall(".//item")
        record.read(items[86])
        dei = record.asdict()
        
        jr = {'company_name': 'LANCASTER COLONY CORP', 'form_type': '10-Q', 'cik': 57515, 'adsh': '0000057515-18-000005', 'period': datetime.date(2017, 12, 31), 'file_date': datetime.date(2018, 1, 30), 'fye': '0630', 'fy': 2017}
        
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
              'fy': 2016}
        r2 = {'company_name': 'ADOBE SYSTEMS INC', 'form_type': '10-K', 'cik': 796343, 'adsh': '0000796343-18-000015', 'period': datetime.date(2017, 12, 1), 'file_date': datetime.date(2018, 1, 22), 'fye': '1202', 'fy': 2017}
        
        return r1, r2
    
    def test_filing_records(self):
        rss = FilingRSS()
        rss.open_file('resources/rss-2018-01.xml')
        records = rss.filing_records()
        r1, r2 = self.make_records()
        
        self.assertEqual(len(records), 137)
        self.assertDictEqual(records[86], r1)
        self.assertDictEqual(records[55], r2)
        
if __name__ == '__main__':
    unittest.main()

