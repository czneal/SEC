# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 15:04:34 2019

@author: Asus
"""

import unittest
from xbrlxml.xsdfile import XSDFile

class TestXSDFile(unittest.TestCase):
    def test_read(self):
        xsd = XSDFile()
        chapters = xsd.read('resources/aal-20181231.xsd')        
        
        with self.subTest(i='Balance sheet'):
            bs = chapters['http://www.aa.com/role/ConsolidatedBalanceSheets']
            self.assertTrue(bs.roleuri == 'http://www.aa.com/role/ConsolidatedBalanceSheets')
            self.assertTrue(bs.label == 'Consolidated Balance Sheets')
            self.assertTrue(bs.sect == 'sta')
            self.assertTrue(bs.id == 'ConsolidatedBalanceSheets')
        
        with self.subTest(i='... - details - debt - ....'):
            d = chapters['http://www.aa.com/role/Debt20172EetcsDetails']
            self.assertTrue(d.label == 'Debt - 2017-2 EETCs (Details)')
            self.assertTrue(d.sect == 'det')
            
if __name__ == '__main__':
    unittest.main()
