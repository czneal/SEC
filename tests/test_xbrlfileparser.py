# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 12:39:50 2019

@author: Asus
"""
import unittest
import lxml
import json
import deepdiff as dd
import datetime as dt

from xbrlxml.xbrlfileparser import Fact, Context, Unit, FootNote
from xbrlxml.xbrlfileparser import XbrlParser

"""
Не парсить TextBlock z:/sec/2015/02/0001308606-0001308606-15-000029.zip
solved - XMLParser(recover=True)

в dei есть элемент с пустым текстом z:/sec/2015/02/0000074208-0000074208-15-000013.zip
solved - if block in parse_dei

в footnotes есть элемент с пустым текстом z:/sec/2015/02/0001163165-0001193125-15-059281.zip
solveed - if block in parser_footnote

"""
class TestFact(unittest.TestCase):
    def test_name(self):
        f = Fact()
        f.version = 'us-gaap'
        f.tag = 'Assets'
        self.assertEqual(f.name(), 'us-gaap:Assets')
        
    def test_key(self):
        f = Fact()
        f.version = 'us-gaap'
        f.tag = 'Assets'
        f.context = 'somecontext'
        diff = dd.DeepDiff(f.key(), ('us-gaap:Assets', 'somecontext'))
        self.assertEqual(diff, {}, diff)
        
    def test_asdict(self):
        f = Fact()
        f.version = 'us-gaap'
        f.tag = 'Assets'
        f.context = 'somecontext'
        f.decimals = '10'
        f.value = '1000000000.00'
        f.unitid = 'usd'
        f.factid = 'Fact01'
        j = {'name': 'us-gaap:Assets',
             'decimals':'10',
             'unitid':'usd',
             'value':'1000000000.00',
             'context':'somecontext',
             'factid':'Fact01',
             'tag': 'Assets',
             'version': 'us-gaap'}
        diff = dd.DeepDiff(f.asdict(), j)
        self.assertEqual(diff, {}, diff)
        
    def simple_fact(self):
        f = Fact()
        f.version = 'us-gaap'
        f.tag = 'Assets'
        f.context = 'somecontext'
        f.decimals = '10'
        f.value = '1000000000.00'
        f.unitid = 'usd'
        f.factid = 'Fact01'
        return f
    
    def test_update(self):        
        with self.subTest('do update'):
            f = self.simple_fact()
            fu = self.simple_fact()
            fu.decimals = '8'
            fu.value = '2000000.0'
            f.update(fu)
            self.assertTrue(f.value == 2000000.0 and 
                            f.decimals == 8)
            
        with self.subTest(i='do not update'):
            f = self.simple_fact()
            fu = self.simple_fact()
            f.update(fu)
            self.assertTrue(f.value == 1000000000.0 and 
                            f.decimals == 10)
            
        with self.subTest(i='null value'):
            f = self.simple_fact()
            fu = self.simple_fact()
            fu.value = None
            f.update(fu)
            self.assertTrue(f.value == 1000000000.0 and 
                            f.decimals == 10)
            
    def test_decimalize(self):
        with self.subTest(i='normal'):
            f = self.simple_fact()
            f.decimalize()
            self.assertTrue(f.value == 1000000000.0 and 
                            f.decimals == 10)
        with self.subTest(i='value is None'):
            f = self.simple_fact()
            f.value = None
            f.decimalize()
            self.assertTrue(f.value == f.default_value and 
                            f.decimals == 10)
        with self.subTest(i='value not numeric'):
            f = self.simple_fact()
            f.value = 'abrakadabra'
            f.decimalize()
            self.assertTrue(f.value == f.default_value and 
                            f.decimals == 10)
        with self.subTest(i='decimals is None'):
            f = self.simple_fact()
            f.decimals = None
            f.decimalize()
            self.assertTrue(f.value == 1000000000.0 and 
                            f.decimals is f.default_decimals)
        with self.subTest(i='decimals not numeric'):
            f = self.simple_fact()
            f.decimals = 'abrakadabra'
            f.decimalize()
            self.assertTrue(f.value == 1000000000.0 and 
                            f.decimals is f.default_decimals)
            
    def test_equal(self):
        with self.subTest(i='equal'):
            f1 = self.simple_fact()
            f2 = self.simple_fact()        
            self.assertTrue(f1 == f2)            
        with self.subTest(i='not equal in name'):
            f1 = self.simple_fact()
            f2 = self.simple_fact()        
            f2.tag = 'Liabilities'
            self.assertTrue(f1 != f2)
        with self.subTest(i='not equal in context'):
            f1 = self.simple_fact()
            f2 = self.simple_fact()        
            f2.context = 'anothercontext'
            self.assertTrue(f1 != f2)        

class TestContext(unittest.TestCase):
    def simple_context(self):
        c = Context()
        c.contextid = 'context1'
        c.edate = dt.date(2019,1,1)
        c.entity = '0000001'
        return c
    
    def dim_context(self):
        c = self.simple_context()
        c.dim.append('FirstAxis')
        c.member.append('FirstAxisMember')
        c.dim.append('SecondAxis')
        c.member.append('SecondAxisMember')
        return c
    
    def test_asdictdim(self):
        with self.subTest(i='dim == 0'):
            c = self.simple_context()
            j = [{'context': 'context1', 
                  'sdate': None, 
                  'edate': dt.date(2019, 1, 1), 
                  'dim': None, 'member': None}]
            self.assertListEqual(c.asdictdim(), j, msg=c.asdictdim())
        with self.subTest(i='dim == 2'):
            c = self.dim_context()
            j = [{'context': 'context1', 
                  'sdate': None, 
                  'edate': dt.date(2019, 1, 1), 
                  'dim': None, 'member': None}, 
                 {'context': 'context1', 
                  'sdate': None, 
                  'edate': dt.date(2019, 1, 1), 
                  'dim': 'FirstAxis', 'member': 'FirstAxisMember'}, {'context': 'context1', 'sdate': None, 'edate': dt.date(2019, 1, 1), 'dim': 'SecondAxis', 'member': 'SecondAxisMember'}]
            self.assertListEqual(c.asdictdim(), j, msg=c.asdictdim())
        with self.subTest(i='dim == 0 and sdate=2018-01-01'):
            c = self.simple_context()
            c.sdate = dt.date(2018,1,1)
            j = [{'context': 'context1', 'sdate': dt.date(2018, 1, 1), 'edate': dt.date(2019, 1, 1), 'dim': None, 'member': None}]
            self.assertListEqual(c.asdictdim(), j, msg=c.asdictdim())
            
    def test_asdict(self):
        with self.subTest(i='dim == 0'):
            c = self.simple_context()
            j = {'context': 'context1', 'sdate': None, 'edate': dt.date(2019, 1, 1)}
            self.assertDictEqual(c.asdict(), j, msg=c.asdict())
            
        with self.subTest(i='dim == 2'):
            c = self.dim_context()
            j = {'context': 'context1', 'sdate': None, 'edate': dt.date(2019, 1, 1)}
            self.assertDictEqual(c.asdict(), j, msg=c.asdict())
        with self.subTest(i='dim == 0 and sdate=2018-01-01'):
            c = self.simple_context()
            c.sdate = dt.date(2018,1,1)
            j = {'context': 'context1', 'sdate': dt.date(2018, 1, 1), 'edate': dt.date(2019, 1, 1)}
            self.assertDictEqual(c.asdict(), j, msg=c.asdict())
            
    def test_isinstant(self):
        with self.subTest(i='instant'):
            c = self.simple_context()
            self.assertTrue(c.isinstant())
        with self.subTest(i='not instant'):
            c = self.simple_context()
            c.sdate = dt.date(2018,1,1)
            self.assertTrue(not c.isinstant())
            
    def test_isdimentional(self):
        with self.subTest(i='dimentional'):
            c = self.simple_context()
            self.assertTrue(not c.isdimentional())
        with self.subTest(i='not dimentional'):
            c = self.dim_context()
            self.assertTrue(c.isdimentional())
            
    def test_issuccessor(self):
        with self.subTest(i='successor'):
            c = self.dim_context()
            c.member[2] = 'us-gaap:SuccessorMember'
            self.assertTrue(c.issuccessor())
        with self.subTest(i='not successor'):
            c = self.dim_context()            
            self.assertTrue(not c.issuccessor())    
            
    def test_isparent(self):
        with self.subTest(i='parent'):
            c = self.dim_context()
            c.member[2] = 'us-gaap:ParentCompanuMember'
            self.assertTrue(c.isparent())
        with self.subTest(i='not successor'):
            c = self.dim_context()
            self.assertTrue(not c.isparent())  
            
class TestUnit(unittest.TestCase):
    def simple_unit(self):
        u = Unit()
        u.num = 'usd'
        u.unitid = 'isousd'
        return u
    
    def ratio_unit(self):
        u = Unit()
        u.num = 'usd'
        u.denom = 'share'
        u.unitid = 'usdpershare'
        return u
    
    def test_unitstr(self):
        with self.subTest(i='simple unit'):
            u = self.simple_unit()
            self.assertEqual(u.unitstr(), 'usd')
            self.assertEqual(str(u), 'usd')
        with self.subTest(i='ratio unit'):
            u = self.ratio_unit()
            self.assertEqual(u.unitstr(), 'usd/share')
            self.assertEqual(str(u), 'usd/share')
            
class TestXbrlParser(unittest.TestCase):
    def test_parsedei(self):        
        parser = XbrlParser()
        with self.subTest(i='resources/aal-20181231.xml'):
            root = lxml.etree.parse('resources/aal-20181231.xml').getroot()
            #root = lxml.etree.parse('resources/gen-20121231.xml').getroot()
            units = parser.parse_units(root)
            dei = parser.parse_dei(root, units)
            jn = json.loads("""{"fye": [["--12-31", "FD2018Q4YTD"], ["--12-31", "FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember"]], "period": [["2018-12-31", "FD2018Q4YTD"], ["2018-12-31", "FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember"]], "shares": [["449055548", "I2019Q1Feb20"], ["1000", "I2019Q1Feb20_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember"]], "fy": [["2018", "FD2018Q4YTD"], ["2018", "FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember"]], "cik": [["0000006201", "FD2018Q4YTD"], ["0000004515", "FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember"]], "us-gaap":"2018-01-31"}""")
            self.assertEqual(jn, dei)
            
        with self.subTest(i='resources/gen-20121231.xml'):            
            root = lxml.etree.parse('resources/gen-20121231.xml').getroot()
            units = parser.parse_units(root)
            dei = parser.parse_dei(root, units)
            jn = json.loads("""{"fye": [["--12-31", "D2012Q4YTD"]], "period": [["2012-12-31", "D2012Q4YTD"]], "shares": [["1", "I2012Q4"], ["0", "I2012Q4_dei_LegalEntityAxis_gen_GenonAmericasGenerationLlcMember"], ["0", "I2012Q4_dei_LegalEntityAxis_gen_GenonMidAtlanticLlcMember"]], "fy": [["2012", "D2012Q4YTD"]], "cik": [["0001140761", "D2012Q4YTD_dei_LegalEntityAxis_gen_GenonAmericasGenerationLlcMember"], ["0001138258", "D2012Q4YTD_dei_LegalEntityAxis_gen_GenonMidAtlanticLlcMember"], ["0001126294", "D2012Q4YTD"]], "us-gaap":"2012-01-31"}""")
                        
            self.assertEqual(jn, dei)
            
        with self.subTest(i='resources/udr-20141231.xml (e.text is None == True)'):
            root = lxml.etree.parse('resources/udr-20141231.xml').getroot()
            units = parser.parse_units(root)
            dei = parser.parse_dei(root, units)
            jn = json.loads("""{"fye": [["--12-31", "FD2014Q4YTD"]], "period": [["2014-12-31", "FD2014Q4YTD"], ["2014-12-31", "FD2014Q4YTD_dei_LegalEntityAxis_udr_UnitedDominionRealityLPMember"]], "shares": [["258765713", "I2015Q1SD"], ["0", "I2015Q1SD_dei_LegalEntityAxis_udr_UnitedDominionRealityLPMember"]], "fy": [["2014", "FD2014Q4YTD"]], "cik": [["0000074208", "FD2014Q4YTD"], ["0001018254", "FD2014Q4YTD_dei_LegalEntityAxis_udr_UnitedDominionRealityLPMember"]], "us-gaap":"2014-01-31"}""")
            
            self.assertEqual(jn, dei)
            
    def test_parsefact(self):
        pass
    
    def test_parsefootnotes(self):
        parser = XbrlParser()
        with self.subTest(i='resources/cop-20141231.xml (fn.text is None == True)'):
            root = lxml.etree.parse('resources/cop-20141231.xml').getroot()
            fn = parser.parse_footnotes(root)
            self.assertEqual(len(fn), 196)
            
    def test_parse_textblocks(self):
        parser = XbrlParser()
        with self.subTest(i='resources/aag-20131231.xml'):
            root = lxml.etree.parse('resources/aag-20131231.xml').getroot()
            text_blocks = ['ScheduleOfShareBasedCompensationStockOptionsActivityTableTextBlock',
                           'ScheduleOfShareBasedCompensationActivityTableTextBlock',
                           'ScheduleOfShareBasedCompensationSharesAuthorizedUnderStockOptionPlansByExercisePriceRangeTable',
                           'ScheduleOfShareBasedCompensationStockOptionsAndStockAppreciationRightsAwardActivityTableTextBlock']
            data = parser.parse_textblocks(root, text_blocks)
            data = [(d['name'], d['context'], len(d['value'])) 
                        for d in data]
            ethalon = [('ScheduleOfShareBasedCompensationActivityTableTextBlock', 'D2013Q4YTD_dei_LegalEntityAxis_aag_AmericanAirlinesIncMember', 27719), ('ScheduleOfShareBasedCompensationStockOptionsAndStockAppreciationRightsAwardActivityTableTextBlock', 'D2013Q4YTD', 59382), ('ScheduleOfShareBasedCompensationActivityTableTextBlock', 'D2013Q4YTD', 27767)]
            
            diff = dd.DeepDiff(data, ethalon, ignore_order=True)
            self.assertDictEqual(diff, {}, diff)
                
        with self.subTest(i='resources/ba-20131231.xml'):
            root = lxml.etree.parse('resources/ba-20131231.xml').getroot()
            text_blocks = ['ScheduleOfShareBasedCompensationStockOptionsActivityTableTextBlock',
                           'ScheduleOfShareBasedCompensationActivityTableTextBlock',
                           'ScheduleOfShareBasedCompensationSharesAuthorizedUnderStockOptionPlansByExercisePriceRangeTable',
                           'ScheduleOfShareBasedCompensationStockOptionsAndStockAppreciationRightsAwardActivityTableTextBlock']
            data = parser.parse_textblocks(root, text_blocks)
            ethalon = [('ScheduleOfShareBasedCompensationStockOptionsActivityTableTextBlock', 'FD2013Q4YTD', 25042)]
            data = [(d['name'], d['context'], len(d['value'])) 
                        for d in data]
            diff = dd.DeepDiff(ethalon, data, ignore_order=True)
            self.assertDictEqual(diff, {}, diff)

class TestFootNote(unittest.TestCase):
    def test_fotnote(self):
        pass
if __name__ == '__main__':
    unittest.main()
    