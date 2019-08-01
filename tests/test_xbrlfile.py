# -*- coding: utf-8 -*-

import unittest, mock
import datetime as dt
import pandas as pd

from xbrlxml.xbrlfile import XbrlFile
from xbrlxml.xbrlfileparser import Context
from xbrlxml.xbrlfile import XbrlFiles
from xbrlxml.xbrlfile import XbrlException
from xbrlxml.xbrlchapter import Chapter
from xbrlxml.xsdfile import XSDChapter

class TestXbrlFile(unittest.TestCase):
    def make_df_contexts(self):        
        contexts = {}
        c = Context()
        c.contextid = 'context1'
        c.edate = dt.datetime(2018,1,1)
        c.entity = 1000        
        contexts[c.contextid] = c
        
        c = Context()
        c.contextid = 'context2'
        c.edate = dt.datetime(2018,1,1)
        c.entity = 1000
        c.dim.append('us-gaap:FirstAxis')
        c.member.append('us-gaap:FirstMember')
        contexts[c.contextid] = c
        
        c = Context()
        c.contextid = 'context3'
        c.edate = dt.datetime(2018,1,1)
        c.entity = 1000
        c.dim.append('us-gaap:FirstAxis')
        c.member.append('us-gaap:SuccessorMember')
        contexts[c.contextid] = c
        
        c = Context()
        c.contextid = 'context4'
        c.edate = dt.datetime(2018,1,1)
        c.entity = 1000
        c.dim.append('us-gaap:FirstAxis')
        c.member.append('us-gaap:ParentMember')
        contexts[c.contextid] = c
        
        return contexts
        
    def test_choosecntx(self):
        xbrl = XbrlFile()
        with self.subTest(i='shape[0] == 0'):
            f = pd.DataFrame(data=None, columns=['context', 'cnt'])
            self.assertIsNone(xbrl._choosecontext(f, None))
            
        with self.subTest(i='only nondim'):
            f = pd.DataFrame(data=[['context1', 50]], columns=['context', 'cnt'])
            contexts = self.make_df_contexts()
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context1')
        with self.subTest(i='only successor'):
            f = pd.DataFrame(data=[['context3', 50]], columns=['context', 'cnt'])
            contexts = self.make_df_contexts()
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context3')
        with self.subTest(i='only parent'):
            f = pd.DataFrame(data=[['context4', 50]], columns=['context', 'cnt'])
            contexts = self.make_df_contexts()
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context4')
            
        with self.subTest(i='nondim/top > 0.5'):
            f = pd.DataFrame(data=[['context2', 50],
                                   ['context1', 40]], columns=['context', 'cnt'])
            contexts = self.make_df_contexts()            
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context1')
        with self.subTest(i='nondim/top < 0.5'):
            contexts = self.make_df_contexts()
            f = pd.DataFrame(data=[['context2', 50],
                                   ['context1', 20]], columns=['context', 'cnt'])            
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context2')
        with self.subTest(i='no nondim'):            
            contexts = self.make_df_contexts()
            f = pd.DataFrame(data=[['context3', 50],
                                   ['context2', 20]], columns=['context', 'cnt'])            
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context3')
        with self.subTest(i='nondim/top>0.5 and top==successor'):
            contexts = self.make_df_contexts()
            f = pd.DataFrame(data=[['context3', 50],
                                   ['context1', 30]], columns=['context', 'cnt'])            
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context3')
        with self.subTest(i='all present'):
            contexts = self.make_df_contexts()
            f = pd.DataFrame(data=[['context2', 50],
                                   ['context1', 40],
                                   ['context3', 30],
                                   ['context4', 20]], columns=['context', 'cnt'])            
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context3')
        with self.subTest(i='return top if ather less it'):
            contexts = self.make_df_contexts()
            f = pd.DataFrame(data=[['context2', 50],
                                   ['context1', 10],
                                   ['context3', 3],
                                   ['context4', 2]], columns=['context', 'cnt'])            
            self.assertEqual(xbrl._choosecontext(f, contexts), 'context2')
            
    def test_readschemefiles(self):        
        with self.subTest(subtest='no xsd'):
            xbrl = XbrlFile()
            files = XbrlFiles(xbrl = 'resources/gen-20121231.xml',
                          xsd = '', 
                          pres = 'resources/gen-20121231_pre.xml', 
                          defi = 'resources/gen-20121231_def.xml', 
                          calc = 'resources/gen-20121231_cal.xml')
            self.assertRaises(XbrlException, xbrl.readschemefiles, files=files)
            
        with self.subTest(subtest='no pre'):
            xbrl = XbrlFile()
            files = XbrlFiles(xbrl = 'resources/gen-20121231.xml',
                          xsd = 'resources/gen-20121231.xsd', 
                          pres = None, 
                          defi = 'resources/gen-20121231_def.xml', 
                          calc = 'resources/gen-20121231_cal.xml')
            self.assertRaises(XbrlException, xbrl.readschemefiles, files=files)
            
        with self.subTest(subtest='no def'):
            xbrl = XbrlFile()
            files = XbrlFiles(xbrl = 'resources/gen-20121231.xml',
                          xsd = 'resources/gen-20121231.xsd', 
                          pres = 'resources/gen-20121231_pre.xml', 
                          defi = '', 
                          calc = 'resources/gen-20121231_cal.xml')
            self.assertEqual(xbrl.readschemefiles(files=files), None)
            self.assertTrue(xbrl.schemes['defi'] == {})
            
        with self.subTest(subtest='no def'):
            xbrl = XbrlFile()
            files = XbrlFiles(xbrl = 'resources/gen-20121231.xml',
                          xsd = 'resources/gen-20121231.xsd', 
                          pres = 'resources/gen-20121231_pre.xml', 
                          defi = 'resources/gen-20121231_def.xml', 
                          calc = '')
            self.assertEqual(xbrl.readschemefiles(files=files), None)    
            self.assertTrue(xbrl.schemes['calc'] == {})
            
    def test_findmschapters(self):
        with self.subTest(i="raise count of chapters > 3"):
            with mock.patch('xbrlxml.xbrlchapter.Chapter.gettags') as mgettags:
                mgettags.return_value = set('a')
                xbrl = XbrlFile()
                xbrl.pres = {'roleuri1':Chapter(), 
                             'roleuri2':Chapter(), 
                             'roleuri3':Chapter(), 
                             'roleuri4':Chapter()}
                xbrl.xsd = {'roleuri1':XSDChapter('roleuri1', 'Balance Sheet 1', 'sta', 'id1'),
                            'roleuri2':XSDChapter('roleuri2', 'Balance Sheet 2', 'sta', 'id2'),
                            'roleuri3':XSDChapter('roleuri3', 'Income Statement', 'sta', 'id3'),
                            'roleuri4':XSDChapter('roleuri4', 'cash Flow', 'sta', 'id4'),}
                self.assertRaises(XbrlException, xbrl._findmschapters)
        with self.subTest(i="pass"):
            with mock.patch('structure.taxonomy.DefaultTaxonomy') as mockms:
                mockms.return_value.calcscheme.return_value = None
                xbrl = XbrlFile()
                xbrl.pres = {'roleuri1':Chapter(), 
                             'roleuri2':Chapter(), 
                             'roleuri3':Chapter(), 
                             'roleuri4':Chapter()}
                xbrl.xsd = {'roleuri1':XSDChapter('roleuri1', 'Balance Sheet [parenthical]', 'sta', 'id1'),
                            'roleuri2':XSDChapter('roleuri2', 'Balance Sheet 2', 'sta', 'id2'),
                            'roleuri3':XSDChapter('roleuri3', 'Income Statement', 'sta', 'id3'),
                            'roleuri4':XSDChapter('roleuri4', 'cash Flow', 'sta', 'id4'),}
                self.assertTrue(xbrl._findmschapters() is None)
    def test_findperiod(self):
        c = Context()
        c.contextid = 'context'
        c.dim = [None]
        contexts = {c.contextid:c}
        
        with self.subTest(i='dei is none'):
            record = {}
            dei = {'period':[['2012-01-01', 'context']]}
            xbrl = XbrlFile()
            with self.assertRaises(XbrlException):
                period = xbrl._XbrlFile__findperiod(dei, record, contexts)
        with self.subTest(i='period is none'):
            record = {'period':'2012-12-01'}
            dei = {}
            xbrl = XbrlFile()
            with self.assertRaises(XbrlException):
                period = xbrl._XbrlFile__findperiod(dei, record, contexts)
        with self.subTest(i='period and dei doesnt match'):
            record = {'period':'2012-12-01'}
            dei = {'period': [['2013-01-12', 'context']]}
            xbrl = XbrlFile()
            with self.assertRaises(XbrlException):
                period = xbrl._XbrlFile__findperiod(dei, record, contexts)
        with self.subTest(i='period and dei match'):
            record = {'period':'2012-12-01'}
            dei = {'period': [['2012-12-01', 'context']]}
            xbrl = XbrlFile()            
            period = xbrl._XbrlFile__findperiod(dei, record, contexts)
            self.assertEqual(dt.date(2012,12,1), period)
        
        c = Context()
        c.contextid = 'context1'        
        c.dim = [None, 'dimention']
        contexts[c.contextid] = c
        with self.subTest(i='many contexts'):
            record = {'period':'2012-12-01'}
            dei = {'period': [['2012-12-01', 'context'],
                              ['2013-12-01', 'context1']]}
            xbrl = XbrlFile()            
            period = xbrl._XbrlFile__findperiod(dei, record, contexts)
            self.assertEqual(dt.date(2012,12,1), period)
                
if __name__ == '__main__':
    unittest.main()
