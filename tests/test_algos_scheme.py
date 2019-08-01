# -*- coding: utf-8 -*-

import unittest
import json

import algos.scheme
import algos.xbrljson
import resource_chapters as res

class TestExtention(unittest.TestCase):
    def test_extend_calc_scheme(self):
        with self.subTest(i='extend done'):            
            scheme = res.make_chapters()            
            algos.scheme.extend_clac_scheme(
                    'roleuri1', scheme, 
                    extentions = {'NodeLabel2': 'roleuri2'})    
            
            self.assertTrue(scheme['roleuri1'].nodes['NodeLabel2'].children ==
                            scheme['roleuri2'].nodes['NodeLabel2'].children)
            
    def test_findcalcextentions(self):
        scheme = res.make_chapters()
        extentions, warnings = algos.scheme.find_extentions(
                roleuri='roleuri1',
                calc = scheme,
                pres = ['roleuri1', 'roleuri2'],
                xsds = ['roleuri1', 'roleuri2'])
        self.assertTrue(len(warnings)==0)
        self.assertDictEqual(extentions, {'NodeLabel2': 'roleuri2'})

class TestNodeCopy(unittest.TestCase):    
    def test_makenodecopy(self):
        with self.subTest(i='copy Node'):
            chapters = res.make_chapters()
            
            n = chapters['roleuri1'].nodes['NodeLabel0']
            node = n.copy()
            self.assertNotEqual(id(node), id(n))
            self.assertEqual(node.children['NodeLabel1'].parent,
                             node)
            self.assertEqual(node.children['NodeLabel1'].children['NodeLabel3'].parent,
                             node.children['NodeLabel1'])
            self.assertEqual(json.dumps(n, cls = algos.xbrljson.ForTestJsonEncoder),
                             json.dumps(node, cls = algos.xbrljson.ForTestJsonEncoder))
        with self.subTest(i='raise AssertionError'):
            self.assertRaises(AssertionError, algos.scheme.makenodecopy, 'asd')

class TestEnumeration(unittest.TestCase):
    def test_enum(self):
        chapters = res.make_chapters()
        with self.subTest(i = 'enum node by name'):
            etalon = [['us-gaap:NodeName0', 'us-gaap:NodeName1', 1.0, 0, False], ['us-gaap:NodeName1', 'us-gaap:NodeName3', 1.0, 1, True], ['us-gaap:NodeName1', 'us-gaap:NodeName4', -1.0, 1, True], ['us-gaap:NodeName0', 'us-gaap:NodeName2', -1.0, 0, True]]
            data = []
            for item in algos.scheme.enum(chapters['roleuri1'].nodes['NodeLabel0']):
                data.append(item)
            
            self.assertListEqual(etalon, data)
            
        with self.subTest(i = 'enum node by label'):
            etalon = [['NodeLabel0', 'NodeLabel1', 1.0, 0, False], ['NodeLabel1', 'NodeLabel3', 1.0, 1, True], ['NodeLabel1', 'NodeLabel4', -1.0, 1, True], ['NodeLabel0', 'NodeLabel2', -1.0, 0, True]]
            data = []
            for item in algos.scheme.enum(chapters['roleuri1'].nodes['NodeLabel0'],
                                          func = lambda x: x.label):
                data.append(item)            
            
            self.assertListEqual(etalon, data)
            
        with self.subTest(i = 'enum Chapter by name'):
            etalon = [['roleuri2', 'us-gaap:NodeName2', 1.0, 0, False], ['us-gaap:NodeName2', 'mg:NodeName1', -1.0, 1, True], ['us-gaap:NodeName2', 'mg:NodeName2', 1.0, 1, False], ['mg:NodeName2', 'mg:NodeName3', 1.0, 2, True], ['mg:NodeName2', 'mg:NodeName4', -1.0, 2, True]]
            data = []
            for item in algos.scheme.enum(chapters['roleuri2']):
                data.append(item)
            
            self.assertListEqual(etalon, data)
            
        with self.subTest(i = 'enum Chapter by label'):
            etalon = [['roleuri1', 'NodeLabel0', 1.0, 0, False], ['NodeLabel0', 'NodeLabel1', 1.0, 1, False], ['NodeLabel1', 'NodeLabel3', 1.0, 2, True], ['NodeLabel1', 'NodeLabel4', -1.0, 2, True], ['NodeLabel0', 'NodeLabel2', -1.0, 1, True]]
            data = []
            for item in algos.scheme.enum(chapters['roleuri1'],
                                          func = lambda x: x.label):
                data.append(item)                        
            
            self.assertListEqual(etalon, data)
            
        with self.subTest(i = 'enum chapters by label'):
            etalon = [['roleuri1', 'NodeLabel0', 1.0, 0, False], ['NodeLabel0', 'NodeLabel1', 1.0, 1, False], ['NodeLabel1', 'NodeLabel3', 1.0, 2, True], ['NodeLabel1', 'NodeLabel4', -1.0, 2, True], ['NodeLabel0', 'NodeLabel2', -1.0, 1, True], ['roleuri2', 'NodeLabel2', 1.0, 0, False], ['NodeLabel2', 'NodeLabel6', -1.0, 1, True], ['NodeLabel2', 'NodeLabel7', 1.0, 1, False], ['NodeLabel7', 'NodeLabel8', 1.0, 2, True], ['NodeLabel7', 'NodeLabel9', -1.0, 2, True]]
            data = []
            for item in algos.scheme.enum(chapters,
                                          func = lambda x: x.label):
                data.append(item)
            
            self.assertListEqual(etalon, data)
            
        with self.subTest(i = 'enum chapters by label only child'):
            etalon = [['NodeLabel0'], ['NodeLabel1'], ['NodeLabel3'], ['NodeLabel4'], ['NodeLabel2'], ['NodeLabel2'], ['NodeLabel6'], ['NodeLabel7'], ['NodeLabel8'], ['NodeLabel9']]
            data = []
            for item in algos.scheme.enum(chapters, outpattern='c',
                                          func = lambda x: x.label):
                data.append(item)
            
            self.assertListEqual(etalon, data)    
        
        with self.subTest(i = 'enum raise AssertionError'):
            with self.assertRaises(AssertionError):
                for item in algos.scheme.enum(chapters, outpattern='pcwr'):
                    pass
        
        with self.subTest(i = 'enum chapters by label only child and leaf'):
            etalon = [['NodeLabel3', True], ['NodeLabel4', True], ['NodeLabel2', True], ['NodeLabel6', True], ['NodeLabel8', True], ['NodeLabel9', True]]
            data = []
            for item in algos.scheme.enum(chapters, outpattern='cl', leaf=True,
                                          func = lambda x: x.label):
                data.append(item)            
            self.assertListEqual(etalon, data)
            
if __name__ == '__main__':
    unittest.main()
        