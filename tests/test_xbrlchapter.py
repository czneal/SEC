# -*- coding: utf-8 -*-
"""
Created on Sat May 25 17:02:52 2019

@author: Asus
"""

import unittest
import lxml

from xbrlxml.xbrlchapter import ReferenceParser
from xbrlxml.xbrlchapter import CalcChapter, DimChapter, Chapter
from xbrlxml.xbrlchapter import Node
from algos.xbrljson import ForTestJsonEncoder
from xbrlxml.xbrlchapter import ChapterFactory

import json
import resource_referenceparser as resource
import deepdiff as dd

class TestChapterFactory(unittest.TestCase):    
    def test_chapter(self):        
        with self.assertRaises(AssertionError):
            ChapterFactory.chapter('def')
        
        with self.subTest(i='definition'):
            self.assertIsInstance(ChapterFactory.chapter('definition'),
                                  DimChapter)
        with self.subTest(i='calculation'):
            self.assertIsInstance(ChapterFactory.chapter('calculation'),
                                  CalcChapter)
        with self.subTest(i='presentation'):
            self.assertIsInstance(ChapterFactory.chapter('presentation'),
                              DimChapter)
        
class TestChapter(unittest.TestCase):
    def make_chapter(self):
        chapter = Chapter(roleuri='roleuri1')
        
        n1 = Node()        
        n1.tag = 'tag1'
        n1.version = 'us-gaap'
        n1.name = n1.getname()
        n1.label = n1.name
        
        n2 = Node()        
        n2.tag = 'tag2'
        n2.version = 'us-gaap'
        n2.name = n2.getname()
        n2.label = n2.name
        chapter.nodes[n1.label] = n1
        chapter.nodes[n2.label] = n2
        
        labels = {'nodelabel1': 'us-gaap:tag1',
                  'nodelabel2': 'us-gaap:tag2'}
        return chapter, labels
    
    def test_updatearc(self):
        s = """{"roleuri": "roleuri1", "nodes": {"us-gaap:tag1": {"tag": "tag1", "version": "us-gaap", "arc": null, "children": {"us-gaap:tag2": {"tag": "tag2", "version": "us-gaap", "arc": {"wegth": 1.0, "order": 1}, "children": null}}}}}"""
                
        chapter, labels = self.make_chapter()        
        arc = {'from': 'nodelabel1', 'to':'nodelabel2',
               'attrib':{'wegth':1.0, 'order':1}}
        
        chapter.update_arc(arc, labels)
        self.maxDiff = None
        self.assertEqual(s, json.dumps(chapter, cls=ForTestJsonEncoder))
        
        
    def test_getnodes(self):
        s = """[{"tag": "us-gaap:tag1"}, {"tag": "us-gaap:tag2", "wegth": 1.0, "order": 1}]"""
        j = json.loads(s)
        
        chapter, labels = self.make_chapter()
        
        arc = {'from': 'nodelabel1', 'to':'nodelabel2',
               'attrib':{'wegth':1.0, 'order':1}}
                
        chapter.update_arc(arc, labels)
        diff = dd.DeepDiff(chapter.getnodes(), j)
        self.assertEqual(diff, {}, diff)
        
    def test_gettags(self):
        chapter, labels = self.make_chapter()
        
        arc = {'from': 'nodelabel1', 'to':'nodelabel2',
               'attrib':{'wegth':1.0, 'order':1}}
    
        chapter.update_arc(arc, labels)
        self.assertEqual(chapter.gettags(), {'us-gaap:tag1','us-gaap:tag2'})
        
    def test_DimChapter_dimmembers(self):
        for case in resource.dimchapter_test_cases:
            with self.subTest(i=case['filename']):
                parser = ReferenceParser(case['ref_type'])
                chapters = parser.parse(case['filename'])
                
                with self.subTest(i='answer[0]'):
                    dimmems = chapters["http://www.aa.com/role/ConsolidatedBalanceSheets"].dimmembers()
                    diff = dd.DeepDiff(dimmems, json.loads(case['answers'][0]),
                                       ignore_order=True)
                    self.assertEqual(diff, {}, diff)
                
                with self.subTest(i='answer[1]'):                    
                    dimmems = chapters["http://www.aa.com/role/FairValueMeasurementsAndOtherInvestmentsSummaryOfAssetsMeasuredAtFairValueOnRecurringBasisDetails"].dimmembers()
                    diff = dd.DeepDiff(dimmems, json.loads(case['answers'][1]),
                                       ignore_order=True)
                    self.assertEqual(diff, {}, diff)
                    
    def test_DimChapter_dims(self):
        for case in resource.dimchapter_test_cases:
            with self.subTest(i=case['filename']):
                parser = ReferenceParser(case['ref_type'])
                chapters = parser.parse(case['filename'])
                
                with self.subTest(i='answer[2]'):
                    dims = chapters["http://www.aa.com/role/ConsolidatedBalanceSheets"].dims()
                    diff = dd.DeepDiff(dims, json.loads(case['answers'][2]),
                                       ignore_order=True)
                    self.assertEqual(diff, {}, diff)
                
                with self.subTest(i='answer[3]'):                    
                    dims = chapters["http://www.aa.com/role/FairValueMeasurementsAndOtherInvestmentsSummaryOfAssetsMeasuredAtFairValueOnRecurringBasisDetails"].dims()
                    diff = dd.DeepDiff(dims, json.loads(case['answers'][3]),
                                       ignore_order=True)
                    self.assertEqual(diff, {}, diff)
            
class TestReferenceParser(unittest.TestCase):
    def test_node_getname(self):
        n = Node()
        n.version = "us-gaap"
        n.tag = 'Liabilities'
        self.assertEqual(n.getname(), 'us-gaap:Liabilities')
        
    def test_node_asdictdim(self):
        with self.subTest(i='arc is None'):
            n = Node()
            n.version = "us-gaap"
            n.tag = 'Liabilities'
            n.name = n.getname()
            
            diff = dd.DeepDiff(n.asdict(), {'tag':'us-gaap:Liabilities'})
            self.assertEqual(diff, {}, diff)
            
        with self.subTest(i='arc is not None'):
            n = Node()
            n.version = "us-gaap"
            n.tag = 'Liabilities'
            n.name = n.getname()
            n.arc = {'weight':1.0, 'order':1}
            
            diff = dd.DeepDiff(n.asdict(), {'tag':'us-gaap:Liabilities', 'weight':1.0, 'order':1})
            self.assertEqual(diff, {}, diff)
        
    def test_parse_node(self):
        for case in resource.node_test_cases:
            with self.subTest(i=case['filename']):
                root = lxml.etree.parse(case['filename'])
                parser = ReferenceParser(case['ref_type'])
                
                locs = [loc for loc in root.iter('{*}loc')]
            
                index = [1, 100, 56, 34, 78]            
                for index, loc in enumerate([locs[i] for i in index]):
                    n = parser.parse_node(loc)
                    self.assertEqual(json.dumps(n.asdict()), case['answers'][index])
    
    def test_parse_arc(self):        
        for case in resource.arc_test_cases:
            with self.subTest(i=case['filename']):
                root = lxml.etree.parse(case['filename'])
                parser = ReferenceParser(case['ref_type'])
                
                arcs = [arc for arc in root.iter('{*}'+case['ref_type'] + 'Arc')]
                index = [1, 100, 56, 34, 78]                
                
                for index, arc in enumerate([arcs[i] for i in index]):
                    arcdict = parser.parse_arc(arc)                
                    answer = json.loads(case['answers'][index])
                    self.assertEqual(dd.DeepDiff(arcdict, answer), {})
                    
    def test_parse_chapter(self):
        for case in resource.chapter_test_cases:
            with self.subTest(i=case['filename']):
                root = lxml.etree.parse(case['filename'])
                parser = ReferenceParser(case['ref_type'])
                
                link = [e for e in root.iter('{*}'+case['ref_type'] + 'Link')]
                index = [1, 2, 12, 10, 9]
                for index, link in enumerate([link[i] for i in index]):
                    chapter = parser.parse_chapter(link)
                    answer = case['answers'][index]
                    chapter_str = json.dumps(chapter, cls=ForTestJsonEncoder)
                    
                    self.assertEqual(chapter_str, answer)
                    
if __name__ == '__main__':    
    unittest.main(verbosity=0)
#    for case in resource.chapter_test_cases:
#        print(case['filename'])
#        root = lxml.etree.parse(case['filename'])
#        parser = ReferenceParser(case['ref_type'])
#        
#        link = [e for e in root.iter('{*}'+case['ref_type'] + 'Link')]
#        index = [1, 2, 12, 10, 9]
#        for index, link in enumerate([link[i] for i in index]):
#            chapter = parser.parse_chapter(link)
#            case['answers'][index] = json.dumps(chapter, 
#                                        cls=ForTestJsonEncoder)
#            
#    ts = resource.chapter_test_cases