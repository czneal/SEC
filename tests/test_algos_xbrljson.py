# -*- coding: utf-8 -*-

from algos.xbrljson import ForTestJsonEncoder, ForDBJsonEncoder, custom_decoder
import unittest
import resource_chapters as res
import json

class TestCustomDecoder(unittest.TestCase):
    def test_loadchapter(self):
        chapter, chapter_str = res.TestLoad.make_simple_chapter()
        c = json.loads(chapter_str, object_hook = custom_decoder)
        self.assertTrue(c == chapter)
    
    def test_loadnode(self):
        node, node_str = res.TestLoad.make_simple_node()        
        n = json.loads(node_str, object_hook = custom_decoder)        
        self.assertTrue(n == node)
    
    def test_loadall(self):
        chapter, chapter_str = res.TestLoad.make_simple_chapter()
        chapters = {'bs':chapter, 'is':chapter}
        all_str = ("""{"bs": """ + chapter_str +
                       """, "is": """ + chapter_str + """}""")
        structure = json.loads(all_str, object_hook = custom_decoder)
        
        self.assertTrue(chapters == structure)
    
class TestCustomEncoder(unittest.TestCase):
    def test_dumpsnode(self):
        answer1 = """{"tag": "NodeName1", "version": "us-gaap", "arc": {"weight": 1.0}, "children": {"NodeLabel3": {"tag": "NodeName3", "version": "us-gaap", "arc": {"weight": 1.0}, "children": null}, "NodeLabel4": {"tag": "NodeName4", "version": "us-gaap", "arc": {"weight": -1.0}, "children": null}}}"""
        answer2 = """{"name": "us-gaap:NodeName1", "weight": 1.0, "children": {"us-gaap:NodeName3": {"name": "us-gaap:NodeName3", "weight": 1.0, "children": null}, "us-gaap:NodeName4": {"name": "us-gaap:NodeName4", "weight": -1.0, "children": null}}}"""
        chapters = res.make_chapters()
        s1 = json.dumps(chapters['roleuri1'].nodes['NodeLabel1'],
                       cls=ForTestJsonEncoder)
        s2 = json.dumps(chapters['roleuri1'].nodes['NodeLabel1'],
                       cls=ForDBJsonEncoder)
        self.assertEqual(answer1, s1)        
        self.assertEqual(answer2, s2)        
    
    def test_dumpschapter(self):
        answer1 = """{"roleuri": "roleuri2", "nodes": {"NodeLabel2": {"tag": "NodeName2", "version": "us-gaap", "arc": null, "children": {"NodeLabel6": {"tag": "NodeName1", "version": "mg", "arc": {"weight": -1.0}, "children": null}, "NodeLabel7": {"tag": "NodeName2", "version": "mg", "arc": {"weight": 1.0}, "children": {"NodeLabel8": {"tag": "NodeName3", "version": "mg", "arc": {"weight": 1.0}, "children": null}, "NodeLabel9": {"tag": "NodeName4", "version": "mg", "arc": {"weight": -1.0}, "children": null}}}}}}}"""
        answer2 = """{"roleuri": "roleuri2", "nodes": {"us-gaap:NodeName2": {"name": "us-gaap:NodeName2", "weight": 1.0, "children": {"mg:NodeName1": {"name": "mg:NodeName1", "weight": -1.0, "children": null}, "mg:NodeName2": {"name": "mg:NodeName2", "weight": 1.0, "children": {"mg:NodeName3": {"name": "mg:NodeName3", "weight": 1.0, "children": null}, "mg:NodeName4": {"name": "mg:NodeName4", "weight": -1.0, "children": null}}}}}}}"""
        chapters = res.make_chapters()
        s1 = json.dumps(chapters['roleuri2'],
                       cls=ForTestJsonEncoder)        
        s2 = json.dumps(chapters['roleuri2'],
                       cls=ForDBJsonEncoder)        
        
        self.assertEqual(answer1, s1)
        self.assertEqual(answer2, s2)   
        
    def test_dumpsall(self):
        answer1 = """{"roleuri1": {"roleuri": "roleuri1", "nodes": {"NodeLabel0": {"tag": "NodeName0", "version": "us-gaap", "arc": null, "children": {"NodeLabel1": {"tag": "NodeName1", "version": "us-gaap", "arc": {"weight": 1.0}, "children": {"NodeLabel3": {"tag": "NodeName3", "version": "us-gaap", "arc": {"weight": 1.0}, "children": null}, "NodeLabel4": {"tag": "NodeName4", "version": "us-gaap", "arc": {"weight": -1.0}, "children": null}}}, "NodeLabel2": {"tag": "NodeName2", "version": "us-gaap", "arc": {"weight": -1.0}, "children": null}}}}}, "roleuri2": {"roleuri": "roleuri2", "nodes": {"NodeLabel2": {"tag": "NodeName2", "version": "us-gaap", "arc": null, "children": {"NodeLabel6": {"tag": "NodeName1", "version": "mg", "arc": {"weight": -1.0}, "children": null}, "NodeLabel7": {"tag": "NodeName2", "version": "mg", "arc": {"weight": 1.0}, "children": {"NodeLabel8": {"tag": "NodeName3", "version": "mg", "arc": {"weight": 1.0}, "children": null}, "NodeLabel9": {"tag": "NodeName4", "version": "mg", "arc": {"weight": -1.0}, "children": null}}}}}}}}"""
        answer2 = """{"roleuri1": {"roleuri": "roleuri1", "nodes": {"us-gaap:NodeName0": {"name": "us-gaap:NodeName0", "weight": 1.0, "children": {"us-gaap:NodeName1": {"name": "us-gaap:NodeName1", "weight": 1.0, "children": {"us-gaap:NodeName3": {"name": "us-gaap:NodeName3", "weight": 1.0, "children": null}, "us-gaap:NodeName4": {"name": "us-gaap:NodeName4", "weight": -1.0, "children": null}}}, "us-gaap:NodeName2": {"name": "us-gaap:NodeName2", "weight": -1.0, "children": null}}}}}, "roleuri2": {"roleuri": "roleuri2", "nodes": {"us-gaap:NodeName2": {"name": "us-gaap:NodeName2", "weight": 1.0, "children": {"mg:NodeName1": {"name": "mg:NodeName1", "weight": -1.0, "children": null}, "mg:NodeName2": {"name": "mg:NodeName2", "weight": 1.0, "children": {"mg:NodeName3": {"name": "mg:NodeName3", "weight": 1.0, "children": null}, "mg:NodeName4": {"name": "mg:NodeName4", "weight": -1.0, "children": null}}}}}}}}"""
        chapters = res.make_chapters()
        s1 = json.dumps(chapters,
                       cls=ForTestJsonEncoder)        
        s2 = json.dumps(chapters,
                       cls=ForDBJsonEncoder)        
        
        self.assertEqual(answer1, s1)
        self.assertEqual(answer2, s2)
        
if __name__ == '__main__':
    unittest.main()
