# -*- coding: utf-8 -*-
from algos.xbrljson import (ForTestJsonEncoder,
                            ForDBJsonEncoder,
                            custom_decoder,
                            loads, dumps, Chapter)
import unittest
import algos.tests.resource_chapters as res
import json


class TestCustomDecoder(unittest.TestCase):
    def setUp(self):
        with open('algos/tests/res/test_structure.json') as f:
            self.structure = f.read()

    def test_loads(self):
        chapters = loads(self.structure)

        self.assertEqual(type(chapters), dict, msg='chapters is not dict')
        self.assertEqual(
            len(chapters),
            4, msg='test chapters count should be 4')
        self.assertTrue('bs' in chapters)
        self.assertTrue(isinstance(chapters['bs'], Chapter))
        self.assertTrue('us-gaap:Assets' in chapters['bs'].nodes)


class TestCustomEncoder(unittest.TestCase):
    def setUp(self):
        with open('algos/tests/res/test_dumps_db.json') as f:
            self.test_dumps_db = f.read()

        with open('algos/tests/res/test_dumps_test.json') as f:
            self.test_dumps_test = f.read()

    def test_dumpsnode(self):
        answer1 = """{"tag": "NodeName1", "version": "us-gaap", "arc": {"weight": 1.0}, "children": {"us-gaap:NodeName3": {"tag": "NodeName3", "version": "us-gaap", "arc": {"weight": 1.0}, "children": null}, "us-gaap:NodeName4": {"tag": "NodeName4", "version": "us-gaap", "arc": {"weight": -1.0}, "children": null}}}"""
        answer2 = """{"name": "us-gaap:NodeName1", "weight": 1.0, "children": {"us-gaap:NodeName3": {"name": "us-gaap:NodeName3", "weight": 1.0, "children": null}, "us-gaap:NodeName4": {"name": "us-gaap:NodeName4", "weight": -1.0, "children": null}}}"""
        chapters = res.make_chapters()
        s1 = json.dumps(chapters['bs'].nodes['us-gaap:NodeName1'],
                        cls=ForTestJsonEncoder)
        s2 = json.dumps(chapters['bs'].nodes['us-gaap:NodeName1'],
                        cls=ForDBJsonEncoder)
        self.assertEqual(answer1, s1)
        self.assertEqual(answer2, s2)

    def test_dumpschapter(self):
        answer1 = """{"roleuri": "roleuri2", "nodes": {"us-gaap:NodeName2": {"tag": "NodeName2", "version": "us-gaap", "arc": {}, "children": {"mg:NodeName1": {"tag": "NodeName1", "version": "mg", "arc": {"weight": -1.0}, "children": null}, "mg:NodeName2": {"tag": "NodeName2", "version": "mg", "arc": {"weight": 1.0}, "children": {"mg:NodeName3": {"tag": "NodeName3", "version": "mg", "arc": {"weight": 1.0}, "children": null}, "mg:NodeName4": {"tag": "NodeName4", "version": "mg", "arc": {"weight": -1.0}, "children": null}}}}}}, "label": "income statement"}"""
        answer2 = """{"roleuri": "roleuri2", "nodes": {"us-gaap:NodeName2": {"name": "us-gaap:NodeName2", "weight": 1.0, "children": {"mg:NodeName1": {"name": "mg:NodeName1", "weight": -1.0, "children": null}, "mg:NodeName2": {"name": "mg:NodeName2", "weight": 1.0, "children": {"mg:NodeName3": {"name": "mg:NodeName3", "weight": 1.0, "children": null}, "mg:NodeName4": {"name": "mg:NodeName4", "weight": -1.0, "children": null}}}}}}, "label": "income statement"}"""
        chapters = res.make_chapters()
        s1 = json.dumps(chapters['is'],
                        cls=ForTestJsonEncoder)
        s2 = json.dumps(chapters['is'],
                        cls=ForDBJsonEncoder)

        self.assertEqual(answer1, s1)
        self.assertEqual(answer2, s2)

    def test_dumpsall(self):
        chapters = res.make_chapters()
        s1 = json.dumps(chapters,
                        cls=ForTestJsonEncoder,
                        indent=4)
        s2 = json.dumps(chapters,
                        cls=ForDBJsonEncoder,
                        indent=4)

        self.assertEqual(self.test_dumps_db, s2)
        self.assertEqual(self.test_dumps_test, s1)


if __name__ == '__main__':
    unittest.main()
