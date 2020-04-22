# -*- coding: utf-8 -*-

import unittest
import json

import algos.scheme
from algos.xbrljson import ForTestJsonEncoder

import algos.tests.resource_chapters as res


class TestExtention(unittest.TestCase):
    def test_extend_calc_scheme(self):
        with self.subTest(i='extend done'):
            scheme = res.make_chapters()
            scheme['roleuri1'] = scheme['bs']
            scheme['roleuri2'] = scheme['is']
            algos.scheme.extend_clac_scheme(
                'roleuri1', scheme,
                extentions={'us-gaap:NodeName2': 'roleuri2'})

            self.assertDictEqual(
                scheme['bs'].nodes['us-gaap:NodeName2'].children,
                scheme['is'].nodes['us-gaap:NodeName2'].children)

    def test_findcalcextentions(self):
        scheme = res.make_chapters()
        scheme['roleuri1'] = scheme['bs']
        scheme['roleuri2'] = scheme['is']

        extentions, warnings = algos.scheme.find_extentions(
            roleuri='roleuri1',
            calc=scheme,
            pres=['roleuri1', 'roleuri2'],
            xsds=['roleuri1', 'roleuri2'])
        self.assertTrue(len(warnings) == 0)
        self.assertDictEqual(extentions, {'us-gaap:NodeName2': 'roleuri2'})


class TestNodeCopy(unittest.TestCase):
    def test_makenodecopy(self):
        with self.subTest(i='copy Node'):
            chapters = res.make_chapters()

            n = chapters['bs'].nodes['us-gaap:NodeName0']
            node = n.copy()
            self.assertNotEqual(id(node), id(n))
            self.assertEqual(node.children['us-gaap:NodeName1'].parent,
                             node)
            self.assertEqual(
                node.children['us-gaap:NodeName1'].children
                ['us-gaap:NodeName3'].parent, node.children
                ['us-gaap:NodeName1'])
            self.assertEqual(json.dumps(n, cls=ForTestJsonEncoder),
                             json.dumps(node, cls=ForTestJsonEncoder))
        with self.subTest(i='raise AssertionError'):
            self.assertRaises(AssertionError, algos.scheme.makenodecopy, 'asd')


class TestEnumeration(unittest.TestCase):
    def test_enum(self):
        chapters = res.make_chapters()
        with self.subTest(i='enum node by name'):
            etalon = [
                ['us-gaap:NodeName0', 'us-gaap:NodeName1', 1.0, 0, False],
                ['us-gaap:NodeName1', 'us-gaap:NodeName3', 1.0, 1, True],
                ['us-gaap:NodeName1', 'us-gaap:NodeName4', -1.0, 1, True],
                ['us-gaap:NodeName0', 'us-gaap:NodeName2', -1.0, 0, True]]
            data = []
            for item in algos.scheme.enum(
                    chapters['bs'].nodes['us-gaap:NodeName0']):
                data.append(item)

            self.assertListEqual(etalon, data)

        with self.subTest(i='enum node by tag'):
            etalon = [['NodeName0', 'NodeName1', 1.0, 0, False],
                      ['NodeName1', 'NodeName3', 1.0, 1, True],
                      ['NodeName1', 'NodeName4', -1.0, 1, True],
                      ['NodeName0', 'NodeName2', -1.0, 0, True]]
            data = []
            for item in algos.scheme.enum(
                    chapters['bs'].nodes['us-gaap:NodeName0'],
                    func=lambda x: x.tag):
                data.append(item)

            self.assertListEqual(etalon, data)

        with self.subTest(i='enum Chapter by name'):
            etalon = [['roleuri2', 'us-gaap:NodeName2', 1.0, 0, False],
                      ['us-gaap:NodeName2', 'mg:NodeName1', -1.0, 1, True],
                      ['us-gaap:NodeName2', 'mg:NodeName2', 1.0, 1, False],
                      ['mg:NodeName2', 'mg:NodeName3', 1.0, 2, True],
                      ['mg:NodeName2', 'mg:NodeName4', -1.0, 2, True]]
            data = []
            for item in algos.scheme.enum(chapters['is']):
                data.append(item)

            self.assertListEqual(etalon, data)

        with self.subTest(i='enum Chapter by tag'):
            etalon = [['roleuri1', 'NodeName0', 1.0, 0, False],
                      ['NodeName0', 'NodeName1', 1.0, 1, False],
                      ['NodeName1', 'NodeName3', 1.0, 2, True],
                      ['NodeName1', 'NodeName4', -1.0, 2, True],
                      ['NodeName0', 'NodeName2', -1.0, 1, True]]
            data = []
            for item in algos.scheme.enum(chapters['bs'],
                                          func=lambda x: x.tag):
                data.append(item)

            self.assertListEqual(etalon, data)

        with self.subTest(i='enum chapters by tag'):
            etalon = [['roleuri1', 'NodeName0', 1.0, 0, False],
                      ['NodeName0', 'NodeName1', 1.0, 1, False],
                      ['NodeName1', 'NodeName3', 1.0, 2, True],
                      ['NodeName1', 'NodeName4', -1.0, 2, True],
                      ['NodeName0', 'NodeName2', -1.0, 1, True],
                      ['roleuri2', 'NodeName2', 1.0, 0, False],
                      ['NodeName2', 'NodeName1', -1.0, 1, True],
                      ['NodeName2', 'NodeName2', 1.0, 1, False],
                      ['NodeName2', 'NodeName3', 1.0, 2, True],
                      ['NodeName2', 'NodeName4', -1.0, 2, True]]
            data = []
            for item in algos.scheme.enum(chapters,
                                          func=lambda x: x.tag):
                data.append(item)

            self.assertListEqual(etalon, data)

        with self.subTest(i='enum chapters by tag only child'):
            etalon = [
                ['us-gaap:NodeName0'],
                ['us-gaap:NodeName1'],
                ['us-gaap:NodeName3'],
                ['us-gaap:NodeName4'],
                ['us-gaap:NodeName2'],
                ['us-gaap:NodeName2'],
                ['mg:NodeName1'],
                ['mg:NodeName2'],
                ['mg:NodeName3'],
                ['mg:NodeName4']]
            data = []
            for item in algos.scheme.enum(chapters, outpattern='c',
                                          func=lambda x: x.name):
                data.append(item)

            self.assertListEqual(etalon, data)

        with self.subTest(i='enum raise AssertionError'):
            with self.assertRaises(AssertionError):
                for item in algos.scheme.enum(chapters, outpattern='pcwr'):
                    pass

        with self.subTest(i='enum chapters by tag only child and leaf'):
            etalon = [
                ['NodeName3', True],
                ['NodeName4', True],
                ['NodeName2', True],
                ['NodeName1', True],
                ['NodeName3', True],
                ['NodeName4', True]]
            data = []
            for item in algos.scheme.enum(chapters, outpattern='cl', leaf=True,
                                          func=lambda x: x.tag):
                data.append(item)
            self.assertListEqual(etalon, data)


if __name__ == '__main__':
    unittest.main()
