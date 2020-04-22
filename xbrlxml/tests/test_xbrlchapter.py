import unittest
import lxml
import lxml.etree
import os

from xbrlxml.xbrlchapter import ReferenceParser
from xbrlxml.xbrlchapter import CalcChapter, DimChapter, Chapter
from xbrlxml.xbrlchapter import Node
from algos.xbrljson import ForTestJsonEncoder
from xbrlxml.xbrlchapter import ChapterFactory

import json
import xbrlxml.tests.resource_referenceparser as resource


def make_absolute(path: str) -> str:
    dir_name = os.path.dirname(__file__)
    return os.path.join(dir_name, path)


class TestChapterFactory(unittest.TestCase):
    def test_chapter(self):
        with self.assertRaises(ValueError):
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

        n2 = Node()
        n2.tag = 'tag2'
        n2.version = 'us-gaap'
        n2.name = n2.getname()

        chapter.nodes[n1.name] = n1
        chapter.nodes[n2.name] = n2

        labels = {'nodelabel1': n1.name,
                  'nodelabel2': n2.name}
        return chapter, labels

    def test_updatearc(self):
        chapter, labels = self.make_chapter()
        arc = {'from': 'nodelabel1', 'to': 'nodelabel2',
               'attrib': {'wegth': 1.0, 'order': 1}}

        chapter.update_arc(arc, labels)

        self.assertEqual(id(chapter.nodes['us-gaap:tag2'].parent),
                         id(chapter.nodes['us-gaap:tag1']))
        self.assertDictEqual(chapter.nodes['us-gaap:tag2'].arc, arc['attrib'])

    def test_getnodes(self):
        s = """[{"tag": "us-gaap:tag1"}, {"tag": "us-gaap:tag2", "wegth": 1.0, "order": 1}]"""
        j = json.loads(s)

        chapter, labels = self.make_chapter()

        arc = {'from': 'nodelabel1', 'to': 'nodelabel2',
               'attrib': {'wegth': 1.0, 'order': 1}}

        chapter.update_arc(arc, labels)
        self.assertSequenceEqual(chapter.getnodes(), j)

    def test_gettags(self):
        chapter, labels = self.make_chapter()

        self.assertEqual(chapter.gettags(), {'us-gaap:tag1', 'us-gaap:tag2'})

    def test_DimChapter_dimmembers(self):
        for case in resource.dimchapter_test_cases:
            with self.subTest(i=case['filename']):
                filename = make_absolute('res/xbrlparser/' + case['filename'])
                parser = ReferenceParser(case['ref_type'])
                chapters = parser.parse(filename)

                with self.subTest(i='answer[0]'):
                    dimmems = chapters["http://www.aa.com/role/ConsolidatedBalanceSheets"].dimmembers()
                    self.assertSequenceEqual(
                        dimmems, case['answers'][0])

                with self.subTest(i='answer[1]'):
                    dimmems = chapters["http://www.aa.com/role/FairValueMeasurementsAndOtherInvestmentsSummaryOfAssetsMeasuredAtFairValueOnRecurringBasisDetails"].dimmembers()
                    self.assertSequenceEqual(
                        dimmems, case['answers'][1])

    # @unittest.skip('need better implementation')
    def test_DimChapter_dims(self):
        for case in resource.dimchapter_test_cases:
            with self.subTest(i=case['filename']):
                filename = make_absolute('res/xbrlparser/' + case['filename'])
                parser = ReferenceParser(case['ref_type'])
                chapters = parser.parse(filename)

                with self.subTest(i='answer[2]'):
                    dims = chapters["http://www.aa.com/role/ConsolidatedBalanceSheets"].dims()
                    self.assertSetEqual(
                        set(dims), set(case['answers'][2]))

                with self.subTest(i='answer[3]'):
                    dims = chapters["http://www.aa.com/role/FairValueMeasurementsAndOtherInvestmentsSummaryOfAssetsMeasuredAtFairValueOnRecurringBasisDetails"].dims()
                    self.assertSetEqual(
                        set(dims), set(case['answers'][3]))


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

            self.assertDictEqual(n.asdict(), {'tag': 'us-gaap:Liabilities'})

        with self.subTest(i='arc is not None'):
            n = Node()
            n.version = "us-gaap"
            n.tag = 'Liabilities'
            n.name = n.getname()
            n.arc = {'weight': 1.0, 'order': 1}

            self.assertDictEqual(
                n.asdict(), {
                    'tag': 'us-gaap:Liabilities', 'weight': 1.0, 'order': 1})

    def test_parse_node(self):
        for case in resource.node_test_cases:
            with self.subTest(i=case['filename']):
                filename = make_absolute('res/xbrlparser/' + case['filename'])
                root = lxml.etree.parse(filename)
                parser = ReferenceParser(case['ref_type'])

                locs = [loc for loc in root.iter('{*}loc')]

                index = [1, 100, 56, 34, 78]
                for index, loc in enumerate([locs[i] for i in index]):
                    n, label = parser.parse_node(loc)
                    self.assertDictEqual(
                        n.asdict(),
                        case['answers'][index][0])
                    self.assertEqual(label, case['answers'][index][1])

    def test_parse_arc(self):
        for case in resource.arc_test_cases:
            with self.subTest(i=case['filename']):
                filename = make_absolute('res/xbrlparser/' + case['filename'])
                root = lxml.etree.parse(filename)
                parser = ReferenceParser(case['ref_type'])

                arcs = [arc
                        for arc in root.iter(
                            '{*}' + case['ref_type'] + 'Arc')]
                index = [1, 100, 56, 34, 78]

                for index, arc in enumerate([arcs[i] for i in index]):
                    arcdict = parser.parse_arc(arc)
                    answer = json.loads(case['answers'][index])
                    self.assertDictEqual(arcdict, answer)

    def test_parse_chapter(self):
        for case in resource.chapter_test_cases:
            with self.subTest(i=case['filename']):
                filename = make_absolute('res/xbrlparser/' + case['filename'])
                root = lxml.etree.parse(filename)
                parser = ReferenceParser(case['ref_type'])

                link = [
                    e for e in root.iter(
                        '{*}' +
                        case['ref_type'] +
                        'Link')]
                index = [1, 2, 12, 10, 9]
                for j, link in enumerate([link[i] for i in index]):
                    chapter = parser.parse_chapter(link)
                    answer = case['answers'][j]

                    self.assertEqual(chapter.roleuri, answer['roleuri'])

                    self.assertEqual(len(chapter.nodes), answer['nodes'])

                    if answer['parent_child'][0] != '':
                        p = chapter.nodes[answer['parent_child'][0]]
                        c = chapter.nodes[answer['parent_child'][1]]
                        self.assertEqual(c.parent.name, p.name)

                    if case['ref_type'] == 'calculation' and answer['tag'][0] != '':
                        self.assertEqual(
                            chapter.nodes[answer['tag'][0]].getweight(),
                            answer['tag'][1])


if __name__ == '__main__':
    unittest.main()
