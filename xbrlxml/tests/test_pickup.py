import unittest
import unittest.mock
import datetime as dt
import pandas as pd

import xbrlxml.pickup
from xbrlxml.xbrlfile import XbrlFile, XbrlException
from xbrlxml.xbrlfileparser import Context
from xbrlxml.xbrlchapter import Chapter
from xbrlxml.xsdfile import XSDChapter


class TestContextChooser(unittest.TestCase):
    def make_df_contexts(self):
        contexts = {}
        c = Context()
        c.contextid = 'context1'
        c.edate = dt.datetime(2018, 1, 1)
        c.entity = 1000
        contexts[c.contextid] = c

        c = Context()
        c.contextid = 'context2'
        c.edate = dt.datetime(2018, 1, 1)
        c.entity = 1000
        c.dim.append('us-gaap:FirstAxis')
        c.member.append('us-gaap:FirstMember')
        contexts[c.contextid] = c

        c = Context()
        c.contextid = 'context3'
        c.edate = dt.datetime(2018, 1, 1)
        c.entity = 1000
        c.dim.append('us-gaap:FirstAxis')
        c.member.append('us-gaap:SuccessorMember')
        contexts[c.contextid] = c

        c = Context()
        c.contextid = 'context4'
        c.edate = dt.datetime(2018, 1, 1)
        c.entity = 1000
        c.dim.append('us-gaap:FirstAxis')
        c.member.append('us-gaap:ParentMember')
        contexts[c.contextid] = c

        return contexts

    def test_choosecontext(self):
        xbrlfile = unittest.mock.MagicMock()
        xbrlfile.contexts = self.make_df_contexts()
        cntx = xbrlxml.pickup.ContextChooser(xbrlfile)

        with self.subTest(i='shape[0] == 0'):
            f = pd.DataFrame(data=None, columns=['context', 'cnt'])
            self.assertIsNone(cntx._choosecontext(f, None))

        with self.subTest(i='only nondim'):
            f = pd.DataFrame(
                data=[['context1', 50]],
                columns=['context', 'cnt'])

            self.assertEqual(cntx._choosecontext(f), 'context1')

        with self.subTest(i='only successor'):
            f = pd.DataFrame(
                data=[['context3', 50]],
                columns=['context', 'cnt'])

            self.assertEqual(cntx._choosecontext(f), 'context3')

        with self.subTest(i='only parent'):
            f = pd.DataFrame(
                data=[['context4', 50]],
                columns=['context', 'cnt'])

            self.assertEqual(cntx._choosecontext(f), 'context4')

        with self.subTest(i='nondim/top > 0.5'):
            f = pd.DataFrame(
                data=[['context2', 50],
                      ['context1', 40]],
                columns=['context', 'cnt'])

            self.assertEqual(cntx._choosecontext(f), 'context1')

        with self.subTest(i='nondim/top < 0.5'):
            f = pd.DataFrame(
                data=[['context2', 50],
                      ['context1', 20]],
                columns=['context', 'cnt'])
            self.assertEqual(cntx._choosecontext(f), 'context2')

        with self.subTest(i='no nondim'):
            f = pd.DataFrame(
                data=[['context3', 50],
                      ['context2', 20]],
                columns=['context', 'cnt'])
            self.assertEqual(cntx._choosecontext(f), 'context3')

        with self.subTest(i='nondim/top>0.5 and top==successor'):
            f = pd.DataFrame(
                data=[['context3', 50],
                      ['context1', 30]],
                columns=['context', 'cnt'])
            self.assertEqual(cntx._choosecontext(f,), 'context3')

        with self.subTest(i='all present'):
            f = pd.DataFrame(
                data=[['context2', 50],
                      ['context1', 40],
                      ['context3', 30],
                      ['context4', 20]],
                columns=['context', 'cnt'])
            self.assertEqual(cntx._choosecontext(f), 'context3')

        with self.subTest(i='return top if ather less it'):
            f = pd.DataFrame(
                data=[['context2', 50],
                      ['context1', 10],
                      ['context3', 3],
                      ['context4', 2]],
                columns=['context', 'cnt'])
            self.assertEqual(cntx._choosecontext(f), 'context2')


class TestChapterChooser(unittest.TestCase):
    def test_findmschapters(self):
        with self.subTest(i="raise count of chapters > 3"):
            with unittest.mock.patch('xbrlxml.xbrlchapter.Chapter.gettags') as mgettags:
                mgettags.return_value = set('a')
                xbrl = XbrlFile()
                xbrl.pres = {'roleuri1': Chapter(),
                                        'roleuri2': Chapter(),
                                        'roleuri3': Chapter(),
                                        'roleuri4': Chapter()}
                xbrl.xsd = {
                    'roleuri1': XSDChapter(
                        'roleuri1', 'Balance Sheet 1', 'sta', 'id1'), 'roleuri2': XSDChapter(
                        'roleuri2', 'Balance Sheet 2', 'sta', 'id2'), 'roleuri3': XSDChapter(
                        'roleuri3', 'Income Statement', 'sta', 'id3'), 'roleuri4': XSDChapter(
                        'roleuri4', 'cash Flow', 'sta', 'id4'), }
                chapter_ch = xbrlxml.pickup.ChapterChooser(xbrl)

                self.assertRaises(XbrlException, chapter_ch.choose)

        with self.subTest(i="pass"):
            with unittest.mock.patch('xbrlxml.xbrlchapter.Chapter.gettags') as mgettags:
                mgettags.return_value = set('a')
                xbrl = XbrlFile()
                xbrl.pres = {'roleuri1': Chapter(),
                                        'roleuri2': Chapter(),
                                        'roleuri3': Chapter(),
                                        'roleuri4': Chapter()}
                xbrl.xsd = {
                    'roleuri1': XSDChapter(
                        'roleuri1', 'Balance Sheet [parenthical]', 'sta', 'id1'),
                    'roleuri2': XSDChapter(
                        'roleuri2', 'Balance Sheet 2', 'sta', 'id2'),
                    'roleuri3': XSDChapter(
                        'roleuri3', 'Income Statement', 'sta', 'id3'),
                    'roleuri4': XSDChapter(
                        'roleuri4', 'cash Flow', 'sta', 'id4'), }

                chapter_ch = xbrlxml.pickup.ChapterChooser(xbrl)

                self.assertTrue(len(chapter_ch.choose()) == 3)


if __name__ == '__main__':
    unittest.main()
