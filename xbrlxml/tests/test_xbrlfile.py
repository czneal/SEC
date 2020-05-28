# -*- coding: utf-8 -*-

import unittest
import unittest.mock
import datetime as dt

from xbrlxml.xbrlfile import XbrlFile
from xbrlxml.xbrlfileparser import Context
from xbrlxml.xbrlfile import XbrlFiles
from xbrlxml.xbrlfile import XbrlException
from xbrlxml.xbrlrss import FileRecord


class TestXbrlFile(unittest.TestCase):
    def test_readschemefiles(self):
        with self.subTest(subtest='no xsd'):
            xbrl = XbrlFile()
            files = XbrlFiles(
                xbrl='xbrlxml/tests/xbrlxml/tests/res/xbrlfile/gen-20121231.xml', xsd='',
                pres='xbrlxml/tests/xbrlxml/tests/res/xbrlfile/gen-20121231_pre.xml',
                defi='xbrlxml/tests/xbrlxml/tests/res/xbrlfile/gen-20121231_def.xml',
                calc='xbrlxml/tests/xbrlxml/tests/res/xbrlfile/gen-20121231_cal.xml')
            self.assertRaises(
                XbrlException,
                xbrl._XbrlFile__read_scheme_files,
                files=files)

        with self.subTest(subtest='no pre'):
            xbrl = XbrlFile()
            files = XbrlFiles(
                xbrl='xbrlxml/tests/res/xbrlfile/gen-20121231.xml',
                xsd='xbrlxml/tests/res/xbrlfile/gen-20121231.xsd',
                pres=None,
                defi='xbrlxml/tests/res/xbrlfile/gen-20121231_def.xml',
                calc='xbrlxml/tests/res/xbrlfile/gen-20121231_cal.xml')
            self.assertRaises(
                XbrlException,
                xbrl._XbrlFile__read_scheme_files,
                files=files)

        with self.subTest(subtest='no def'):
            xbrl = XbrlFile()
            files = XbrlFiles(
                xbrl='xbrlxml/tests/res/xbrlfile/gen-20121231.xml',
                xsd='xbrlxml/tests/res/xbrlfile/gen-20121231.xsd',
                pres='xbrlxml/tests/res/xbrlfile/gen-20121231_pre.xml',
                defi='',
                calc='xbrlxml/tests/res/xbrlfile/gen-20121231_cal.xml')
            self.assertEqual(
                xbrl._XbrlFile__read_scheme_files(
                    files=files), None)
            self.assertTrue(xbrl.defi == {})

        with self.subTest(subtest='no calc'):
            xbrl = XbrlFile()
            files = XbrlFiles(
                xbrl='xbrlxml/tests/res/xbrlfile/gen-20121231.xml',
                xsd='xbrlxml/tests/res/xbrlfile/gen-20121231.xsd',
                pres='xbrlxml/tests/res/xbrlfile/gen-20121231_pre.xml',
                defi='xbrlxml/tests/res/xbrlfile/gen-20121231_def.xml',
                calc='')
            self.assertEqual(
                xbrl._XbrlFile__read_scheme_files(
                    files=files), None)
            self.assertTrue(xbrl.calc == {})

    def test_findperiod(self):
        c = Context()
        c.contextid = 'context'
        c.dim = [None]
        contexts = {c.contextid: c}

        with self.subTest(i='dei is none'):
            record = FileRecord()
            xbrl = XbrlFile()
            xbrl.dei.period = [(dt.date(2012, 1, 1), 'context')]
            xbrl.contexts = contexts
            with self.assertRaises(XbrlException):
                xbrl._XbrlFile__find_period(record)

        with self.subTest(i='period is none'):
            record = FileRecord()
            record.period = dt.date(2012, 12, 1)

            xbrl = XbrlFile()
            xbrl.contexts = contexts

            with self.assertRaises(XbrlException):
                xbrl._XbrlFile__find_period(record)

        with self.subTest(i='period and dei doesnt match'):
            record = FileRecord()
            record.period = dt.date(2012, 12, 1)

            xbrl = XbrlFile()
            xbrl.dei.period = [(dt.date(2013, 1, 12), 'context')]
            xbrl.contexts = contexts

            with self.assertRaises(XbrlException):
                xbrl._XbrlFile__find_period(record)

        with self.subTest(i='period and dei match'):
            record = FileRecord()
            record.period = dt.date(2012, 12, 1)

            xbrl = XbrlFile()
            xbrl.dei.period = [(dt.date(2012, 12, 1), 'context')]
            xbrl.contexts = contexts

            xbrl._XbrlFile__find_period(record)
            self.assertEqual(dt.date(2012, 12, 1), xbrl.period)

        c = Context()
        c.contextid = 'context1'
        c.dim = [None, 'dimention']
        contexts[c.contextid] = c
        with self.subTest(i='many contexts'):
            record = FileRecord()
            record.period = dt.date(2012, 12, 1)

            xbrl = XbrlFile()
            xbrl.dei.period = [(dt.date(2012, 12, 1), 'context'),
                               (dt.date(2013, 12, 1), 'context1')]
            xbrl.contexts = contexts

            xbrl._XbrlFile__find_period(record)
            self.assertEqual(dt.date(2012, 12, 1), xbrl.period)


if __name__ == '__main__':
    unittest.main()
