# -*- coding: utf-8 -*-
import unittest
import unittest.mock as mock
import os
import shutil
from parameterized import parameterized  # type: ignore

import xbrldown.download
from xbrlxml.xbrlzip import XBRLZipPacket
from xbrlxml.xbrlexceptions import XbrlException


def make_absolute(path: str) -> str:
    dir_name = os.path.dirname(__file__)
    return os.path.join(dir_name, path)


class TestProcs(unittest.TestCase):
    @parameterized.expand([
        ('xbrldown_good.zip', None),
        ('xbrldown_noxbrl.zip', XbrlException),
        ('xbrldown_noxsd.zip', XbrlException),
        ('xbrldown_noxsdxbrl.zip', XbrlException),
        ('xbrldown_nocal.zip', None),
        ('xbrldown_nopre.zip', XbrlException)])
    def test_check_zip_file(self, zipfilename, cls_):
        filename = os.path.join(make_absolute('res'), zipfilename)

        if cls_ is None:
            self.assertTrue(
                xbrldown.download.check_zip_file(filename) is None)
        else:
            with self.assertRaises(cls_):
                xbrldown.download.check_zip_file(filename)

    def test_download_sec_rss(self):
        filename = os.path.join(TestProcs.test_dir, '2019/05/rss-2019-05.xml')

        with unittest.mock.patch('settings.Settings.root_dir') as root_dir:
            root_dir.return_value = TestProcs.test_dir
            xbrldown.download.download_rss(2019, 5)

            self.assertTrue(os.path.exists(filename))

        with unittest.mock.patch('settings.Settings.root_dir') as root_dir, \
                unittest.mock.patch('urllib3.PoolManager') as http:
            root_dir.return_value = TestProcs.test_dir
            http.return_value.request.side_effect = Exception('test')

            with self.assertRaises(XbrlException):
                xbrldown.download.download_rss(2019, 5)

    def test_download_files_from_sec(self):
        cik = 1467858
        adsh = '0001467858-14-000043'
        files = xbrldown.download.download_files_from_sec(cik, adsh)

        self.assertEqual(len(files), 6)
        self.assertTrue(files['xbrl'][0] == 'gm-20131231.xml')
        self.assertTrue(files['xsd'][1] is not None)

    def test_save_zip(self):
        cik = 1467858
        adsh = '0001467858-14-000043'
        files = xbrldown.download.download_files_from_sec(cik, adsh)

        packet = XBRLZipPacket()
        packet.save_packet(
            files,
            os.path.join(
                TestProcs.test_dir,
                'test_save_packet.zip'))

    @classmethod
    def setUpClass(cls):
        cls.test_dir = make_absolute('res/tmp')
        if not os.path.exists(cls.test_dir):
            os.mkdir(cls.test_dir)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_dir):
            for i in range(10):
                try:
                    shutil.rmtree(make_absolute('res/tmp'))
                    break
                except Exception:
                    continue


if __name__ == '__main__':
    unittest.main()
