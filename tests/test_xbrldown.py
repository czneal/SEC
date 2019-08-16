# -*- coding: utf-8 -*-
import unittest
import os
from parameterized import parameterized

import xbrldown.download
from settings import Settings
from xbrlxml.xbrlexceptions import XbrlException

class TestProcs(unittest.TestCase):
    @parameterized.expand([
            ('xbrldown_good.zip', None),
            ('xbrldown_noxbrl.zip', XbrlException),
            ('xbrldown_noxsd.zip', XbrlException),
            ('xbrldown_noxbrlxsd.zip', XbrlException),
            ('xbrldown_nocal.zip', None),
            ('xbrldown_nopre.zip', XbrlException)])
    def test_check_zip_file(self, zipfilename, cls_):
        test_dir = Settings.app_dir() + 'tests/resources/'
        if cls_ is None:
            xbrldown.download.check_zip_file(test_dir + zipfilename)
        else:
            with self.assertRaises(cls_):
                xbrldown.download.check_zip_file(test_dir + zipfilename)
                
    def test_download_sec_rss(self):
        test_dir = Settings.app_dir() + 'tests/resources/'
        file_dir = test_dir + '2019/05/' 
        filename = file_dir + 'rss-2019-05.xml'
            
        with unittest.mock.patch('settings.Settings.root_dir') as root_dir:            
            root_dir.return_value = test_dir
            
            xbrldown.download.download_rss(2019, 5)
            
            self.assertTrue(os.path.exists(filename))
            
        with unittest.mock.patch('settings.Settings.root_dir') as root_dir, \
             unittest.mock.patch('urllib3.PoolManager') as http:
                 root_dir.return_value = test_dir
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
        
        from xbrlxml.xbrlzip import XBRLZipPacket
        packet = XBRLZipPacket()
        packet.save_packet(files, 
                           Settings.app_dir() + 
                           '/tests/resources/test_save_packet.zip')
    
    def tearDown(self):
        test_dir = Settings.app_dir() + 'tests/resources/'
        file_dir = test_dir + '2019/05/' 
        filename = file_dir + 'rss-2019-05.xml'
        
        if os.path.exists(filename):
            os.remove(filename)
        if os.path.exists(file_dir):
            os.rmdir(file_dir)
        
        if os.path.exists(Settings.app_dir() + 
                           '/tests/resources/test_save_packet.zip'):
            os.remove(Settings.app_dir() + 
                           '/tests/resources/test_save_packet.zip')
            
if __name__ == '__main__':
    unittest.main()
