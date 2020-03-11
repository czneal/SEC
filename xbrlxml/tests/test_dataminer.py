import unittest
import unittest.mock

import xbrlxml.dataminer
from xbrlxml.xbrlchapter import CalcChapter


class TestDump(unittest.TestCase):
    def test_dumps_structure(self):
        with self.subTest(test='simple'):
            miner = unittest.mock.MagicMock()

            miner.sheets.mschapters = {'bs': 'roleuri1',
                                       'cf': 'roleuri2',
                                       'is': 'roleuri3'}
            xsd1 = unittest.mock.MagicMock()
            xsd1.label = 'label1'
            xsd2 = unittest.mock.MagicMock()
            xsd2.label = 'label2'

            miner.xbrlfile.schemes = {
                'xsd': {
                    'roleuri1': xsd1,
                    'roleuri2': xsd2},
                'calc': {
                    'roleuri1': CalcChapter('roleuri1'),
                    'roleuri2': CalcChapter('roleuri2')}}

            s = xbrlxml.dataminer._dump_structure(miner)
            self.assertEqual(
                s,
                """{"bs": {"roleuri": "roleuri1", "nodes": {}, "label": "label1"}, "cf": {"roleuri": "roleuri2", "nodes": {}, "label": "label2"}}""")

        with self.subTest(test='CalcChapter absent'):
            miner = unittest.mock.MagicMock()

            miner.sheets.mschapters = {'bs': 'roleuri1',
                                       'cf': 'roleuri2',
                                       'is': 'roleuri3'}
            xsd1 = unittest.mock.MagicMock()
            xsd1.label = 'label1'
            xsd2 = unittest.mock.MagicMock()
            xsd2.label = 'label2'

            miner.xbrlfile.schemes = {
                'xsd': {
                    'roleuri1': xsd1,
                    'roleuri2': xsd2},
                'calc': {
                    'roleuri1': CalcChapter('roleuri1')}
            }

            s = xbrlxml.dataminer._dump_structure(miner)
            self.assertEqual(
                s,
                """{"bs": {"roleuri": "roleuri1", "nodes": {}, "label": "label1"}, "cf": {"roleuri": "roleuri2", "nodes": {}, "label": "label2"}}""")


if __name__ == '__main__':
    unittest.main()
