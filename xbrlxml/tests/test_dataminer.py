import unittest
import unittest.mock
import os
import json

from typing import Dict, List

import xbrlxml.dataminer
from xbrlxml.xbrlchapter import CalcChapter
from xbrlxml.xbrlrss import record_from_str
from utils import add_app_dir, add_root_dir


MYDEBUG: bool = False


class TestDump(unittest.TestCase):
    @unittest.skipIf(MYDEBUG, 'debug')
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


class TestConsistence(unittest.TestCase):
    @unittest.skipIf(not MYDEBUG, 'debug')
    def test_calc_from_dim_fail(self):
        adsh = '0000037996-16-000092'

        record = record_from_str(
            """{"company_name": "FORD MOTOR CO", "form_type": "10-K", "cik": 37996, "sic": 3711, "adsh": "0000037996-16-000092", "period": "2015-12-31", "file_date": "2016-02-11", "fye": "1231", "fy": 2015}""")
        file_link = add_root_dir(
            '2016/02/0000037996-0000037996-16-000092.zip')

        dm = xbrlxml.dataminer.NumericDataMiner()

        dm.feed(record.__dict__, file_link)

        df = dm.numeric_facts

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_NetIncomeLoss_fail(self):
        with open(make_absolute('res/0000097476-0001564590-18-002832.json')) as f:
            record = record_from_str(f.read())
            file_link = make_absolute(
                'res/0000097476-0001564590-18-002832.zip')

        dm = xbrlxml.dataminer.NumericDataMiner()

        dm.feed(record.__dict__, file_link)

        df = dm.numeric_facts
        self.assertEqual(
            df[df['name'] == 'us-gaap:NetIncomeLoss'].iloc[0]['value'],
            3682000000.0)
        self.assertEqual(
            df[df['name'] == 'us-gaap:Liabilities'].iloc[0]
            ['value'],
            7305000000.0)
        self.assertEqual(
            df[df['name'] == 'us-gaap:Assets'].iloc[0]['value'],
            17642000000.0)
        self.assertEqual(df.shape[0], 95)

    def check_facts(
            self, dm: xbrlxml.dataminer.NumericDataMiner,
            facts: Dict[str, float]) -> List[str]:
        df = dm.numeric_facts
        if df is None:
            return ['facts not parsed']

        msg: List[str] = []
        if df.shape[0] != len(facts):
            msg.append(f'facts count differ {df.shape[0]}, {len(facts)}')

        names = set(df['name'].unique())
        for fact, value in facts.items():
            if fact not in names:
                msg.append(f'{fact} not parsed now')
                continue

            f = df[df['name'] == fact]
            if f.shape[0] != 1:
                msg.append(f'only one instnace of {fact} should be parsed')
                continue
            new = f.iloc[0]['value']
            if new != value:
                msg.append(f'value for {fact} differ old: {value}, new:{new}')

        diff = names.difference(facts)
        if diff:
            msg.append(f'new facts parsed {diff}')

        return msg

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_backward_compatibility(self):
        res_dir = make_absolute('res/backward')
        with open(os.path.join(res_dir, 'adshs.json')) as f:
            adshs: List[str] = json.load(f)

        dm = xbrlxml.dataminer.NumericDataMiner()

        for adsh in adshs:
            print(adsh)
            with self.subTest(adsh=adsh):
                with open(os.path.join(res_dir, adsh + '.facts')) as f:
                    facts: Dict[str, float] = json.load(f)
                with open(os.path.join(res_dir, adsh + '.record')) as f:
                    record = record_from_str(f.read())

                r = dm.feed(
                    record.__dict__, os.path.join(
                        res_dir, adsh + '.zip'))
                self.assertTrue(r)

                msg = self.check_facts(dm, facts)
                self.assertTrue(len(msg) == 0, msg='\n'.join(msg))


def make_absolute(path: str) -> str:
    dir_name = os.path.dirname(__file__)
    return os.path.join(dir_name, path)


if __name__ == '__main__':
    unittest.main()
