import unittest
import unittest.mock
import os
import json
import datetime

from typing import Dict, List, Tuple

import xbrlxml.dataminer
import xbrlxml.xbrlrss
from xbrlxml.xbrlchapter import CalcChapter
from xbrlxml.xbrlrss import record_from_str, FileRecord
from utils import add_app_dir, add_root_dir, remove_root_dir
from mysqlio.basicio import MySQLTable, open_connection


MYDEBUG: bool = False
SKIP_LONG = True


def read_report(adsh: str) -> Tuple[xbrlxml.dataminer.NumericDataMiner,
                                    FileRecord,
                                    bool]:

    en = xbrlxml.xbrlrss.MySQLEnumerator()
    en.set_filter_method(
        'explicit', after=datetime.date(
            2013, 1, 1), adsh=adsh)

    records = en.filing_records()
    if not records:
        raise ValueError(f'no such adsh: {adsh}')

    record, file_link = records[0]
    file_link = add_root_dir(file_link)

    dm = xbrlxml.dataminer.NumericDataMiner()
    r = dm.feed(record, file_link)

    return (dm, record, r)


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

            miner.xbrlfile.xsd = {
                'roleuri1': xsd1,
                'roleuri2': xsd2}
            miner.xbrlfile.calc = {
                'roleuri1': CalcChapter('roleuri1'),
                'roleuri2': CalcChapter('roleuri2')}

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

            miner.xbrlfile.xsd = {
                'roleuri1': xsd1,
                'roleuri2': xsd2}
            miner.xbrlfile.calc = {
                'roleuri1': CalcChapter('roleuri1')}

            s = xbrlxml.dataminer._dump_structure(miner)
            self.assertEqual(
                s,
                """{"bs": {"roleuri": "roleuri1", "nodes": {}, "label": "label1"}, "cf": {"roleuri": "roleuri2", "nodes": {}, "label": "label2"}}""")


class TestConsistence(unittest.TestCase):
    @unittest.skipIf(MYDEBUG, 'debug')
    def test_calc_from_dim_fail(self):
        adsh = '0000037996-16-000092'

        record = record_from_str(
            """{"company_name": "FORD MOTOR CO", "form_type": "10-K", "cik": 37996, "sic": 3711, "adsh": "0000037996-16-000092", "period": "2015-12-31", "file_date": "2016-02-11", "fye": "1231", "fy": 2015}""")
        file_link = add_root_dir(
            '2016/02/0000037996-0000037996-16-000092.zip')

        dm = xbrlxml.dataminer.NumericDataMiner()

        r = dm.feed(record, file_link)

        df = dm.numeric_facts

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_NetIncomeLoss_fail(self):
        adsh = '0001564590-18-002832'
        dm, record, r = read_report(adsh)
        self.assertTrue(r)

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

    @unittest.skipIf(SKIP_LONG, 'skip long test')
    def test_backward_compatibility(self):
        res_dir = make_absolute('res/backward')

        adshs: Set[str] = set()
        for root, dirs, filenames in os.walk(res_dir):
            for filename in filenames:
                adshs.add(filename[:20])

        dm = xbrlxml.dataminer.NumericDataMiner()

        for adsh in adshs:
            print(adsh)
            with self.subTest(adsh=adsh):
                with open(os.path.join(res_dir, adsh + '.facts')) as f:
                    facts: Dict[str, float] = json.load(f)

                dm, record, r = read_report(adsh)
                self.assertTrue(r)

                msg = self.check_facts(dm, facts)
                self.assertTrue(len(msg) == 0, msg='\n'.join(msg))

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_find_proper_company_name(self):
        adsh = '0000065100-13-000016'
        dm, record, result = read_report(adsh)
        self.assertTrue(result, msg=f'fail to load report {adsh}')

        self.assertEqual(record.cik, 1051829)
        self.assertEqual(record.company_name,
                         'MERRILL LYNCH PREFERRED CAPITAL TRUST III')

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_mine_dei_for_shares(self):
        with self.subTest('dei shares not found'):
            with unittest.mock.patch('logs.get_logger') as get_logger:
                logger = unittest.mock.MagicMock()
                get_logger.return_value = logger
                adsh = '0001387131-19-002347'
                dm, record, result = read_report(adsh)
                self.assertTrue(result, msg=f'fail to load report {adsh}')

                logger.warning.assert_has_calls(
                    [unittest.mock.call('dei shares not found')])

        with self.subTest('dei shares not found after filter'):
            with unittest.mock.patch('logs.get_logger') as get_logger:
                logger = unittest.mock.MagicMock()
                get_logger.return_value = logger
                adsh = '0001104659-19-011995'
                dm, record, result = read_report(adsh)
                self.assertTrue(result, msg=f'fail to load report {adsh}')

                logger.warning.assert_has_calls(
                    [unittest.mock.call('dei shares not found after filter')])

        with self.subTest('success'):
            adsh = '0001652044-19-000004'
            dm, record, result = read_report(adsh)

            self.assertTrue(result, msg=f'fail to load report {adsh}')
            self.assertEqual(dm.shares_facts.shape, (3, 6))

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_calculate(self):
        with self.subTest('calculation failed, unable calculate chapter without'):
            with unittest.mock.patch('logs.get_logger') as get_logger:
                logger = unittest.mock.MagicMock()
                get_logger.return_value = logger
                adsh = '0001477932-20-000261'
                dm, record, result = read_report(adsh)
                self.assertTrue(result, msg=f'fail to load report {adsh}')

                logger.warning.assert_called()
                kall = logger.warning.call_args
                self.assertTrue('msg' in kall[1])
                self.assertEqual(kall[1]['msg'], 'calculation failed')
                self.assertTrue('extra' in kall[1])
                self.assertTrue(
                    'unable calculate chapter without'
                    in kall[1]['extra']['details'])

        with self.subTest('calculation failed, calc from dim returns none'):
            with unittest.mock.patch('logs.get_logger') as get_logger:
                logger = unittest.mock.MagicMock()
                get_logger.return_value = logger
                adsh = '0001493152-20-000510'
                dm, record, result = read_report(adsh)
                self.assertTrue(result, msg=f'fail to load report {adsh}')

                logger.warning.assert_called()
                kall = logger.warning.call_args
                self.assertTrue('msg' in kall[1])
                self.assertEqual(kall[1]['msg'], 'calculation failed')
                self.assertTrue('extra' in kall[1])
                self.assertTrue(
                    'calc_from_dim returns none'
                    in kall[1]['extra']['details'])

        with self.subTest('calculation failed, calc_from_dim returns more than'):
            with unittest.mock.patch('logs.get_logger') as get_logger:
                logger = unittest.mock.MagicMock()
                get_logger.return_value = logger
                adsh = '0001005276-19-000018'
                dm, record, result = read_report(adsh)
                self.assertTrue(result, msg=f'fail to load report {adsh}')

                logger.warning.assert_any_call(
                    msg='calculation failed',
                    extra={
                        'details': 'calc_from_dim returns more than one value',
                        'fact': 'us-gaap:CostOfGoodsAndServicesSold',
                        'sheet': 'is',
                        'roleuri': 'http://www.mtga.com/role/ConsolidatedStatementsOfIncomeLossAndComprehensiveIncomeLoss'})


class TestPrepare(unittest.TestCase):
    @unittest.skipIf(MYDEBUG, 'debug')
    def test_prepare_report(self):
        adsh = '0000065100-13-000016'
        with self.subTest(adsh=adsh):
            dm, record, result = read_report(adsh)
            self.assertTrue(result, msg=f'fail to load report {dm.adsh}')

            report = xbrlxml.dataminer.prepare_report(dm, record)
            self.assertEqual(report['cik'], 1051829)
            self.assertEqual(dm.xbrlfile.period, report['period'])
            self.assertEqual(adsh, report['adsh'])
            self.assertEqual(
                report['file_link'],
                remove_root_dir(
                    dm.zip_filename))

            self.assertSetEqual(set(report.keys()), {'adsh', 'cik', 'period',
                                                     'period_end',
                                                     'fin_year',
                                                     'taxonomy',
                                                     'form',
                                                     'quarter',
                                                     'file_date',
                                                     'file_link',
                                                     'trusted',
                                                     'structure',
                                                     'contexts'})

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_prepare_nums(self):
        adsh = '0000065100-13-000016'
        with self.subTest(adsh=adsh):
            dm, record, result = read_report(adsh)
            self.assertTrue(result, msg=f'fail to load report {dm.adsh}')

            nums = xbrlxml.dataminer.prepare_nums(dm)
            con = open_connection()
            table = MySQLTable('mgnums', con)
            con.close()

            self.assertTrue(table.fields.issubset(nums.columns))

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_prepare_company(self):
        adsh = '0000065100-13-000016'
        with self.subTest(adsh=adsh):
            dm, record, result = read_report(adsh)
            self.assertTrue(result, msg=f'fail to load report {dm.adsh}')

            company = xbrlxml.dataminer.prepare_company(dm, record)
            con = open_connection()
            table = MySQLTable('companies', con)
            con.close()

            self.assertTrue(table.fields.issubset(company.keys()))

    @unittest.skipIf(MYDEBUG, 'debug')
    def test_prepare_share(self):
        adsh = '0001652044-19-000004'
        with self.subTest(adsh=adsh):
            dm, record, result = read_report(adsh)
            self.assertTrue(result, msg=f'fail to load report {dm.adsh}')

            shares = xbrlxml.dataminer.prepare_shares(dm)
            con = open_connection()
            table = MySQLTable('sec_shares', con)
            con.close()

            self.assertTrue(table.fields_not_null.issubset(shares.keys()))


def make_absolute(path: str) -> str:
    dir_name = os.path.dirname(__file__)
    return os.path.join(dir_name, path)


if __name__ == '__main__':
    unittest.main()
