import unittest

from mysqlio.indio import IndicatorsWriter, MySQLIndicatorFeeder
from mysqlio.tests.dbtest import DBTestBase  # type: ignore


class TestWriteIndicators(DBTestBase):
    queries = [
        "truncate table indicators",
        "truncate table ind_proc_info",
        "truncate table ind_rest_info",
        "truncate table ind_classified_pairs"]

    def setUp(self):
        self.run_set_up_queries(self.queries, params=[{}, {}, {}, {}])

    def tearDown(self):
        self.run_set_up_queries(self.queries, params=[{}, {}, {}, {}])

    def test_write_indicators(self):
        data = [{'name': 'mg_equity',
                 'adsh': '00000000',
                 'cik': 1,
                 'value': 12345.67,
                 'fy': 2019},
                {'name': 'mg_laibilities',
                 'adsh': '00000000',
                 'cik': 1,
                 'value': 12345.67,
                 'fy': 2018},
                {'name': 'mg_equity',
                 'adsh': '00000001',
                 'cik': 2,
                 'value': 12345.67,
                 'fy': 2017}]

        iw = IndicatorsWriter()
        iw.write(data)
        iw.flush()
        iw.close()

        ir = MySQLIndicatorFeeder()
        with self.subTest(test='all'):
            data = ir.fetch_indicators(1, 2019, 3)
            self.assertDictEqual(
                data, {
                    'mg_equity': {
                        2019: 12345.67}, 'mg_laibilities': {
                        2018: 12345.67}})

        with self.subTest(test='deep'):
            data = ir.fetch_indicators(2, 2019, 2)
            self.assertDictEqual(data, {})

        ir.close()

    def test_write_info(self):
        with self.subTest(test='empty sets'):
            iw = IndicatorsWriter()
            iw.write_ind_info([], [])
            iw.flush()
            iw.close()

            ir = MySQLIndicatorFeeder()
            proc, rest = ir.fetch_ind_info()
            self.assertSequenceEqual(proc, [])
            self.assertSequenceEqual(rest, [])
            ir.close()

        with self.subTest(test='how it works'):
            iw = IndicatorsWriter()
            proc = [{'name': 'name1',
                     'dp': '["us-gaap:Laibilities"]',
                     'deep': 1},
                    {'name': 'name2',
                     'dp': '["us-gaap:Assets"]',
                     'deep': 2}]
            rest = [{'name': 'name3',
                     'model_name': 'simple_model',
                     'model_id': 0,
                     'class_id': 0,
                     'chapter': 'bs',
                     'nodes': "['us-gaap:Liabilities', 'us-gaap:Assets']"},
                    {'name': 'name4',
                     'model_name': 'complex_model',
                     'model_id': 0,
                     'class_id': 1,
                     'chapter': 'bs',
                     'nodes': "['us-gaap:Liabilities', 'us-gaap:Assets']"}]
            iw.write_ind_info(proc, rest)
            iw.flush()
            iw.close()

            ir = MySQLIndicatorFeeder()
            db_proc, db_rest = ir.fetch_ind_info()
            ir.close()

            self.assertSequenceEqual(proc, db_proc)
            self.assertSequenceEqual(rest, db_rest)

    def test_write_pairs(self):
        iw = IndicatorsWriter()
        ir = MySQLIndicatorFeeder()

        iw.write_classified_pairs(
            [{'parent': 'someTag', 'child': 'anotherTag', 'model_id': 1, 'label': 0},
             {'parent': 'someTag', 'child': 'anotherTag', 'model_id': 1, 'label': 1},
             {'parent': 'someTag', 'child': 'anotherTag1', 'model_id': 1, 'label': 0},
             {'parent': 'someTag', 'child': 'anotherTag1', 'model_id': 0, 'label': 0}])
        iw.flush()
        iw.close()

        pairs = ir.fetch_classified_pairs()
        ir.close()
        self.assertEqual(len(pairs), 3)


class TestReader(DBTestBase):
    def set_up_fetch_snp500_ciks(self):
        queries = [
            "truncate table stocks_index",
            """
            insert into stocks_index
            select * from reports.stocks_index
            """]
        self.run_set_up_queries(queries, params=[{}, {}])

    def set_up_fetch_facts(self):
        queries = [
            "delete from mgnums where adsh = '0001213900-19-003416'",
            """
            insert into mgnums
            select * from reports.mgnums where adsh = '0001213900-19-003416'
            """]
        self.run_set_up_queries(queries, params=[{}, {}])

    def set_up_fetch_indicator_data(self):
        queries = [
            "delete from reports where cik = 1487843",
            """
            delete from mgnums
            where adsh in (select adsh from reports.reports where cik = 1487843)""",
            """
            insert into reports
            select * from reports.reports
            where cik = 1487843
            """,
            """
            insert into mgnums
            select * from reports.mgnums
            where adsh in (select adsh from reports.reports where cik = 1487843)
            """]

        self.run_set_up_queries(queries, [{}, {}, {}, {}])

    def test_fetch_snp500_ciks(self):
        self.set_up_fetch_snp500_ciks()

        ir = MySQLIndicatorFeeder()
        ciks = ir.fetch_snp500_ciks(2018)

        # Berkshire in ciks
        self.assertTrue(1067983 in ciks)
        ir.close()

    def test_fetch_facts(self):
        self.set_up_fetch_facts()

        r = MySQLIndicatorFeeder()

        facts = r.fetch_facts('0001213900-19-003416')

        self.assertTrue('us-gaap:Liabilities' in facts)
        self.assertTrue('us-gaap:Assets' in facts)

    def test_fetch_indicator_data(self):
        self.set_up_fetch_indicator_data()

        r = MySQLIndicatorFeeder()
        with self.subTest(test='real data'):
            chapters, fy_adsh = r.fetch_indicator_data(
                cik=1487843, fy=2018, deep=4)

            self.assertEqual(len(fy_adsh), 4)
            self.assertEqual(len(chapters), 4)

            self.assertEqual(fy_adsh[2017], '0001213900-18-006738')
            self.assertEqual(len(chapters[2017]), 4)

        with self.subTest(test='unexisted data 1'):
            chapters, fy_adsh = r.fetch_indicator_data(
                cik=1487843, fy=1999, deep=4)
            self.assertEqual(chapters, {})
            self.assertEqual(fy_adsh, {})

        with self.subTest(test='unexisted data 2'):
            chapters, fy_adsh = r.fetch_indicator_data(
                cik=-1, fy=2018, deep=4)
            self.assertEqual(chapters, {})
            self.assertEqual(fy_adsh, {})

    def test_combination(self):
        self.set_up_fetch_indicator_data()

        r = MySQLIndicatorFeeder()
        _, fy_adsh = r.fetch_indicator_data(
            cik=1487843, fy=2018, deep=4)
        self.assertTrue(len(fy_adsh) != 0)

        nums = r.fetch_nums(fy_adsh)
        for fy, _ in fy_adsh.items():
            self.assertTrue(len(nums[fy]) != 0)


if __name__ == '__main__':
    unittest.main()
