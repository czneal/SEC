import unittest

from mysqlio.indio import IndicatorsWriter, MySQLIndicatorFeeder


class TestWriteIndicators(unittest.TestCase):
    def tearDown(self):
        import mysqlio.basicio as do
        with do.OpenConnection() as con:
            cur = con.cursor()
            cur.execute('delete from indicators where cik in (1,2)')
            cur.execute(
                "delete from ind_proc_info where name in ('name1','name2')")
            cur.execute(
                "delete from ind_rest_info where name in ('name3','name4')")
            con.commit()
            cur.execute(
                "delete from ind_classified_pairs where parent='someTag'")
            con.commit()

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

    def test_write_info(self):
        iw = IndicatorsWriter()
        ir = MySQLIndicatorFeeder()

        with self.subTest(test='empty sets'):
            iw.write_ind_info([], [])
            iw.flush()
            proc, rest = ir.fetch_ind_info()
            self.assertListEqual(proc, [])
            self.assertListEqual(rest, [])

        with self.subTest(test='how it works'):
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
            db_proc, db_rest = ir.fetch_ind_info()

            self.assertListEqual(proc, db_proc)
            self.assertListEqual(rest, db_rest)

    def test_write_pairs(self):
        iw = IndicatorsWriter()
        ir = MySQLIndicatorFeeder()

        iw.write_classified_pairs(
            [{'parent': 'someTag', 'child': 'anotherTag', 'model_id': 1, 'label': 0},
             {'parent': 'someTag', 'child': 'anotherTag', 'model_id': 1, 'label': 1},
             {'parent': 'someTag', 'child': 'anotherTag1', 'model_id': 1, 'label': 0},
             {'parent': 'someTag', 'child': 'anotherTag1', 'model_id': 0, 'label': 0}])
        iw.flush()
        pairs = ir.fetch_classified_pairs()

        self.assertEqual(len(pairs), 3)


if __name__ == '__main__':
    unittest.main()
