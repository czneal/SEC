import unittest

from mysqlio.indio import MySQLIndicatorFeeder


class TestReader(unittest.TestCase):
    def test_fetch_facts(self):
        r = MySQLIndicatorFeeder()

        facts = r.fetch_facts('0001213900-19-003416')

        self.assertTrue('us-gaap:Liabilities' in facts)
        self.assertTrue('us-gaap:Assets' in facts)

    def test_fetch_indicator_data(self):
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
        r = MySQLIndicatorFeeder()
        chapters, fy_adsh = r.fetch_indicator_data(
            cik=1487843, fy=2018, deep=4)
        self.assertTrue(len(fy_adsh) != 0)

        nums = r.fetch_nums(fy_adsh)
        for fy, adsh in fy_adsh.items():
            self.assertTrue(len(nums[fy]) != 0)


if __name__ == '__main__':
    unittest.main()
