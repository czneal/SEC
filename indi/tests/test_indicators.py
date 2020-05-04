import unittest
import indi.feeder
import indi.modclass
import indi.indicators
import mysqlio.indio as do


class TestIndicator(unittest.TestCase):
    def test_mg_r_sales(self):
        r = do.MySQLIndicatorFeeder()
        cl = indi.modclass.create(
            model_name='income_st_multiclass_pch_v2019-03-13.h5',
            model_id=0,
            fdict='dictionary.csv',
            pc=True,
            multi=True,
            max_len=60
        )

        feeder = indi.feeder.create(
            chapter='is',
            names=["us-gaap:IncomeLoss",
                   "us-gaap:NetIncomeLoss",
                   "us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic",
                   "us-gaap:EarningsPerShareBasic",
                   "us-gaap:ProfitLoss"
                   ],
            strict=False)

        indicator = indi.indicators.IndicatorRestated(
            name='mg_r_sales',
            classifier=cl,
            class_id=2, feeder=feeder)

        with self.subTest(cik=2488, fy=2019):
            fy_structure, fy_adsh = r.fetch_indicator_data(
                cik=2488, fy=2019, deep=5)
            nums = r.fetch_nums(fy_adsh)
            value = indicator.calc(nums, 2019, fy_structure[2019])

            self.assertEqual(value, 6731000000.0)

        with self.subTest(cik=14272, fy=2019):
            fy_structure, fy_adsh = r.fetch_indicator_data(
                cik=14272, fy=2019, deep=5)
            nums = r.fetch_nums(fy_adsh)
            value = indicator.calc(nums, 2019, fy_structure[2019])

            self.assertEqual(value, 26145000000.0)

        cik, fy = 882184, 2017
        with self.subTest(cik=cik, fy=fy):
            fy_structure, fy_adsh = r.fetch_indicator_data(
                cik=cik, fy=fy, deep=5)
            nums = r.fetch_nums(fy_adsh)
            value = indicator.calc(nums, fy, fy_structure[fy])

            self.assertEqual(value, 14091000000.0)

    def test_mg_acquired_realestate(self):
        r = do.MySQLIndicatorFeeder()
        indicator1 = indi.indprocs.create('mg_acquired_realestate')
        indicator2 = indi.indprocs.create('mg_invest_fix_assets')

        cik, fy = 70858, 2019
        with self.subTest(cik=cik, fy=fy):
            fy_structure, fy_adsh = r.fetch_indicator_data(
                cik=cik, fy=fy, deep=5)
            nums = r.fetch_nums(fy_adsh)

            value = indicator1.calc(nums, fy, fy_structure[2019])
            self.assertEqual(value, None)

            value = indicator2.calc(nums, fy, fy_structure[2019])
            self.assertEqual(value, None)


if __name__ == '__main__':
    unittest.main()
