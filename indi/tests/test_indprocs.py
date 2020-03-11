import unittest
import random
import indi.indprocs as prc

from itertools import product, combinations


class TestSyntax(unittest.TestCase):
    def test_syntax(self):
        for k, cls_ in prc.__dict__.items():
            if not k.startswith('mg_'):
                continue
            with self.subTest(class_name=cls_.__name__):
                proc = cls_()
                dp = proc.dp.copy()

                years = []
                facts = []
                for r in range(1, 3):
                    for yy in combinations([2019, 2018], r):
                        years.append(yy)
                for rr in range(1, len(dp) + 1):
                    for ff in combinations(dp, rr):
                        facts.append(ff)

                print(cls_.__name__, len(dp), len(facts))

                for yy in years:
                    for ff in product(facts, repeat=len(yy)):
                        nums = {}
                        for i, y in enumerate(yy):
                            for f in ff[i]:
                                nums.setdefault(y, {})[f] = random.random()
                        proc.run_it(nums, 2019)

    def test_mg_income(self):
        income = prc.mg_income()

        with self.subTest(i=0):
            nums = {
                2019: {
                    'us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted': 2.0}}
            self.assertEqual(income.run_it(nums, 2019), 2.0)

        with self.subTest(i=1):
            nums = {2019: {
                'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic': 1.0
            }}
            self.assertEqual(income.run_it(nums, 2019), 1.0)

        with self.subTest(i=2):
            nums = {2019: {}}
            self.assertEqual(income.run_it(nums, 2019), None)

        with self.subTest(i=3):
            nums = {2019: {'us-gaap:NetIncomeLoss': 3.0}}
            self.assertEqual(income.run_it(nums, 2019), 3.0)

        with self.subTest(i=4):
            nums = {2019: {'us-gaap:ProfitLoss': 4.0}}
            self.assertEqual(income.run_it(nums, 2019), 4.0)

        with self.subTest(i=5):
            nums = {2019: {'mg_capitalized_software': 5.0}}
            self.assertEqual(income.run_it(nums, 2019), 5.0)

        with self.subTest(i=6):
            nums = {2019: {'mg_capitalized_software': 5.0,
                           'us-gaap:ProfitLoss': 4.0}}
            self.assertEqual(income.run_it(nums, 2019), 9.0)

    def test_mg_acquired_realestate(self):
        proc = prc.mg_acquired_realestate()
        with self.subTest(i=0):
            nums = {}
            self.assertEqual(proc.run_it(nums, 2019), None)

        with self.subTest(i=1):
            nums = {
                2019: {
                    'us-gaap:PaymentsToAcquireRealEstate': 1.0,
                    'us-gaap:PaymentsToAcquireRealEstateHeldForInvestment': -2.0}}
            self.assertEqual(proc.run_it(nums, 2019), -1.0)

    def test_mg_capitalized_software(self):
        proc = prc.mg_capitalized_software()
        with self.subTest(i=0):
            self.assertEqual(proc.run_it({}, 2019), None)

        with self.subTest(i=1):
            nums = {2019: {'us-gaap:CapitalizedComputerSoftwareNet': 1.0}}
            self.assertEqual(proc.run_it(nums, 2019), None)

        with self.subTest(i=2):
            nums = {2018: {'us-gaap:CapitalizedComputerSoftwareNet': 1.0}}
            self.assertEqual(proc.run_it(nums, 2019), None)

        with self.subTest(i=3):
            nums = {2019: {'us-gaap:CapitalizedComputerSoftwareNet': 1.0},
                    2018: {'us-gaap:CapitalizedComputerSoftwareNet': 2.0}}
            self.assertEqual(proc.run_it(nums, 2019), -1)

    def test_mg_cash_operating_activities(self):
        proc = prc.mg_cash_operating_activities()
        with self.subTest(i=0):
            self.assertEqual(proc.run_it({}, 2019), None)

        with self.subTest(i=1):
            nums = {
                2019: {
                    'us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations': 1.0}}
            self.assertEqual(proc.run_it(nums, 2019), 1.0)

        with self.subTest(i=2):
            nums = {
                2019: {
                    'us-gaap:NetCashProvidedByUsedInOperatingActivities': 2.0}}
            self.assertEqual(proc.run_it(nums, 2019), 2.0)

        with self.subTest(i=3):
            nums = {
                2019: {
                    'us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations': 1.0,
                    'us-gaap:NetCashProvidedByUsedInOperatingActivities': 2.0}}
            self.assertEqual(proc.run_it(nums, 2019), 2.0)

    def test_mg_equity(self):
        proc = prc.mg_equity()
        with self.subTest(i=0):
            self.assertEqual(proc.run_it({}, 2019), None)

        with self.subTest(i=1):
            nums = {2019: {'us-gaap:Assets': 1.0,
                           'us-gaap:Liabilities': 2.0}}
            self.assertEqual(proc.run_it(nums, 2019), None)

        with self.subTest(i=2):
            nums = {2018: {'us-gaap:Assets': 1.0,
                           'us-gaap:Liabilities': 2.0}}
            self.assertEqual(proc.run_it(nums, 2019), -1.0)

        with self.subTest(i=2):
            nums = {2018: {'us-gaap:Assets': 1.0}}
            self.assertEqual(proc.run_it(nums, 2019), None)

    def test_mg_intangibles(self):
        proc = prc.mg_intangibles()
        with self.subTest(i=0):
            self.assertEqual(proc.run_it({}, 2019), None)

        with self.subTest(i=1):
            nums = {2019: {"us-gaap:IntangibleAssetsNetIncludingGoodwill": 1.0}}
            self.assertEqual(proc.run_it(nums, 2019), 1.0)

        with self.subTest(i=2):
            nums = {2019: {"us-gaap:IntangibleAssetsNetIncludingGoodwill": 1.0,
                           "us-gaap:Goodwill": 2.0}}
            self.assertEqual(proc.run_it(nums, 2019), 1.0)

        with self.subTest(i=3):
            nums = {2019: {"us-gaap:OtherIntangibleAssetsNet": 1.0,
                           "us-gaap:Goodwill": 2.0}}
            self.assertEqual(proc.run_it(nums, 2019), 3.0)

        with self.subTest(i=4):
            nums = {2019: {"us-gaap:Goodwill": 2.0}}
            self.assertEqual(proc.run_it(nums, 2019), 2.0)

    def test_mg_invest_fix_assets(self):
        proc = prc.mg_invest_fix_assets()

        nums = {2019: {
            'mg_payments_capital': 1.0,
            'us-gaap:PaymentsToDevelopSoftware': 2.0,
            'us-gaap:PaymentsForSoftware': 3.0,
            'us-gaap:PaymentsToAcquireBusinessesAndInterestInAffiliates': 4.0,
            'us-gaap:PaymentsToAcquireBusinessesGross': 5.0,
            'us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired': 6.0,
            'us-gaap:PaymentsToAcquireSoftware': 7.0}}

        with self.subTest(i=0):
            self.assertEqual(proc.run_it(nums, 2019), 22.0)


if __name__ == '__main__':
    unittest.main()
