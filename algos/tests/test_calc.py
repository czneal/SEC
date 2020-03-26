import unittest

from algos.calc import calc_indicator, calc_indicator_whole
from algos.xbrljson import loads


class TestCalc(unittest.TestCase):
    def setUp(self):
        with open('algos/tests/res/test_structure.json') as f:
            self.structure = loads(f.read())
        with open('algos/tests/res/test_structure_2.json') as f:
            self.structure_2 = loads(f.read())
        with open('algos/tests/res/test_structure_3.json') as f:
            self.structure_3 = loads(f.read())

    def test_calc_indicator(self):
        with self.subTest(test='root value'):
            used_tags = set()
            facts = {'us-gaap:Liabilities': 1.0}
            value = calc_indicator(
                self.structure['bs'].nodes['us-gaap:Liabilities'],
                facts, used_tags)
            self.assertEqual(value, 1.0)
            self.assertEqual(set(facts.keys()).difference(used_tags), set())

        with self.subTest(test='root value None'):
            used_tags = set()
            facts = {}
            value = calc_indicator(
                self.structure['bs'].nodes['us-gaap:Liabilities'],
                facts, used_tags)
            self.assertEqual(value, None)
            self.assertEqual(set(facts.keys()).difference(used_tags), set())

        with self.subTest(test='from children simple'):
            used_tags = set()
            facts = {
                "us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": 10.0,
                'us-gaap:ProvisionForLoanLeaseAndOtherLosses': 5.0,
                'us-gaap:OtherNoncashIncomeExpense': 6.0}
            value = calc_indicator(
                self.structure['cf'].nodes
                [
                    'us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'],
                facts, used_tags)
            self.assertEqual(value, -1.0)
            self.assertEqual(set(facts.keys()).difference(used_tags), set())

        with self.subTest(test='from children two steps'):
            used_tags = set()
            facts = {
                "us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": 10.0,
                'us-gaap:ProvisionForLoanLeaseAndOtherLosses': 5.0,
                'us-gaap:OtherNoncashIncomeExpense': 6.0}
            value = calc_indicator(
                self.structure['cf'].nodes
                ["us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"],
                facts, used_tags)
            self.assertEqual(value, -1.0)
            self.assertEqual(set(facts.keys()).difference(used_tags), set())

    def test_calc_indicator_whole(self):
        with self.subTest(adsh=''):
            facts = {
                "us-gaap:FinancialServicesRevenue": 349500000.0,
                'us-gaap:HomeBuildingRevenue': 13653200000.0,
                'us-gaap:LandSales': 88300000.0}
            value = calc_indicator_whole(
                self.structure_2['is'],
                facts)

            self.assertEqual(value, 14091000000.0)

        with self.subTest(adsh='0000040545-20-000009'):
            facts = {
                "us-gaap:Revenues": 95214000000.0,
                'us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax': 87487000000.0,
                'us-gaap:OperatingLeasesIncomeStatementLeaseRevenue': 7728000000.0,
                'us-gaap:PreferredStockDividendsIncomeStatementImpact': 460000000.0}
            value = calc_indicator_whole(
                self.structure_3['is'],
                facts)

            self.assertEqual(value, 95215000000.0 - 460000000.0)


if __name__ == '__main__':
    unittest.main()
