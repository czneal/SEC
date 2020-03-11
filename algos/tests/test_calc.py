import unittest

from algos.calc import calc_indicator
from algos.xbrljson import loads


class TestCalc(unittest.TestCase):
    def setUp(self):
        with open('algos/tests/res/test_structure.json') as f:
            self.structure = loads(f.read())

    def test_calc_indicator(self):
        with self.subTest(test='root value'):
            facts = {'us-gaap:Liabilities': 1.0}
            value = calc_indicator(
                self.structure['bs'].nodes['us-gaap:Liabilities'], facts)
            self.assertEqual(value, 1.0)

        with self.subTest(test='root value None'):
            facts = {}
            value = calc_indicator(
                self.structure['bs'].nodes['us-gaap:Liabilities'], facts)
            self.assertEqual(value, None)

        with self.subTest(test='from children simple'):
            facts = {
                "us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": 10.0,
                'us-gaap:ProvisionForLoanLeaseAndOtherLosses': 5.0,
                'us-gaap:OtherNoncashIncomeExpense': 6.0}
            value = calc_indicator(
                self.structure['cf'].nodes
                [
                    'us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'],
                facts)
            self.assertEqual(value, -1.0)

        with self.subTest(test='from children two steps'):
            facts = {
                "us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": 10.0,
                'us-gaap:ProvisionForLoanLeaseAndOtherLosses': 5.0,
                'us-gaap:OtherNoncashIncomeExpense': 6.0}
            value = calc_indicator(
                self.structure['cf'].nodes
                ["us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"],
                facts)
            self.assertEqual(value, -1.0)


if __name__ == '__main__':
    unittest.main()
