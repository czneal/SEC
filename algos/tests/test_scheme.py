import unittest

from algos.xbrljson import loads
from algos.scheme import _enum_filtered


def filter_func_1(p: str, c: str) -> bool:
    if c == 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest':
        return True
    if c == 'Deposits':
        return True
    return False


def filter_func_2(p: str, c: str) -> bool:
    if c == 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest':
        return True
    return False


def filter_func_3(p: str, c: str) -> bool:
    if c in [
        'Deposits',
        'ShortTermBorrowings',
        'AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent',
        'LongTermDebt',
        'DerivativeLiabilities',
            'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest']:
        return True

    return False


def filter_func_4(p: str, c: str) -> bool:
    if c in [
        'Deposits',
        'ShortTermBorrowings',
        'AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent',
        'LongTermDebt',
        'DerivativeLiabilities',
        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
        'Assets',
            'Liabilities']:
        return True

    return False


class TestEnumFiltered(unittest.TestCase):
    def setUp(self):
        with open('algos/tests/res/test_structure.json') as f:
            self.structure = loads(f.read())

    def test_enum_filtered_1(self):
        start = self.structure['bs'].nodes['us-gaap:LiabilitiesAndStockholdersEquity']
        tuples = [t for t in _enum_filtered(
            start,
            offset=0,
            nfunc=lambda x: x.tag,
            ffunc=filter_func_1)]

        self.assertEqual(len(tuples), 5)
        self.assertEqual(tuples[4][4], False)
        self.assertEqual(tuples[1][4], True)

    def test_enum_filtered_2(self):
        start = self.structure['bs'].nodes['us-gaap:LiabilitiesAndStockholdersEquity']
        tuples = [t for t in _enum_filtered(
            start,
            offset=0,
            nfunc=lambda x: x.tag,
            ffunc=filter_func_2)]

        self.assertEqual(len(tuples), 6)

    def test_enum_filtered_3(self):
        start = self.structure['bs'].nodes['us-gaap:LiabilitiesAndStockholdersEquity']
        tuples = [t for t in _enum_filtered(
            start,
            offset=0,
            nfunc=lambda x: x.tag,
            ffunc=filter_func_3)]

        self.assertEqual(len(tuples), 1)
        self.assertEqual(tuples[0][4], True)

    def test_enum_filtered_4(self):
        start = self.structure['bs']
        tuples = [t for t in _enum_filtered(
            start,
            offset=0,
            nfunc=lambda x: x.tag,
            ffunc=filter_func_4)]

        self.assertEqual(len(tuples), 4)
        self.assertEqual(tuples[3][4], True)


if __name__ == '__main__':
    unittest.main()
