import unittest
import unittest.mock
import numpy as np

import indi.indcache
from algos.xbrljson import loads
from indi.feeder import Feeder, ClassFeeder
from indi.modclass import SingleParentAndChild, SingleOnlyChild
from indi.modclass import get_model


def predict_single_ones(x: np.ndarray) -> np.ndarray:
    return np.ones((x.shape[0], 1))


def predict_single_zeros(x: np.ndarray) -> np.ndarray:
    return np.zeros((x.shape[0], 1)).reshape(x.shape[0], 1)


def predict_multi(x: np.ndarray) -> np.ndarray:
    return np.random.rand(x.shape[0], 5)


class TestFeeder(unittest.TestCase):
    def setUp(self):
        with open('indi/tests/res/test_structure.json') as f:
            self.structure = loads(f.read())

        with open('indi/tests/res/test_structure_2.json') as f:
            self.structure_2 = loads(f.read())

        with open('indi/tests/res/test_structure_3.json') as f:
            self.structure_3 = loads(f.read())

        indi.indcache.reset()

    def test_feed_simple(self):

        with self.subTest(test=0):
            feeder = Feeder('bs', ['us-gaap:Assets'], True)
            tags = feeder.filter(self.structure)

            self.assertEqual(len(tags), 17)
            self.assertEqual(tags[0][1], 'us-gaap:CashAndDueFromBanks')

        with self.subTest(test=1):
            feeder = Feeder('bs', ['us-gaap:IncomeLoss', 'us-gaap:Assets'],
                            strict=True)
            tags = feeder.filter(self.structure)

            self.assertEqual(len(tags), 17)
            self.assertEqual(tags[0][1], 'us-gaap:CashAndDueFromBanks')

        with self.subTest(test=2):
            feeder = Feeder('bs', ['us-gaap:LiabilitiesAndStockholdersEquity'],
                            strict=True)
            tags = feeder.filter(self.structure)

            self.assertEqual(len(tags), 13)
            self.assertEqual(
                tags[2][1],
                'us-gaap:AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent')

        with self.subTest(test=3):
            feeder = Feeder('bs', ['IncomeLoss'], strict=True)
            tags = feeder.filter(self.structure)
            self.assertEqual(len(tags), 0)

        with self.subTest(test=4):
            feeder = Feeder('is', ['Liabilities'], strict=True)
            tags = feeder.filter(self.structure)
            self.assertEqual(len(tags), 0)

        with self.subTest(test=5):
            feeder = Feeder('is', ['Liabilities'], strict=False)
            node = feeder.find_start(self.structure)
            self.assertEqual(
                node.name,
                'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic')

        with self.subTest(test=6):
            feeder = Feeder('bs', ['Liabilities'], strict=False)
            tags = feeder.filter(self.structure)
            self.assertNotEqual(len(tags), 0)

        with self.subTest(test=7):
            feeder = Feeder('is', ['Liabilities'], strict=False)
            node = feeder.find_start(self.structure_3)
            self.assertEqual(
                node.name,
                'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments')

    def test_classified_feeder(self):
        with self.subTest(test='zeros'):
            indi.indcache.reset()
            model = unittest.mock.MagicMock()
            model.predict.side_effect = predict_single_zeros

            cl = SingleOnlyChild('dictionary.csv', 0, 60, 'model', model)
            feeder = ClassFeeder(
                'bs', ['us-gaap:LiabilitiesAndStockholdersEquity'],
                True, cl, 0)
            tags = feeder.filter(self.structure)

            self.assertEqual(len(tags), 0)

        with self.subTest(test='ones'):
            indi.indcache.reset()
            model = unittest.mock.MagicMock()
            model.predict.side_effect = predict_single_ones

            cl = SingleOnlyChild(
                'dictionary.csv', 0, 40,
                'liabilities_class_pch_v2019-03-13.h5', model)
            feeder = ClassFeeder(
                'bs', ['us-gaap:LiabilitiesAndStockholdersEquity'],
                True, cl, 0)
            tags = feeder.filter(self.structure)

            self.assertEqual(len(tags), 13)
            self.assertEqual(tags[0][0].startswith('us-gaap'), True)

        with self.subTest(test='real model'):
            indi.indcache.reset()
            model = get_model('liabilities_class_pch_v2019-03-13.h5')
            cl = SingleParentAndChild(
                'dictionary.csv', 0, 40,
                'liabilities_class_pch_v2019-03-13.h5', model)
            feeder = ClassFeeder(
                'bs', ['us-gaap:LiabilitiesAndStockholdersEquity'],
                True, cl, 0)
            tags = feeder.filter(self.structure_2)

            self.assertEqual(len(tags), 5)
            self.assertEqual(tags[0][0].startswith('us-gaap'), True)


if __name__ == '__main__':
    unittest.main()
