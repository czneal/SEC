# -*- coding: utf-8 -*-

import unittest
import unittest.mock
import numpy as np
import json

from typing import List

import indi.modclass as mcl


def predict_single_ones(x: np.ndarray) -> np.ndarray:
    return np.ones((x.shape[0], 1))


def predict_single_zeros(x: np.ndarray) -> np.ndarray:
    return np.zeros((x.shape[0], 1)).reshape(x.shape[0], 1)


def predict_multi(x: np.ndarray) -> np.ndarray:
    return np.random.rand(x.shape[0], 5)


def predict_calc_ones(pairs) -> List[int]:
    return [1 for i in range(len(pairs))]


def predict_calc_zeros(pairs) -> List[int]:
    return [0 for i in range(len(pairs))]


class TestPredict(unittest.TestCase):
    def setUp(self):
        with open('indi/tests/res/test_pairs.json') as f:
            self.pairs = [tuple(e) for e in json.load(f)]

        np.random.seed(1)

    def test_predict_single_pc(self):
        model = unittest.mock.MagicMock()
        model.predict.side_effect = predict_single_ones
        cl = mcl.SingleParentAndChild('dictionary.csv', 0, 40, 'model', model)

        answer = cl.predict(self.pairs)

        self.assertEqual(len(answer), len(self.pairs))
        self.assertEqual(answer[0], 1)

    def test_predict_single_c(self):
        model = unittest.mock.MagicMock()
        model.predict.side_effect = predict_single_ones
        cl = mcl.SingleOnlyChild('dictionary.csv', 0, 40, 'model', model)

        answer = cl.predict(self.pairs)

        self.assertEqual(len(answer), len(self.pairs))
        self.assertEqual(answer[0], 1)

    def test_predict_multi_pc(self):
        model = unittest.mock.MagicMock()
        model.predict.side_effect = predict_multi
        cl = mcl.MultiParentAndChild('dictionary.csv', 0, 40, 'model', model)

        answer = cl.predict(self.pairs)

        self.assertEqual(len(answer), len(self.pairs))
        self.assertEqual(answer[1], 4)
        self.assertEqual(answer[3], 0)

    def test_to_vector(self):
        model = unittest.mock.MagicMock()
        cl = mcl.SingleOnlyChild('dictionary.csv', 0, 6, 'model', model)
        x = np.ones((10, 6))

        cl._to_vector(
            'US_GAAP:AccumulatedOtherComprehensiveIncomeLossNetOfTax',
            x,
            2,
            pos=3)
        self.assertEqual(x[2, 3], 15)
        self.assertEqual(x[2, 4], 812)
        self.assertEqual(x[2, 5], 215)

    def test_vectorize(self):
        with self.subTest(test='SingleOnlyChild'):
            model = unittest.mock.MagicMock()
            cl = mcl.SingleOnlyChild('dictionary.csv', 0, 6, 'model', model)
            x = np.ones((10, 6))

            cl._vectorize(
                parent='AccumulatedOtherComprehensiveIncomeLossNetOfTax',
                child='TreasuryStockValue', x=x, row=2)
            self.assertEqual(x[2, 0], 1238.0)
            self.assertEqual(x[2, 3], 1.0)

        with self.subTest(test='SingleParentAndChild'):
            model = unittest.mock.MagicMock()
            cl = mcl.SingleParentAndChild(
                'dictionary.csv', 0, 6, 'model', model)
            x = np.ones((10, 6))

            cl._vectorize(
                parent='AccumulatedOtherComprehensiveIncomeLossNetOfTax',
                child='RetainedEarningsAccumulatedDeficit', x=x, row=2)
            self.assertEqual(x[2, 0], 15)
            self.assertEqual(x[2, 1], 812)
            self.assertEqual(x[2, 2], 215)
            self.assertEqual(x[2, 3], 1069)
            self.assertEqual(x[2, 4], 377)
            self.assertEqual(x[2, 5], 15)

    def test_predict(self):
        with unittest.mock.patch('indi.modclass.ModelClassifier.predict_calc') as method:
            method.side_effect = predict_calc_ones
            cl = mcl.SingleParentAndChild(
                'dictionary.csv', 2, 50, 'model', object())

            labels1 = cl.predict(self.pairs[:15])

            method.side_effect = predict_calc_zeros
            labels2 = cl.predict(self.pairs[10:])

            self.assertEqual(labels1[10:15], labels2[0:5])


if __name__ == '__main__':
    unittest.main()
