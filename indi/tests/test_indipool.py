import unittest
import unittest.mock
import numpy as np

import indi.loader
from mysqlio.indio import MySQLIndicatorFeeder


def predict_single_ones(x: np.ndarray) -> np.ndarray:
    return np.ones((x.shape[0], 1))


def predict_single_zeros(x: np.ndarray) -> np.ndarray:
    return np.zeros((x.shape[0], 1)).reshape(x.shape[0], 1)


def predict_multi(x: np.ndarray) -> np.ndarray:
    return np.random.rand(x.shape[0], 5)


def load_fake_model(model_name: str):
    if 'multi' in model_name or 'cashtype' in model_name:
        model = unittest.mock.MagicMock()
        model.predict.side_effect = predict_multi
        return model

    model = unittest.mock.MagicMock()
    model.predict.side_effect = predict_single_ones
    return model


class TestIndiPool(unittest.TestCase):
    def calc(self):
        pool = indi.loader.load()

        f = MySQLIndicatorFeeder()
        chapters, fy_adsh = f.fetch_indicator_data(
            cik=1487843, fy=2018, deep=4)
        nums = f.fetch_nums(fy_adsh)

        before = len(nums[2018])
        nums = pool.calc(nums, chapters)
        after = len(nums[2018])

        self.assertNotEqual(before, after)

    def test_how_it_works_fake_model(self):
        with unittest.mock.patch('indi.modclass.load_model') as load_model:
            load_model.side_effect = load_fake_model
            self.calc()

    def test_how_it_works_real(self):
        self.calc()


if __name__ == '__main__':
    unittest.main()
