import unittest
import unittest.mock

import indi.loader
import indi.feeder
import indi.modclass


class TestIndicatorsPool(unittest.TestCase):
    def _test(self):
        with unittest.mock.patch('indi.modclass.load_model') as load_model:
            model = unittest.mock.MagicMock()
            load_model.return_value = model

            pool = indi.loader.load()

    def test_load_feeders(self):
        with unittest.mock.patch('indi.modclass.load_model') as load_model:
            model = unittest.mock.MagicMock()
            load_model.return_value = model

            classifiers = indi.loader.load_classifiers()
            named_cl = {cl.model_name: cl for cl in classifiers}
            feeders = indi.loader.load_feeders(named_cl)

            for feeder in feeders.values():
                if isinstance(feeder, indi.feeder.ClassFeeder):
                    self.assertTrue(
                        isinstance(
                            feeder.cl,
                            indi.modclass.Classifier))


if __name__ == '__main__':
    unittest.main()
