# -*- coding: utf-8 -*-

import unittest, mock

from indi.modelspool import ClassifierOrder
from indi.exceptions import SortException

class TestClissifierOrder(unittest.TestCase):
    def make_correct_list(self):
        models = {}
        for i in range(10):
            m = mock.Mock()
            m.fmodel = 'model' + str(i)
            m.filter_model = None
            models[m.fmodel] = m
            
        models['model0'].filter_model = models['model1']
        models['model1'].filter_model = models['model3']
        models['model3'].filter_model = models['model2']
        models['model4'].filter_model = models['model8']
        models['model8'].filter_model = models['model6']
        models['model6'].filter_model = models['model9']
        models['model5'].filter_model = models['model8']
        return models
        
    def test_order(self):
        models = self.make_correct_list()
        
        o = ClassifierOrder()
        order = o.order(models)
        ethalon = ['model2', 'model3', 'model1', 'model0', 'model9', 'model6', 'model8', 'model4', 'model5', 'model7']
        self.assertListEqual(order, ethalon)
        
    def test_raise_exception(self):
        models = self.make_correct_list()
        models['model9'].filter_model = models['model5']
        
        o = ClassifierOrder()
        with self.assertRaises(SortException):
            o.order(models)
        
if __name__ == '__main__':
    unittest.main()
