# -*- coding: utf-8 -*-

import unittest
import json
from parameterized import parameterized

from indi.loader import CSVLoader, DBLoader
import tests.resource_indi as res

class TestCSVLoader(unittest.TestCase):
    def test_load(self):
        with unittest.mock.patch('settings.Settings.models_dir') as models_dir:
            models_dir.return_value = "../tests/resources/indi/"
            loader = CSVLoader()
            loader.load()
        
    def test_iterate_indicators(self):
        with unittest.mock.patch('settings.Settings.models_dir') as models_dir:
            models_dir.return_value = "../tests/resources/indi/"
            loader = CSVLoader()
            loader.load()
            
            indicators = {}
            for ind, args in loader.indicators():
                indicators[ind] = args
            
            answer = json.loads(res.loader_indicators)
            self.assertDictEqual(indicators, answer)        
    
    @parameterized.expand(res.loader_get_class)
    def test_get_class(self, fmodel, answer):
        with unittest.mock.patch('settings.Settings.models_dir') as models_dir:
            models_dir.return_value = "../tests/resources/indi/"
            loader = CSVLoader()
            loader.load()
        
        args = loader.get_class(fmodel)
        self.assertDictEqual(answer, args)
    
    @parameterized.expand(res.loader_get_filter)    
    def test_get_filter(self, fmodel, answer):
        with unittest.mock.patch('settings.Settings.models_dir') as models_dir:
            models_dir.return_value = "../tests/resources/indi/"
            loader = CSVLoader()
            loader.load()
        
        args = loader.get_filter(fmodel)
        self.assertDictEqual(answer, args)
        
class TestDBLoader(unittest.TestCase):
    def test_load(self):
        loader = DBLoader()
        loader.load()
        
    def test_indicators(self):
        loader = DBLoader()
        loader.load()
        for ind, args in loader.indicators():
            for p in ['script', 'type', 'dependencies', 'one_time']:
                self.assertTrue(p in args)
                        
        
if __name__ == '__main__':
    unittest.main()

