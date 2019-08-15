# -*- coding: utf-8 -*-

import unittest, mock
import numpy as np
import pandas as pd
import itertools
from parameterized import parameterized

import tests.resource_indi as res
import indi.lstmclass as CL
from indi.indicators import IndicatorRestated
from indi.indicators import IndicatorStatic
from indi.indicators import IndicatorDynamic

class TestRestatedSimple(unittest.TestCase):
    @parameterized.expand(res.indicators_r_test_cases_simple)
    def test(self, args):
        with unittest.mock.patch('indi.lstmmodels.Models') as Models:
            model = mock.Mock()
            model.predict.side_effect = args['predict']
            
            instance = Models.return_value
            instance.models = {'model1': model}
            
            structs = res.Data.structs()
            nums = res.Data.nums()
            adshs = list(structs.index)
            
            #cik = 1467858        
            
            structure = structs.iloc[0]['structure']
            cl = CL.SingleParentAndChild(fdict='dictionary.csv',
                                      fmodel='model1',
                                      max_len=40,
                                      start_chapter = args['start_chapter'],
                                      start_tag=args['start_tag'],
                                      leaf=args['leaf'])
            ind = IndicatorRestated('mg_r_liabilities',
                                    classifier=cl,
                                    class_id=args['class_id'])
            
            cl.predict_all(structure)
            r, p = ind.calc(nums, 2012, adshs[0])
            
            #res.Data.predicted.append(p.copy())
            
            for c in res.indicators_p_columns:
                self.assertTrue(c in p.columns)
            
            if np.isnan(args['value']):
                self.assertTrue(np.isnan(r))
            else:
                self.assertEqual(r, args['value'])
                
@unittest.skip
class TestStaticDynamicSintax(unittest.TestCase):
    @parameterized.expand(res.indicators_sd_test_cases_sintax)
    def test(self, class_name, type_):
        structs = res.Data.structs()
        nums = res.Data.nums()
        adshs = list(structs.index)
        
        if type_ == 'static':
            ind = IndicatorStatic(name = class_name,
                                  one_time=False)
        else:
            ind = IndicatorDynamic(name = class_name,
                                   one_time=False)
        
        nums['name'] = nums['version'] + ':' + nums['tag']
        r, p = ind.calc(nums, 2013, adshs[0])
        print(r)

        res.Data.predicted.append(p.copy())
        
def get_sd_indicator_names():
    import indi.indprocs as procs
    import inspect
    
    names = []
    for name, obj in inspect.getmembers(procs):
        if inspect.isclass(obj) and name.startswith('mg_'):
            names.append((name, obj.btype()))
            
    return (names)

class TestStaticDynamicDeep(unittest.TestCase):
    @parameterized.expand(get_sd_indicator_names())
    def test(self, class_name, type_):
        if type_ == 'static':
            ind = IndicatorStatic(name=class_name,
                                  one_time=False)
        else:
            ind = IndicatorDynamic(name=class_name,
                                  one_time=False)
        
        full_nums = pd.DataFrame([], columns=['name', 'value', 'fy', 'adsh'])
        full_nums['value'] = np.random.rand(len(ind.dp), 1)[:,0]
        full_nums['name'] = list(ind.dp)
        full_nums['fy'] = 2018
        full_nums['adsh'] = 'adsh'
        
        
        for e in itertools.product([True, False], repeat=len(ind.dp)):
            with self.subTest(i=e):
                nums = full_nums.copy()
                nums.loc[list(e), 'value'] = np.nan
                r, p = ind.calc(nums, 2018, 'adsh')
    
if __name__ == '__main__':    
    unittest.main()
    

