# -*- coding: utf-8 -*-
from typing import Dict
import pandas as pd
import datetime

from indi.modelspool import ModelsPool
from indi.indicators import Indicator, create
from indi.loader import CSVLoader
from indi.exceptions import SortException

class IndicatorPool(object):
    def __init__(self, class_pool: ModelsPool,
                       csv_loader: CSVLoader):
        self.indicators = {} #type: Dict[str, Indicator]
        self.class_pool = class_pool #type: ModelPool
        self._init_indicators(class_pool,
                              csv_loader)
        

    def _init_indicators(self, class_pool: ModelsPool,
                               csv_loader: CSVLoader):

        for ind_name, kwargs in csv_loader.indicators():
            self.indicators[ind_name] = create(ind_name,                                               
                                               class_pool=class_pool,
                                               kwargs=kwargs)

        self._sort_indicators()


    def _sort_indicators(self):
        self.indicator_order = []
        total = -1
        while total < len(self.indicator_order):
            total = len(self.indicator_order)
            
            for ind_name, ind in self.indicators.items():
                if ind_name in self.indicator_order:
                    continue
                if ind.dependencies().issubset(self.indicator_order):
                    self.indicator_order.append(ind_name)
            
        if len(self.indicator_order) != len(self.indicators):
            sdiff = set(self.indicator_order).symmetric_difference(set(self.indicators.keys()))
            message = "impossible to order indicators: {0}".format(sdiff)
            for ind in sdiff:
                dp = self.indicators[ind].dependencies()
                dp = dp.difference(set(self.indicator_order))
                message += '\nindicator: {0}, dep: {1}'.format(ind, dp)
            raise SortException(message)
            
        return

    def calc(self, nums, fy_structure):
        """
        calculate indicators for one cik and all possible years
        fy_structure - Dict[fy, [adsh, structure]]
        nums - DataFrame columns = [adsh, fy, value, tag, name]
        """

        info = []
        
        max_year = max(fy_structure)
        
        data = []
        print('part A')
        for fy, (adsh, structure) in fy_structure.items():
            self.class_pool.predict_all(structure)            
            for ind in [self.indicators[o] for o in self.indicator_order]:                
                if ind.one_time and fy != max_year:
                    continue
                if ind.dependencies():
                    continue
                
                value, n = ind.calc(nums, fy, adsh)
                info.append(n)
                data.append({'adsh': adsh,
                                    'fy': fy,
                                    'value': value,
                                    'tag': ind.name,
                                    'version': '',
                                    'name': ind.name})
        nums = nums.append(data, ignore_index=True)
        
        data = []
        print('part B')
        for fy, (adsh, structure) in fy_structure.items():
            for ind in [self.indicators[o] for o in self.indicator_order]:                
                if ind.one_time and fy != max_year:
                    continue
                if not ind.dependencies():
                    continue
                                                
                value, n = ind.calc(nums, fy, adsh)
                info.append(n)
                nums = nums.append([{'adsh': adsh,
                                    'fy': fy,
                                    'value': value,
                                    'tag': ind.name,
                                    'version': '',
                                    'name': ind.name}],
                                    ignore_index=True)
        
        info = pd.concat(info, sort=False)
        info.fillna(value={'l': True}, inplace=True)
        
        columns = info.columns.tolist() + ['value'] + ['sadsh']
        info = (pd.merge(info, nums[['fy', 'name', 'value', 'adsh']],
                         left_on=['sname','fy'], right_on=['name', 'fy'],
                         how='left',
                         suffixes=('', '_y'))
                  .rename({'adsh_y':'sadsh'}, axis='columns'))
                
        info['sadsh'] = info['sadsh'].fillna(info['adsh'])
        
        return nums, info[columns]

def one_pass(cycles: int, use_mock) -> datetime.timedelta:
    assert cycles>0
    
    from tests.resource_indi import Data    
        
    csv_loader = CSVLoader()
    csv_loader.load()
    
    if use_mock:
        import unittest.mock, mock
        with unittest.mock.patch('indi.lstmmodels.Models') as mock_models:
            instance = mock_models.return_value
            
            mock_multi = mock.Mock()
            mock_multi.predict.side_effect = Data.predict_multi
            
            mock_single = mock.Mock()
            mock_single.predict.side_effect = Data.predict_single_ones
            
            models_dict = {}
            for fmodel, kwargs in csv_loader.models():
                if kwargs['multi'] == 1:
                    models_dict[fmodel] = mock_multi
                else:
                    models_dict[fmodel] = mock_single
            instance.models = models_dict
            
            m_pool = ModelsPool()
            m_pool.load(csv_loader)
    else:        
        m_pool = ModelsPool()
        m_pool.load(csv_loader)
            
    ind_pool = IndicatorPool(class_pool=m_pool,
                             csv_loader=csv_loader)
    raw_nums = Data.nums()
    raw_nums['name'] =raw_nums['version'] + ':' + raw_nums['tag']
    structs = Data.structs()
    
    fy_structures = {}
    for adsh in raw_nums['adsh'].unique():
        fy = raw_nums[raw_nums['adsh'] == adsh]['fy'].max()
        fy_structures[fy] = [adsh, structs.loc[adsh]['structure']] 
    
            
    start = datetime.datetime.now()
    for i in range(cycles):
        n, info = ind_pool.calc(raw_nums, fy_structures)
    end = datetime.datetime.now()
    td = (end - start)/cycles
    
    return(td, n, info)
    
if __name__ == '__main__':
    td, n, info = one_pass(cycles=1, use_mock=False)
    print(td)

