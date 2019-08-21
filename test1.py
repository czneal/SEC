# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 15:23:57 2019

@author: Asus
"""
import pandas as pd
import json
from typing import Tuple, List, Dict, Any, Union

import mysqlio.basicio as do
import queries as q
import indicators2dba as i2d
import settings as gs

from utils import ProgressBar
from algos.xbrljson import custom_decoder
from log_file import Logs
from exceptions import XbrlException
from indi.modelspool import ModelsPool
from indi.indpool import IndicatorsPool
from indi.loader import CSVLoader


def init_classifiers_indicators_pools(use_mock: bool) -> IndicatorsPool:
    csv_loader = CSVLoader()
    csv_loader.load()
    
    if use_mock:
        from tests.resource_indi import Data
        import unittest.mock, mock
        with unittest.mock.patch('indi.lstmmodels.Models') as mock_models:
            instance = mock_models.return_value
            
            mock_multi = mock.Mock()
            mock_multi.predict.side_effect = Data.predict_multi
            
            mock_single = mock.Mock()
            mock_single.predict.side_effect = Data.predict_single
            
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
            
    ind_pool = IndicatorsPool(class_pool=m_pool,
                             csv_loader=csv_loader)
    
    return ind_pool

def read_mgnums_structures(adshs: List[str]) -> Tuple[pd.DataFrame,
                                                      pd.DataFrame]:
    """
    nums columns: tag, version, value, fy, adsh, uom, name
    structures columns: 'adsh', 'fy', 'structure'
    """
    
    nums = do.read_reports_nums(adshs)
    nums = nums[~(nums['tag'].str.startswith('mg_'))]
    nums['name'] = nums['version'] + ':' + nums['tag']
    structures = do.read_report_structures(adshs)
    return nums, structures

def make_fy_structures(structures: pd.DataFrame) -> Dict[int, List[Union[int, Any]]]:
    """
    return dictionary with fy as dict key and list [adsh, structure]
    structures should have 'adsh' as index and ['fy', 'structure'] columns
    """
    
    fy_structures : Dict[int, List[Union[int, Any]]] = {}
    for index, row in structures.iterrows():
        fy_structures[row['fy']] = [index, 
                                    json.loads(row['structure'],
                                               object_hook=custom_decoder)]
                                    
    return fy_structures

def process_indicators(year: int, 
                       truncate: bool,
                       slice_: slice,
                       logs: Logs) -> None:
            
    logs.log('init classifiers and indicators pools...')
    #init classifiers and indicators pools
    ind_pool = init_classifiers_indicators_pools(use_mock=True)
    logs.log('ok')
    
    logs.log('prepare tables and data...')
    #truncate tables if needed
    if truncate:
        with do.OpenConnection() as con:
            cur = con.cursor()
            cur.execute('truncate table mgreporttable;')
            cur.execute('truncate table mgparamstype1;')
            con.commit()
        
    #read latest reports for this year
    with do.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute(q.select_newest_reports, {'fy': year})
        reports = pd.DataFrame(cur.fetchall(), 
                               columns=['adsh', 'cik', 'fy', 
                                        'file_date', 'form'])
    logs.log('ok')        
        
    
    ciks = list(reports[reports['fy'] == year]['cik'].unique())
    
    pb = ProgressBar()
    pb.start(len(ciks[slice_]))
    
    for cik in ciks[slice_]:
        try:
            logs.set_header([cik])
            
            r = reports[reports['cik'] == cik]
            
            nums, structs = read_mgnums_structures(list(r['adsh'].unique()))
            fy_structures = make_fy_structures(structs)
            mgnums, info = ind_pool.calc(nums, fy_structures)
            
            i2d.write_mg_descr(ind_pool)
            i2d.write_mg_nums(mgnums)
            i2d.write_params(info)
            i2d.write_report(mgnums, cik, adsh = fy_structures[max(fy_structures)][0])
        
        except Exception as e:
            logs.error(str(e))
            logs.traceback()
            
        print('\r' + pb.message(), end='')
        pb.measure()
    print()
    
    logs.log('finish')
        
if __name__ == '__main__':
    try:
        logs = Logs(log_dir = 'outputs/',#gs.Settings.root_dir(),
                    append_log=False,
                    name='calc_log')
        
        process_indicators(year=2018,
                           truncate=True,
                           slice_=slice(0,1),
                           logs=logs)
        
    except Exception as e:
        logs.error(str(e))
        logs.traceback()
        raise e
    finally:
        logs.close()
    
