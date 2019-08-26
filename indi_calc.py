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

from exceptions import XbrlException
from utils import ProgressBar
from algos.xbrljson import custom_decoder
from log_file import Logs
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
    nums columns: 'tag', 'version', 'value', 'fy', 'adsh', 'uom', 'name'
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

def read_newest_reports(ciks: List[int], year: int) -> pd.DataFrame:
    """
    return pd.DataFrame with columns: 
        adsh, cik, fy, file_date, form 
        from reports table        
    read newest reports for specified year and every cik in ciks list
    if ciks empty return all reports    
    """
    
    with do.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        if ciks:
            cur.execute(q.create_tmp_cik_table)
            cur.executemany(q.insert_tmp_cik, [(cik,) for cik in ciks])
            cur.execute(q.select_newest_reports_ciks, {'fy': year})
        else:
            cur.execute(q.select_newest_reports, {'fy': year})
        
        df = pd.DataFrame(cur.fetchall())
        
    return df

def clear_calc_tables(adshs: List[str]) -> None:
    """
    remove calculated params from tables 
    mgnums, mgreporttable, mgparamstype1 for specified adshs
    if adshs is empty remove all calculated params
    """
    with do.OpenConnection() as con:
        cur = con.cursor()
        if adshs:
            cur.execute(q.create_tmp_adsh_table)
            cur.executemany(q.insert_tmp_adsh, [(adsh,) for adsh in adshs])
            [result for result in cur.execute(q.clear_calc_tables_adsh, 
                                              multi=True)]
        else:
            [result for result in cur.execute(q.clear_calc_tables, 
                                              multi=True)]
        con.commit()
        
def process_indicators(year: int,
                       ciks: List[int],
                       logs: Logs) -> None:
    
    logs.set_header([])
    
    logs.log('init classifiers and indicators pools...')    
    ind_pool = init_classifiers_indicators_pools(use_mock=False)
    logs.log('init classifiers and indicators pools...ok')
    
    logs.log('read newest reports for year: {}'.format(year))
    reports = read_newest_reports(ciks=ciks, year=year)
    logs.log('read newest reports...ok')
    
    logs.log('clear calculated data...')
    if ciks:
        clear_calc_tables(list(reports['adsh'].unique()))
    else:
        clear_calc_tables([])
    logs.log('clear calculated data...ok')
    
    pb = ProgressBar()
    pb.start(len(ciks))
    ciks = list(reports['cik'].unique())
    
    for cik in ciks:
        try:
            logs.set_header([cik])
            
            r = reports[reports['cik'] == cik]
            if r.shape[0] == 0:
                logs.warning('no reports for this filer')
                continue
            
            nums, structs = read_mgnums_structures(list(r['adsh'].unique()))
            if nums.shape[0] == 0:
                logs.warning('no data for this filer')
                continue
            
            fy_structures = make_fy_structures(structs)
            mgnums, info = ind_pool.calc(nums, fy_structures)
            
            i2d.write_mg_descr(ind_pool)
            i2d.write_mg_nums(mgnums)
            i2d.write_params(info)
            i2d.write_report(mgnums, cik, adsh = fy_structures[max(fy_structures)][0])
            
            logs.log('calculated')
        
        except Exception as e:
            logs.error(str(e))
            logs.traceback()
        
        pb.measure()
        print('\r' + pb.message(), end='')
        
    print()
    
    logs.set_header([])
    logs.log('finish')
        
if __name__ == '__main__':
    try:
        logs = Logs(log_dir = 'outputs/',#gs.Settings.root_dir(),
                    append_log=False,
                    name='test_log')
        
        process_indicators(year=2018,
                           ciks=[80661, 16875, 24491, 37808, 70858],
                           logs=logs)
    except XbrlException as e:
        print(str(e))
        
    except Exception as e:
        logs.error(str(e))
        logs.traceback()
        raise e
        
    finally:
        if 'logs' in locals(): logs.close()
    
    
