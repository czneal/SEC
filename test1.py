# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 15:23:57 2019

@author: Asus
"""
import database_operations as do
import queries as q
import lstm_class as cl
import indicators as ind
import indicators2dba as i2d
import pandas as pd
import json
import log_file as log
import settings as gs
import sys

con = None
try:
    print('init models and indicators...', end='')
    if 'cl_pool' not in locals():
        cl_pool = cl.ClassifierPool()
    ind_pool = ind.IndicatorPool(cl_pool)
    i2d.write_mg_descr(ind_pool)
    ind.indicator_scripts()    
    print('ok')
    
    print('read newest reports...', end = '')
    con = do.OpenConnection()
    cur = con.cursor(dictionary=True)
    
    cur.execute('truncate table mgreporttable;')
    cur.execute('truncate table mgparamstype1;')
    con.commit()
    
    cur.execute(q.select_newest_reports, {'fy':2017})
    reports = pd.DataFrame(cur.fetchall(), columns=['adsh', 'cik', 'fy', 'file_date', 'form'])
    con = con.close()
    con = None
    print('ok')
    
    out = log.LogFile(filename = gs.Settings.output_dir() + '/calc_log.log', append=False)
    err = log.LogFile(filename = gs.Settings.output_dir() + '/calc_log_err.log', append=False)
    cik_filter = -1
    ciks = reports[reports['fy'] == 2017]['cik'].unique()
    total = len(ciks)
    for index, cik in enumerate(ciks):
        if cik != cik_filter and cik_filter != -1:
            continue
        
        print('cik: {0}'.format(cik), "{0} of {1}".format(index+1, total))
        try:
            r = reports[reports['cik'] == cik]            
                        
            out.write2(cik, 'read nums')
            adshs = r['adsh'].unique()
            nums = do.read_reports_nums(adshs)
            if nums.shape[0] == 0:
                err.write2(cik, 'fail to read nums')                
                continue
            
            fy_structure = {}
            data = []
            for adsh in adshs:
                fy = int(nums[nums['adsh'] == adsh].iloc[0]['fy'])
                fy_structure[fy] = [json.loads(do.read_report_structures([adsh]).loc[adsh]['structure']),
                                    adsh]
                data.append({'adsh':adsh, 'fy':fy, 'tag':'us-gaap:mg_tax_rate', 'value':0.2})
    
            nums = nums.append(data, ignore_index=True)            
    
            out.write2(cik, 'calculate')
            df, dep = ind_pool.calc(nums, fy_structure)
            
    
            out.write2(cik, 'write to database')
            i2d.write_gaap_descr(list(dep[dep['sname'].str.find(':')!=-1]['sname'].unique()))
            
            i2d.write_params(dep)
            i2d.write_report(df, cik, fy_structure[max(fy_structure)][1])
            
        except:
            err.write_tb2(cik, sys.exc_info())
            
    out.close()
    err.close()            
except:
    raise
finally:
    if con: con.close()

