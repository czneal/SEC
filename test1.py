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

con = None
try:
    print('init models and indicators...', end='')
    #cl_pool = cl.ClassifierPool()
    ind_pool = ind.IndicatorPool(cl_pool)
    i2d.write_mg_descr(ind_pool)
    ind.indicator_scripts()    
    print('ok')
    
    print('read newest reports...', end = '')
    con = do.OpenConnection()
    cur = con.cursor(dictionary=True)
    cur.execute('delete from mgreporttable; delete from mgparamstype1;', multi=True)
    cur.execute(q.select_newest_reports, {'fy':2017})
    reports = pd.DataFrame(cur.fetchall(), columns=['adsh', 'cik', 'fy', 'file_date', 'form'])
    con = con.close()
    con = None
    print('ok')
    
    first = 10
    cik_filter = -1
    for cik in reports['cik'].unique()[10:10+first]:
        if cik != cik_filter and cik_filter != -1:
            continue
        
        print('calculate cik: {0}'.format(cik))
        print('read nums...', end='')
        adshs = reports[reports['cik'] == cik]['adsh'].unique()
        nums = do.read_reports_nums(adshs)
        fy_structure = {}
        data = []
        for adsh in adshs:
            fy = int(nums[nums['adsh'] == adsh].iloc[0]['fy'])
            fy_structure[fy] = [json.loads(do.read_report_structures([adsh]).loc[adsh]['structure']),
                                adsh]
            data.append({'adsh':adsh, 'fy':fy, 'tag':'us-gaap:mg_tax_rate', 'value':0.2})

        nums = nums.append(data, ignore_index=True)
        print('ok')

        print('calc...', end='')
        df, dep = ind_pool.calc(nums, fy_structure)
        print('ok')

        print('write...', end='')        
        i2d.write_gaap_descr(list(dep[dep['sname'].str.find(':')!=-1]['sname'].unique()))
        
        i2d.write_params(dep)
        i2d.write_report(df, cik, fy_structure[max(fy_structure)][1])
        print('ok')    
except:
    raise
finally:
    if con: con.close()

