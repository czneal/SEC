# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 16:42:10 2019

@author: Asus
"""

import database_operations as do
import pandas as pd
from utils import ProgressBar

if __name__ == '__main__':
    query = """
    select q.*, dim, member from 
    (
    	select adsh, context, count(context) as cnt
    	from raw_nums n
    	where n.instant	
    	group by n.adsh, n.context
    ) q, raw_contexts c
    where q.adsh = c.adsh
    	and q.context = c.context
    order by q.adsh, q.cnt desc
    """
    try:
        print('fetch data')
        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall())
        
        adshs = list(df['adsh'].unique())
        pb = ProgressBar()
        pb.start(len(adshs))
        data = []
        for adsh in adshs:            
            f = df[df['adsh'] == adsh]
            row = {'adsh':adsh}
            for i in range(min(3,f.shape[0])):
                row['c%s' % i] = f.iloc[i]['context']
                row['c%s_cnt' % i] = f.iloc[i]['cnt']
            if 'c0' not in row: row['c0'] = None
            if 'c1' not in row: row['c1'] = None
            if 'c2' not in row: row['c2'] = None
            if 'c0_cnt' not in row: row['c0_cnt'] = None
            if 'c1_cnt' not in row: row['c1_cnt'] = None
            if 'c2_cnt' not in row: row['c2_cnt'] = None
            data.append(row)
                
            pb.measure()
            print('\r' + pb.message(), end='')
        print()
        res = pd.DataFrame(data)
    except:
        if 'con' in locals() and con is not None: con.close()
        raise

