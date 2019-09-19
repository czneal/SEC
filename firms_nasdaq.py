# -*- coding: utf-8 -*-
import datetime as dt

from typing import List
from itertools import product

import firms.fetch as f
import mysqlio.firmsio as fio
from firms.tickers import attach


def update_sec_forms(years: List[int], months: List[int]) -> None:
    quarters = set([int((m + 2)/3) for m in months])
    for y, q in product(years, quarters):
        print('get crawler file for year: {0}, quarter: {1}'.format(y, q))
        forms = f.get_sec_forms(y, q)
        print('ok')
        
        print('write sec forms for year: {0}, quarter: {1} to database'
                  .format(y, q))
        fio.write_sec_forms(forms)
        print('ok')
        
def update_forms_companies_nasdaq() -> None:
    try:
        print('update SEC forms, companies and attach nasdaq symbols')
        
        today = dt.datetime.now().date()
        print(today)
        
        year = today.year
        month = today.month
        quarter = int((month-1)/4) + 1
        
        print('read SEC new forms...')
        sec_forms = f.get_sec_forms(year, quarter)
        print('ok')
        print('write SEC forms to database...')
        fio.write_sec_forms(sec_forms)
        print('ok')
        
        print('read new cik...')
        new_companies = fio.get_new_companies()
        print('ok')
        ciks = list(new_companies['cik'].unique())
        print('get info for new {0} cik from SEC site...'.format(len(ciks)))
        companies = f.companies_search_mpc(ciks)
        companies['updated'] = today
        print('ok')
        print('write new companies to database...')
        fio.write_companies(companies)
        print('ok')
        
        print('attach nasdaq symbols...')
        nasdaq = attach()
        print('ok')
        print('write nasdaq symbols to database...')
        fio.write_nasdaq(nasdaq)
        print('ok')
    except Exception as e:
        print('something not right:(')
        print(e)
    
if __name__ == '__main__':
    update_forms_companies_nasdaq()
#    nasdaq = attach()
#    update_sec_forms(years=[2019], months=[8, 9])
    
    
    
#    with OpenConnection() as con:
#        cur = con.cursor(dictionary=True)
#        cur.execute("""select ticker, n.company_name as n_name,
#                            c.cik as cik, c.company_name as c_name
#                    from nasdaq n, companies c where n.cik = c.cik""")
#        df = pd.DataFrame(cur.fetchall())
#        df['dist'] = df[['n_name', 'c_name']].apply(
#                lambda x: distance(x['n_name'], x['c_name']),
#                axis=1)
#        
#        f = df[df['dist']<0.9]