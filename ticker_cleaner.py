# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 13:10:55 2018

@author: Asus
"""

import mysqlio.basicio as do
import numpy as np

def clear_name(name):
    words = {"the ":"", 
             "corporation": "corp", 
             "incorporated":"inc", 
             "company":"co",
             "limited":"ltd",
             "common stock":"",
             "common shares":"",
             "ordinary shares":"",
             "etf":""}
    symbols = {',':' ','.':'','/':' ','&':' ','-':' ',
               '\\':' ','\'':'','(':' ',')':' ','#':' ',':':' ','!':' '}
    name = name.lower()
    index = name.find("/")
    if index >= 5:
        name = name[0:index]
    for k,v in words.items():
        name = name.replace(k,v)
    for k,v in symbols.items():
        name = name.replace(k,v)    
    
    return name.replace("  "," ")

def compare(name1, name2):
    name1 = clear_name(name1)
    name2 = clear_name(name2)
    
    words1 = name1.split(" ")
    words2 = name2.split(" ")
    words1 = set([w for w in words1 if w!=""])
    words2 = set([w for w in words2 if w!=""])
    diff = words1.intersection(words2)
    diff_len = np.sum(np.array([len(w) for w in diff]))
    w1_len = np.sum(np.array([len(w) for w in words1]))
    w2_len = np.sum(np.array([len(w) for w in words2]))
    
    return 2.0*diff_len/(w1_len + w2_len)

def comparision():
    try:
        thres_down = 0
        thres_up = 0.8
        delete = "update nasdaqtraded set cik = null where symbol=%s"
        select = "select * from nasdaqtraded where cik = %s"
        con = do.OpenConnection("localhost")
        cur = con.cursor(dictionary=True)
        with open("comparision.txt","w") as f:
            cur.execute("select cik, company_name from companies")
            for index,r in enumerate(cur.fetchall()):
                print("\rcomparision:{0}".format(index), end="")
                cur.execute(select, (r["cik"],))
                for t in cur.fetchall():
                    rate = compare(r["company_name"],t["security_name_clean"])
                    if rate<thres_up and rate>thres_down:
                        f.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(r["cik"], r["company_name"], t["security_name"], rate, t["symbol"]))
                    if rate<=thres_down:
                        cur.execute(delete, (t["symbol"],))
        con.commit()
        print()
    except:
        con.close()
        raise
        
def test():        
    pairs = {0:["Alexander's, Inc.","ALEXANDERS IN"], 
             1:["Adams Resources & Energy, Inc.","ADAMS RESOURCES & ENERGY, INC."],
             2:["CECO Environmental Corp.","CECO ENVIRONMENTAL CORP"],
             3:["ID SYSTEMS INC", "I.D. Systems, Inc."]}
    index = 2
    print(compare(pairs[index][0],pairs[index][1]))

def nasdaqtraded_clear_sec_name():
    try:
        con = do.OpenConnection("localhost")
        cur = con.cursor(dictionary=True)
        cur_update = con.cursor(dictionary=True)
        cur.execute("select * from nasdaqtraded")
        for index, row in enumerate(cur.fetchall()):
            print("\rcleared:{0}".format(index), end="")
            clean_name = clear_name(row["security_name"])
            cur_update.execute(
                    "update nasdaqtraded set security_name_clean = '{0}' where symbol='{1}'".format(clean_name, row["symbol"]))
        con.commit()
        print()
    except:
        con.close()
        raise
def nasdaqtraded_update_ciks():
    try:
        
        con = do.OpenConnection("localhost")
        cur_select = con.cursor(dictionary=True)
        cur_update = con.cursor(dictionary=True)
        cur_update.execute("update nasdaqtraded set cik = null")
        cur_select.execute("select * from companies")
        for index, row in enumerate(cur_select.fetchall()):
            print("\rupdated:{0}".format(index), end="")
            name = clear_name(row["company_name"])
            update_cmd = "update nasdaqtraded set cik = {0} where security_name_clean like '{1}%'".format(row["cik"], name)
            cur_update.execute(update_cmd)
        print()
        con.commit()
    except:
        con.close()
        raise
        
def nasdaqtraded_update_ciks2(thres = 0.5):
    try:
        con = do.OpenConnection("localhost")
        cur = con.cursor(dictionary=True)
        cur.execute("""select company_name, c.cik, t.security_name, symbol 
                    from companies c, nasdaqtraded t 
                    where t.cik is null limit 100000""")
        data = []
        for index, row in enumerate(cur):
            print("\rcompared:{0}".format(index+1), end="")
            if compare(row["company_name"], row["security_name"]) > 0.5:
                data.append([row["symbol"], row["cik"]])
        print()
        
        print(data)
        for index, [symbol, cik] in enumerate(data):
            print("\rupdateded:{0}".format(index+1), end="")
            cur.execute("update nasdaqtraded set cik=%s where symbol=%s",(cik,symbol))
        print()
        con.commit()
        
    except:
        con.close()
        raise
        
from typing import List
import pandas as pd
import re

from mysqlio.basicio import OpenConnection

def split_company_name(company_name: str) -> List[str]:
    company_name = re.sub('\.+|\'+', '', company_name)
    symbols = {',':' ','.':'','/':' ','&':' ','-':' ',
               '\\':' ','\'':'','(':' ',')':' ','#':' ',
               ':':' ','!':' ', ' ': ' '}
               
    parts = [company_name]
    for symbol in symbols:
        new_parts = []
        for part in parts:
            new_parts.extend([p.strip().lower() for p in part.split(symbol)
                                            if p.strip()])
        parts = new_parts
        
    return set(parts)
        
if __name__ == '__main__':
    import numpy as np
    
#    with OpenConnection() as con:
#        cur = con.cursor(dictionary=True)
#        cur.execute('select c.cik, c.company_name from companies c, ' +
#                    '(select cik, max(file_date) as file_date from reports ' +
#                    'group by cik) r ' +
#                    'where c.cik = r.cik ' +
#                    "	and r.file_date >= '2018-06-01';")
#        companies = pd.DataFrame(cur.fetchall()).set_index('cik')
#        
#    df1 = (pd.read_csv('c:/users/asus/downloads/companylist.csv', sep=',')
#            )
#    df2 = (pd.read_csv('c:/users/asus/downloads/companylist_nyse.csv', sep=',')
#            )
#    df3 = (pd.read_csv('c:/users/asus/downloads/companylist_amex.csv', sep=',')
#            )
#    nasdaq = pd.concat([df1, df2, df3]).drop_duplicates().set_index('Symbol')
#    
#    
#    data = []
#    for cik, row in companies.iterrows():
#        company_name = row['company_name']
#        parts = split_company_name(company_name)
#        data.extend([[cik, company_name, p] for p in parts])
#        
#    companies = pd.DataFrame(data, columns=['cik', 'name', 'word'])
#    g1 = companies.groupby('word')['cik'].count().sort_values(ascending=False)
#    
#    data = []
#    for symbol, row in nasdaq.iterrows():
#        parts = split_company_name(row['Name'])
#        data.extend([[symbol, row['Name'], p] for p in parts])
#    nasdaq = pd.DataFrame(data, columns=['symbol', 'name', 'word'])
#    nasdaq.loc[nasdaq['word'] == 'corporation', 'word'] = 'corp'
#    nasdaq.loc[nasdaq['word'] == 'company', 'word'] = 'co'
#    g2 = nasdaq.groupby('word')['name'].count().sort_values(ascending=False)
#    
#    nasdaq['cik'] = np.nan
#    for symbol in list(nasdaq['symbol'].unique()):
#        words = list(nasdaq[nasdaq['symbol'] == symbol]['word'].unique())
#        f = (companies[companies['word'].isin(words)]
#                .groupby('cik')['cik']
#                .count()
#                .sort_values(ascending=False))
#        f = f[f>=f.max()]
#        if f.shape[0] == 1:
#            nasdaq.loc[nasdaq['symbol'] == symbol, 'cik'] = f.index
            
    from xbrlxml.xbrlrss import SecEnumerator
    rss = SecEnumerator(years=[2019], months=[3])
    df = pd.DataFrame([[r['cik'], r['company_name'], 
                        r['file_date'], r['form_type']] 
                            for (r, _) in rss.filing_records(all_types=True)], 
                      columns=['cik', 'company_name', 
                               'file_date', 'form_type'])
        