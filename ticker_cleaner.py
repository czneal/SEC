# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 13:10:55 2018

@author: Asus
"""
import numpy as np
import pandas as pd
import re

from typing import List, Set

import mysqlio.basicio as do

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
        
       
def measure(words1: Set[str], words2: Set[str]) -> float:
    words1.discard('etf')
    words2.discard('etf')
    words1.discard('the')
    words2.discard('the')
    
    inter = words1.intersection(words2)
    union = words1.union(words2)
    L = sum([len(w) for w in union])    
    l = sum([len(w) for w in inter])
    
    return float(l)/L

        
if __name__ == '__main__':    
    
        
    nasdaq = [] 
    nasdaq.append(pd.read_csv('c:/users/asus/downloads/companylist.csv', 
                              sep=','))
    nasdaq.append(pd.read_csv('c:/users/asus/downloads/companylist_nyse.csv', 
                              sep=','))
    nasdaq.append(pd.read_csv('c:/users/asus/downloads/companylist_amex.csv', 
                              sep=','))
    nasdaq = pd.concat(nasdaq).drop_duplicates().set_index('Symbol')
        
    data = []
    for cik, row in companies.iterrows():
        company_name = row['company_name']
        parts = split_company_name(company_name)
        data.extend([[cik, company_name, p] for p in parts])
        
    company_words = pd.DataFrame(data, columns=['cik', 'name', 'word'])
    company_words['wlen'] = company_words['word'].str.len()
    g1 = (company_words.groupby('word')['cik']
                       .count()
                       .sort_values(ascending=False))
    
#    data = []
#    for symbol, row in nasdaq.iterrows():
#        parts = split_company_name(row['Name'])
#        data.extend([[symbol, row['Name'], p] for p in parts])
#    nasdaq = pd.DataFrame(data, columns=['symbol', 'name', 'word'])
#    nasdaq.loc[nasdaq['word'] == 'corporation', 'word'] = 'corp'
#    nasdaq.loc[nasdaq['word'] == 'company', 'word'] = 'co'
#    g2 = nasdaq.groupby('word')['name'].count().sort_values(ascending=False)
    
    nasdaq['cik'] = int(0)
    
    for symbol in nasdaq.index:
        name = nasdaq.loc[symbol]['Name']
        words = split_company_name(name)            
        f = (company_words[company_words['word'].isin(words)]
                .groupby('cik')
                .agg({'word': 'count', 'wlen': 'sum'}))
        f['w'] = f['word'] + f['wlen']/len(name)
        f = f.sort_values('w', ascending=False)
        f = f[f['w']>=f['w'].max()]
        
        for cik in f.index:
            c_words = set(company_words[company_words['cik'] == cik]['word'].unique())
            if c_words == words:
                nasdaq.loc[symbol, 'cik'] = cik
                break
            
            if measure(words, c_words) > 0.9:
                nasdaq.loc[symbol, 'cik'] = cik
                break
            
    n = n.sort_values('cap', ascending=False)
    n = n[n['cap']>=100000000]
    for ticker, row in n.iterrows():
        cik = get_cik_by_ticker(ticker)
        print(cik, ticker, row['Name'])
        if cik != 0:
            nasdaq.loc[ticker, 'cik'] = cik            
        
            
            
