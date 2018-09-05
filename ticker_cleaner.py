# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 13:10:55 2018

@author: Asus
"""

import database_operations as do
import sys
import numpy as np
import traceback

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
        
#nasdaqtraded_clear_sec_name()
nasdaqtraded_update_ciks2()
#comparision()