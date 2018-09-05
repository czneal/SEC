# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 13:46:45 2018

@author: Asus
"""

import urllib
import urllib.request
import json
import database_operations as do
import sys
import time as t


def scrape_tickers_from_yahoo():
    exch = {"NAS","NCM","NGM","NMS","NIM","NYQ","NYS","ASE","ASQ"}
    types = {"S"}
    
    link = "http://d.yimg.com/autoc.finance.yahoo.com/autoc"
    req = urllib.request.Request("http://d.yimg.com/autoc.finance.yahoo.com/autoc")
    params = {"lang" :"en", "query": "Wells Fargo", "region":"US"}
    
    try:
        con = do.OpenConnection(host="server")
        
        cur = con.cursor(dictionary=True)
        cur_insert = con.cursor(dictionary=True)
        insert = """insert into tickers (cik, ticker, exch, exchDisp, type, typeDisp, name)
                    values (%(cik)s, %(symbol)s, %(exch)s, %(exchDisp)s ,%(type)s, %(typeDisp)s, %(name)s)"""
        cur.execute("select * from Companies where cik not in(select distinct cik from tickers)")
        companies = cur.fetchall()
        count = 0
        total = len(companies)
        for r in companies:
            count += 1
            params["query"] = "Accenture plc"#r["company_name"]
            data = urllib.parse.urlencode(params)
            data = data.encode('ascii') # data should be bytes
            req = urllib.request.Request(link, data)
            tryout = 10
            while tryout>0:
                try:
                    with urllib.request.urlopen(req) as response:
                        res = json.load(response)
                    break
                except:
                    t.sleep(10)
                    tryout -= 1
            if tryout == 0:
                continue
                    
            res = res["ResultSet"]["Result"]
            
            if len(res) == 0:
                continue
            company_name = ""
            for res_r in res:
                if res_r["type"] in types and res_r["exch"] in exch:
                    company_name = res_r["name"]
                    break
            if company_name == "":
                continue
            data = []
            for res_r in res:
                if res_r["name"] == company_name and res_r["type"] in types and res_r["exch"] in exch:
                    res_r["cik"] = r["cik"]
                    data.append(res_r)
            if len(data) == 0:
                continue
            cur.executemany(insert, data)
            con.commit()
            
            print("\rProcessed with {0} of {1}".format(count, total) , end = "")
            t.sleep(2)
    except:
        con.close()
        print(sys.exc_info())
        
def load_tickers_from_file(filename):
    try:
        con = do.OpenConnection("server")
        table = do.Table("ticker_cik", con, buffer_size=1000)
        table.truncate(con)
        cur = con.cursor()
        print(table.fields)
                
        with open(filename) as f:
            skip = False
            for line in f.readlines():
                if skip: 
                    skip = False
                    continue
                line = line.replace("\n","")
                line = line.split("|")
                header = {"cik":0, "ticker":1, "name":2, "exchdisp":3, "sic":4, "business":5, "inc":6, "irs":7}
                table.write(header, line, cur)
            table.flush(cur)
            con.commit()
    except:
        con.close()
        print(sys.exc_info())
        
load_tickers_from_file("RawData/cik_ticker.csv")