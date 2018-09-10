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
import pandas as pd
import numpy as np
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols


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
        
def get_nasdaq_symbols2():
    def app_func(ser):
        columns = ["Nasdaq Traded","ETF","Test Issue","NextShares"]
        for c in columns:
            val = ser.loc[c]
            if val == 'Y': 
                ser.loc[c] = True
            elif val == 'N':
                ser.loc[c] = False
            else:
                ser.loc[c] = np.nan
        return ser
    
    ftp = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt"
    req = urllib.request.Request(ftp)
    url = urllib.request.urlopen(req)
    
    df = pd.read_csv(url, sep="|")
    url.close()
    df.set_index(["Symbol"], inplace=True)
    df = df[df.index.notnull()]    
    df = df.apply(app_func, axis=1)
    
    return df
    
def update_tickers():
    symbols = get_nasdaq_symbols()
    symbols = symbols[symbols["Test Issue"]==False]
    #symbols.fillna(value='', inplace=True)
    
    df2header = {'Nasdaq Traded':'traded',
                 'Security Name':'security_name',
                 'Listing Exchange':'listing_exchange',
                 'Market Category':'market_category',
                 'ETF':'etf',
                 'Round Lot Size':'lot_size',
                 'NASDAQ Symbol':'nasdaq_symbol',
                 'Financial Status':'financial_status',
                 'CQS Symbol':'cqs_symbol'}
    
    symbols.rename(inplace=True, columns=df2header)
    symbols.index.name = 'symbol'
    
    try:
        con = do.OpenConnection("localhost")
        cur = con.cursor(dictionary=True)
        table = do.Table("nasdaqtraded", con)
        table.write_df(symbols, cur)
        con.commit()
    except:
        con.close()
        raise

def tickers_managment():
    try:
        words_dict = {}
        symbols_to_replce = {'.',',',':',';','/','-','(',')'}
        con = do.OpenConnection("localhost")
        cur = con.cursor(dictionary=True)
        cur.execute("select * from nasdaqtraded where traded = 1")
        for row in cur:
            security_name = row["security_name"]
            for s in symbols_to_replce:
                security_name = security_name.replace(s,' ')
            sec_name_words = security_name.split(" ")
            for w in sec_name_words:
                if w in words_dict:
                    words_dict[w] += 1
                else:
                    words_dict[w] = 1
        sorted_dict = sorted(words_dict.items(), key=lambda v: v[1], reverse=True )
    except:
        con.close()
        raise
        
def iex_tickers():
    url_text = "https://api.iextrading.com/1.0/ref-data/symbols"
    req = urllib.request.Request(url_text)
    url = urllib.request.urlopen(req)
    body = url.read()
    url.close()
    
    return pd.read_json(body, orient="records")

def iex_ticker_lookup():
    df = iex_tickers()
    df = df[df["type"] != 'et']
    words_dict = {}
    symbols_to_replce = {'.',',',':',';','/','-','(',')'}
            
    for index, security_name in df["name"].iteritems():
        for s in symbols_to_replce:
            security_name = security_name.replace(s,' ')
        for i in range(4,1,-1):
            security_name = security_name.replace("".ljust(i," ")," ").strip()
        sec_name_words = security_name.split(" ")
        for w in sec_name_words:
            if w in words_dict:
                words_dict[w] += 1
            else:
                words_dict[w] = 1
    sorted_dict = sorted(words_dict.items(), key=lambda v: v[1], reverse=True )
    
smb = get_nasdaq_symbols2()