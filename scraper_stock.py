# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 08:22:01 2018

@author: Asus
"""

import urllib
import urllib.request
import json
import pandas as pd
import database_operations as do
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols

def alpha_vantage():
    params = {"function":"TIME_SERIES_DAILY_ADJUSTED",
              "symbol":"AA",
              "outputsize":"compact",
              "datatype":"json",
              "apikey":"3TQY9OJPR04V5KUD"
              }
    params_str = urllib.parse.urlencode(params)
    url_text = "https://www.alphavantage.co/query?"
    req = urllib.request.Request(url_text+params_str)
    url = urllib.request.urlopen(req)
    body = url.read()
    url.close()
    
    data = json.loads(body)
    df = pd.read_json(json.dumps(data["Time Series (Daily)"]), orient="index")
    print(df.head())

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

def update_interdaily(tickers, date):
    symbols = ""
    for e in tickers:
        symbols += e + ","
    symbols = symbols[0:-1]
    params = {"symbols":symbols}
    
    params_str = urllib.parse.urlencode(params)
    url_text = "https://api.iextrading.com/1.0/stock/market/chart/date/"+date+"?"
    req = urllib.request.Request(url_text+params_str)
    url = urllib.request.urlopen(req)
    body = url.read()
    url.close()
    
    data = json.loads(body)
    try:
        con = do.OpenConnection("localhost")
        cur = con.cursor(dictionary=True)
        table = do.Table("md_interdaily", con)
        for index,serie in enumerate(data):
            print(tickers[index])
            if len(serie) == 0:
                print("no data")
                continue
            
            df = pd.read_json(json.dumps(serie), orient="records")
            df.insert(0, "ticker", tickers[index], allow_duplicates=True)
            df.set_index(["ticker","date","minute"], inplace=True, verify_integrity =True)
            
            table.write_df(df, cur)
        con.commit()
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
    