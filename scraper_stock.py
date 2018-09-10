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

def alpha_vantage(symbol, compact=True):
    params = {"function":"TIME_SERIES_DAILY_ADJUSTED",
              "symbol":symbol,
              "outputsize": "compact" if compact else "full",
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
    
    return df


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

df = alpha_vantage("WFC", compact=False)
df.to_csv("outputs/wfc_stock.csv", sep="\t")