# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 12:30:37 2018

@author: Asus
"""

import database_operations as do
import classificators as cl
from settings import Settings
import json
import tree_operations as to
import pandas as pd

def get_values():
    return ['bs', 'is', 'cf', 'a', 'ac', 'anc', 'l', 'lc', 'lnc', 'lshe', 'nil']
def get_key_words():
    words = {'bs':cl.ChapterClassificator.match_balance_sheet, 
             'is':cl.ChapterClassificator.match_income_statement, 
             'cf':cl.ChapterClassificator.match_cash_flow, 
             'a':'us-gaap:Assets', 
             'ac':'us-gaap:AssetsCurrent', 
             'anc':'us-gaap:AssetsNoncurrent', 
             'l':'us-gaap:Liabilities', 
             'lc':'us-gaap:LiabilitiesCurrent', 
             'lnc':'us-gaap:LiabilitiesNoncurrent', 
             'lshe':'us-gaap:LiabilitiesAndStockholdersEquity',
             'nil':'us-gaap:NetIncomeLoss'}
    return words

def set_mask(symbols, mask=0):
    values = get_values()
    for s in symbols:
        mask = mask|(1<<values.index(s))
    return mask

def get_mask(mask):
    unmasked = []
    values = get_values()
    for i, v in enumerate(values):
        if (1<<i)&mask:
            unmasked.append(v)
    return unmasked

def change_markers(markers, node, words):
    for w, f in words.items():
        if type(f) == type(str()):
            if len(node[4])<= markers[w]:
                markers[w] = -1
            if node[1].lower() == f.lower():
                markers[w] = len(node[4])
        else:
            if len(node[4])<= markers[w]:
                markers[w] = -1
            if ":" not in node[0] and f(node[0]):
                markers[w] = len(node[4])
    
def taggraph():
    try:
        years = 'fin_year between ' + str(Settings.years()[0]) + ' and ' + str(Settings.years()[1])
        con = do.OpenConnection(host="server")
        cur = con.cursor(dictionary=True)
        sql = "select * from reports where " + years + Settings.select_limit()
        cur.execute(sql)
        data = []
        for index, r in enumerate(cur):
            structure = json.loads(r["structure"])
            markers = {v:-1 for v in get_values()}
            words = get_key_words()
            for node in to.enumerate_tags_basic_leaf(structure):
                
                change_markers(markers, node, words)
                    
                symbols=[m for m,v in markers.items() if v!=-1]
                mask = set_mask(symbols)
                data.append([r["adsh"], r["cik"], 
                             node[0], node[1], node[2], node[5], 
                             mask, json.dumps(get_mask(mask)),
                             r["fin_year"], r["trusted"]])
            print("\rProcessed with {0}".format(index+1), end="")
                    
        df = (pd.DataFrame(data=data, columns=["adsh", "cik", 
                                               "parent", "child",
                                               "weight", "leaf",
                                               'mask', 'unmasked',
                                               'fy', 'trusted'])                
                )
        table = do.Table("taggraph", con)
        table.write_df(df, cur)
        con.commit()
    finally:
        con.close()
    
    return df

df = taggraph()