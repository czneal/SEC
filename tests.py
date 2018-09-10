# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:54:55 2018

@author: Asus
"""

import classificators as cl
import database_operations as do
import sys
import json
import pandas as pd
import traceback
import tree_operations as to

def set_chapters(items):
    val = {}
    val["chapter"] = items["chapter"]
    val["bs"] = int(cl.ChapterClassificator.match_balance_sheet(items["chapter"]))
    val["cf"] = int(cl.ChapterClassificator.match_cash_flow(items["chapter"]))
    val["si"] = int(cl.ChapterClassificator.match_statement_income(items["chapter"]))
    return pd.Series(val)

def chapter_classification():
    try:
        result = pd.DataFrame(columns=["adsh", "chapter"])
        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute("select * from reports where fin_year=2016")
        for r in cur:
            structure = json.loads(r["structure"])
            for chapter_name in structure:            
                result = result.append(pd.Series({"adsh":r["adsh"], "chapter":chapter_name}), ignore_index = True)
    #        result.append([r["adsh"], 0, 0, 0])
    #        for chapter_name, chapter in structure.items():
    #            if len(chapter) == 0:
    #                continue
    #            
    #            result[-1][1] += int(cl.ChapterClassificator.match_balance_sheet(chapter_name))
    #            result[-1][2] += int(cl.ChapterClassificator.match_cash_flow(chapter_name))
    #            result[-1][3] += int(cl.ChapterClassificator.match_statement_income(chapter_name))                
        con.close()
        
    except:
        print(sys.exc_info())
        con.close()
        
        
    result = result.set_index("adsh")
    result["bs"] = 0
    result["cf"] = 0
    result["si"] = 0
    
    result = result.apply(set_chapters, axis=1)
    filtered = result[(~(result["bs"] | result["cf"] | result["si"]))]
    
    grp = result.groupby("adsh")[["bs","cf", "si"]].sum()
    
    filtered = grp[(grp["bs"]<1) | (grp["cf"]<1) | (grp["si"]<1)]
    
#liab = cl.LiabilititesClassificator()
#print(liab.predict("CashCollateralDepositsForCommodityPurchases"))
    
def chapter_analizator(fy):
    try:
        con = do.OpenConnection(host="95.31.1.243")
        cur = con.cursor(dictionary=True)
        cur.execute("select * from reports where fin_year=%(fy)s", {"fy":fy})
        unclass_chapters = []
        for index, r in enumerate(cur):
            structure = json.loads(r["structure"])
            for chapter_name in structure:
                if cl.ChapterClassificator.match(chapter_name) == None:
                    unclass_chapters.append([chapter_name, r["cik"]])
            print("\rProcessed with {0}".format(index), end="")
                    
        df = pd.DataFrame(data=unclass_chapters, columns=["chpater_name", "cik"])
        df.to_csv("unclass_chapters.csv", sep="\t")
        con.close()
    except:
        con.close()
        info = sys.exc_info()
        print(info[0],info[1])
        traceback.print_tb(info[2])
        
def double_classification(fy):
    try:
        con = do.OpenConnection(host="server")
        cur = con.cursor(dictionary=True)
        cur.execute("select * from reports where fin_year=%(fy)s", {"fy":fy})
        chapters = []
        for index, r in enumerate(cur):
            structure = json.loads(r["structure"])
            for chapter_name in structure:
                cnt = 0
                if cl.ChapterClassificator.match_balance_sheet(chapter_name):
                    cnt += 1
                if cl.ChapterClassificator.match_cash_flow(chapter_name):
                    cnt += 1
                if cl.ChapterClassificator.match_statement_income(chapter_name):
                    cnt += 1
                if cnt > 1:
                    chapters.append([chapter_name, r["cik"], cnt])
            print("\rProcessed with {0}".format(index), end="")
                    
        df = pd.DataFrame(data=chapters, columns=["chapter_name", "cik", "cnt"])
        df.to_csv("doubleclass_chapters.csv", sep="\t")
        con.close()
    except:
        con.close()
        info = sys.exc_info()
        print(info[0],info[1])
        traceback.print_tb(info[2])
        
def liabilities_weights(fy):
    try:
        con = do.OpenConnection(host="server")
        cur = con.cursor(dictionary=True)
        cur.execute("select * from reports where fin_year=%(fy)s and trusted=1", {"fy":fy})
        data = []
        for index, r in enumerate(cur):
            structure = json.loads(r["structure"])
            for chapter_name in structure:
                if not cl.ChapterClassificator.match_balance_sheet(chapter_name):
                    continue
                for _, liab in to.enumerate_tags(structure, tag="us-gaap:Liabilities"):
                    for n, _, w in to.enumerate_tags_weight(liab):
                        data.append([r["cik"], r["adsh"], n, w])
            print("\rProcessed with {0}".format(index), end="")
        print()
        df = pd.DataFrame(data, columns=["cik","adsh","name","weight"])
        df = df[df["weight"] != 1]
        df.to_csv("Outputs/liabilities_neg_weights.csv",sep="\t")
    except:
        raise
    finally:
        con.close()
        
liabilities_weights(2016)