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
import numpy as np
import tree_operations as to
import liabilities_custom as lc
import log_file
from settings import Settings
import xbrl_scan
import xbrl_file

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
    
def double_classification():
    try:
        years = 'fin_year between ' + str(Settings.years()[0]) + ' and ' + str(Settings.years()[1])
        con = do.OpenConnection(host="server")
        cur = con.cursor(dictionary=True)
        sql = "select * from reports where " + years + Settings.select_limit()
        cur.execute(sql)
        chapters = []
        for index, r in enumerate(cur):
            structure = json.loads(r["structure"])
            for chapter_name in structure:
                symbols = []
                if cl.ChapterClassificator.match_balance_sheet(chapter_name):
                    symbols.append('bs')
                if cl.ChapterClassificator.match_cash_flow(chapter_name):
                    symbols.append('cf')
                if cl.ChapterClassificator.match_income_statement(chapter_name):
                    symbols.append('is')
                
                chapters.append([r["adsh"], r["cik"], chapter_name, set_mask(symbols)])
            print("\rProcessed with {0}".format(index), end="")
                    
        df = (pd.DataFrame(data=chapters, columns=["adsh", "cik", "chapter", 'mask'])
                .set_index('adsh')
                )
        df.to_csv(Settings.output_dir() + "doubleclass_chapters.csv")
    finally:
        con.close()
    
    return df
        
def liabilities_weights_year(fy):
    try:
        con = do.OpenConnection()
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
        
def calc_liabilities_variants():
    years = list(range(2013, 2018))
    
    print("loading models...", end="")
    diff_liabs = lc.DifferentLiabilities()
    print("ok")
    print("open log file...", end="")
    log = log_file.LogFile("outputs/liab_class.log", 
                            append=False, 
    
                        timestamp=False)
    print("ok")
    
    print("start calculus:")
    frames = []
    for fy in years:
        print("\tyear:{0}".format(fy))
        
        df = diff_liabs.calc_liabilities(fy, log)
        frames.append(df)
        df["fy"] = fy
        
    df = pd.concat(frames)
    log.close()
    print("end calculus")
    
    print("save to csv...", end="")
    df.set_index(["adsh"], inplace=True)
    df.loc[df["us-gaap:Liabilities"]==0.0, "us-gaap:Liabilities"] = np.nan
    error_calculus(df)
    df.to_csv("outputs/liab_custom.csv")
    print("ok")
    
    return df
    
def error_calculus(df):
    l = "us-gaap:Liabilities"
    errors = []
    for i, c in enumerate(df.columns):
        if c.startswith("lcc") or c.startswith("lcpc"):     
            df["err_"+c] = np.abs((df[c] - df[l])/df[l])
            errors.append(["err_"+c, np.mean(df["err_"+c]), df["err_"+c].max(),
                           df["err_"+c].idxmax()])
    
    a = "us-gaap:LiabilitiesAndStockHoldersEquity"
    b = "us-gaap:StockholdersEquity"
    df["lshe-she"] = df[a] - df[b]
    df["err_lshe_she"] = np.abs((df["lshe-she"] - df[l])/df[l])
    errors.append(["err_lshe_she", np.mean(df["err_lshe_she"]), 
                   df["err_lshe_she"].max(),
                   df["err_lshe_she"].idxmax()])
    
    errors = pd.DataFrame(errors, columns=["error_name", "mean", "max", "argmax"]).set_index("error_name")
    
    return errors

def liab_lshe_adv():
    df = pd.read_csv("outputs/diffliabs/liab_custom.csv").set_index("adsh").sort_index()
    f = df
    
    print("read structures...", end="")
    lshe = lc.LSHEAdvanced(f.index)
    print("ok")
    
    print("start calculus...", end="")
    f["lshe_adv"] = f.apply(lambda x: lshe.calc(x.name), axis=1)
    print("ok")

def read_tag_values(query, tags):
    try:
        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute("create temporary table tags (tag VARCHAR(256) CHARACTER SET utf8 not null, PRIMARY KEY (tag))")
        cur.executemany("insert into tags (tag) values (%s)", list((e,) for e in tags))
        cur.execute(query)
        df = (pd.DataFrame(cur.fetchall())
                .set_index(["adsh", 'tag'])
                .unstack()
            )
        df.columns = [c[1] for c in df.columns]
    finally:
        con.close()
        
    return df

def read_values(query):
    try:
        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute(query)
        df = (pd.DataFrame(cur.fetchall()))
    finally:
        con.close()
        
    return df

f = xbrl_scan.AdshFilter("outputs/adsh_for_reread.txt")
log = xbrl_file.LogFile("outputs/l.txt")
for y in range(2013,2018):
    for m in range(1,13):
        xbrl_scan.scan_period(y, m, log, adsh_filter = f.check)

#years = " between {0} and {1}".format(Settings.years()[0], Settings.years()[1])
#df= read_tag_values("""select adsh, concat(version,':',n.tag) as tag, value 
#                from mgnums n, tags t
#                where t.tag = n.tag and fy """ + years + 
#                Settings.select_limit(),
#                ['Assets', 'AssetsCurrent',
#                 'AssetsNoncurrent', 
#                 'LiabilitiesAndStockHoldersEquity',
#                 'Liabilities', 'LiabilitiesCurrent',
#                 'LiabilitiesNoncurrent'])
#    
#reports = do.read_reports_attr(range(Settings.years()[0], Settings.years()[1] + 1))
#df = pd.merge(reports, df, how='left', left_index=True, right_index=True)
#url = "https://www.sec.gov/cgi-bin/viewer?action=view&cik={0}&accession_number={1}&xbrl_type=v#"
#df["url"] = df.apply(lambda x: url.format(x["cik"], x.name), axis=1)
#
#f = df[df['us-gaap:Assets'].isnull() &
#       df['us-gaap:AssetsCurrent'].isnull() &
#       df['us-gaap:AssetsNoncurrent'].isnull()] 




#df = pd.read_csv("outputs/liab_custom_variants.csv", sep="\t")
#df = cal_liabilities_variants(2017)
#df.to_csv("outputs/liab_custom_variants_2017.csv", sep="\t")
#df.set_index(["adsh"], inplace=True)


#structure = json.loads(open("outputs/structure.json").read())
#for (p, c, _, root) in to.enumerate_tags_basic(structure, 
#                               tag="us-gaap:liabilitiesAndStockHoldersEquity", 
#                               chapter="bs"):
#    for node in to.enumerate_tags_basic_leaf(root):
#        print(node[4] + node[1], node[5], node[2])
#    facts = {}
#    for index, (p, c, leaf) in enumerate(to.enumerate_tags_parent_child_leaf(root)):
#        if leaf: facts[c] = float(index)
#    
#    print("by_leafs:{0}".format(to.TreeSum.by_leafs(facts, root)))
#    
#    facts["us-gaap:Liabilities"] = 100
#    print("by_tops:{0}".format(to.TreeSum.by_tops(facts, root)))
#    
#    print(facts)
    
#print("\tenumerate_tags_basic")
#print([(c,w,p) for (c,w,p,_) in to.enumerate_tags_basic(structure)])
#print("\tenumerate_tags_basic chap_id = 'bs'")
#print([(c,w,p) for (c,w,p,_) in to.enumerate_tags_basic(structure,chapter='bs')])
#print("\tenumerate_tags_basic tag='us-gaap:Liabilities'")
#print([(c,w,p) for (c,w,p,_) in to.enumerate_tags_basic(structure, tag='us-gaap:Liabilities')])
#print("\tenumerate_tags_parent_child")
#print([(p,c) for (p,c) in to.enumerate_tags_parent_child(structure)])
#print("\tenumerate_tags_parent_child_leaf")
#print([(p,c,l) for (p,c,l) in to.enumerate_tags_parent_child_leaf(structure)])
#print("\tenumerate_tags_parent_child_leaf chapter='bs'")
#print([(p,c,l) for (p,c,l) in to.enumerate_tags_parent_child_leaf(structure, chapter='bs')])
