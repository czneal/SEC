# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:54:55 2018

@author: Asus
"""

import classificators as cl
import database_operations as do
import json
import pandas as pd
import numpy as np
import tree_operations as to
import liabilities_custom as lc
import log_file
from settings import Settings


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
    years = list(range(Settings.years()[0], Settings.years()[1]))

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
    df = pd.read_csv("outputs/diffliabs/2018-11-13/liab_custom.csv").set_index("adsh").sort_index()

    print("read structures...", end="")
    lshe = lc.LSHEAdvanced(df.index)
    print("ok")

    print("start calculus...", end="")
    df["lshe_adv"] = df.apply(lambda x: lshe.calc(x.name), axis=1)
    l = 'us-gaap:Liabilities'
    df['err_lshe_adv'] = np.abs((df['lshe_adv']-df[l])/df[l])
    df.to_csv('outputs/diffliabs/2018-11-13/liab_custom_lshe.csv')
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

#df = calc_liabilities_variants()
df = liab_lshe_adv()

#f = xbrl_scan.AdshFilter("outputs/adsh_for_reread.txt")
#log = xbrl_file.LogFile("outputs/l.txt")
#for y in range(2013,2018):
#    for m in range(1,13):
#        xbrl_scan.scan_period(y, m, log, adsh_filter = f.check)

#tickers = read_values("select * from nasdaqtraded where cik is not null")
#
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
#df = pd.merge(df, tickers, how='left', left_on='cik', right_on='cik')
#
#f = df[df['us-gaap:Assets'].isnull() &
#       df['us-gaap:AssetsCurrent'].isnull() &
#       df['us-gaap:AssetsNoncurrent'].isnull()]
#
#f.to_excel("outputs/assets.xlsx")

