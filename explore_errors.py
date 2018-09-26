# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 18:42:39 2018

@author: Asus
"""

import pandas as pd
import numpy as np
import database_operations as do
from settings import Settings
import tree_operations as to
import json

def read_class_log(years):
    frames = []
    for y in years:
        frames.append(pd.read_csv("outputs/diffliabs/liab_class_"+str(y)+".log",
                                  names = ["adsh","type","ptag","ctag","status","val"],
                                   sep = "\t",
                                   skiprows=1))
    
    df = pd.concat(frames, ignore_index=True).set_index(["adsh","type","ptag","ctag"])
    df.replace(to_replace="\[+|\]+", value="", regex=True, inplace=True)
    df["val"] = df["val"].astype('float')
    
    return df.sort_index()

def read_errors(years):
    frames = []
    for y in years:
        frames.append(pd.read_csv("outputs/diffliabs/liab_custom_"+str(y)+".csv"))
    
    return pd.concat(frames, ignore_index=True).set_index("adsh").sort_index()

def read_reports_attr(years):
    s = "("
    for y in years:
        s += "{0},".format(y)
    s = s[:-1] + ")"
    
    try:
        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute("""select adsh, trusted, 
                    	case structure
                    		when '{}' then 0
                            else 1
                        end as exist, company_name
                    from reports r, companies c
                    where fin_year in """ + s + """
                        and c.cik = r.cik""" + Settings.select_limit())
        
        reports = pd.DataFrame(cur.fetchall())
        reports.set_index("adsh", inplace=True)    
    finally:
        con.close()
        
    return reports

def read_reports_structure(adshs):
    try:
        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute("create temporary table adshs (adsh VARCHAR(20) CHARACTER SET utf8 not null, PRIMARY KEY (adsh))")
        cur.executemany("insert into adshs (adsh) values (%s)", list((e,) for e in adshs))
        cur.execute("""select r.adsh, structure 
                        from reports r, adshs a 
                        where r.adsh = a.adsh""")
        df = pd.DataFrame(cur.fetchall())
        df.set_index("adsh", inplace=True)
    finally:
        con.close()
        
    return df

def read_report_nums(adsh):
    try:
        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute("select concat(version,':',tag) as tag, value from mgnums where adsh = (%s)",(adsh,))
        df = pd.DataFrame(cur.fetchall()).set_index("tag")
        df["value"] = df["value"].astype('float')
    finally:
        con.close()
    return df.sort_index()
def explore_another_type_errors(tp, df, reports, assets_limit=10000000):
    descr = []
    df["lshe"] = (df["us-gaap:LiabilitiesAndStockHoldersEquity"]-
                  df["us-gaap:StockholdersEquity"])
    
    f = df[pd.isnull(df["us-gaap:Liabilities"])]
    
    stat = f.notna().sum()
    descr.append(["no Liabilities", "{0}".format(f.shape[0])])
    descr.append(["us-gaap:Assets", "{0}".format(stat["us-gaap:Assets"])])
    descr.append(["LiabilitiesAndStockHoldersEquity", "{0}".format(stat["us-gaap:LiabilitiesAndStockHoldersEquity"])])
    descr.append(["LSHE","{0}".format(stat["lshe"])])
    
    f = f[f[tp] != f["lshe"]]
    f = pd.merge(f, reports, on="adsh")
       
    f = f[f["us-gaap:Assets"]>assets_limit]
    descr.append(["Assets>{0:,.0f}".format(assets_limit), f.shape[0]])
    
    f = f.sort_values(by="us-gaap:Assets", ascending =False)
    url = "https://www.sec.gov/cgi-bin/viewer?action=view&cik={0}&accession_number={1}&xbrl_type=v#"
    f["url"] = f.apply(lambda x: url.format(x["cik"], x.name), axis=1)
    columns = []
    for c in f.columns:
        if c in set([tp]):
            columns.append(c)
            continue
        if c.startswith("lc") or c.startswith("err"):
            continue
        columns.append(c)
        
    return f[columns], pd.DataFrame(descr, columns=["name","value"])
    
def explore_one_type_errors(tp, df, reports, assets_limit=10000000):
    tp_err = "err_" + tp    
    
    df.loc[df[tp]==0.0, [tp, tp_err]] = np.nan
    
    stat = df.notna().sum()
    
    descr = [] 
    
    descr.append(["total", "{0}".format(df.shape[0])])
    descr.append(["us-gaap:Liabilities", "{0}".format(stat["us-gaap:Liabilities"])])
    descr.append(["us-gaap:Assets", "{0}".format(stat["us-gaap:Assets"])])
    descr.append(["LiabilitiesAndStockHoldersEquity", "{0}".format(stat["us-gaap:LiabilitiesAndStockHoldersEquity"])])
    descr.append(["calculated","{0}".format(stat[tp])])
    descr.append(["mean", "{0}".format(df[tp_err].mean())])
         
    res = pd.merge(df, reports, on="adsh")
    
    f = (res[tp_err]>0.0).sum()
    descr.append([tp_err + " > 0.0", f])
    
    f = (res[tp_err]>0.1).sum()
    descr.append([tp_err + " > 0.1", f])
    
    f = res[res[tp_err]>0.1]    
    f = f[f["us-gaap:Assets"]>assets_limit]
    descr.append(["Assets>{0:,.0f}".format(assets_limit), f.shape[0]])
    
    f = f[f["exist"] == 1]
    descr.append(["structure exist", f.shape[0]])
    
    trusted = (f["trusted"] == 1).sum()
    descr.append(["trusted", trusted])
    
    f = f.sort_values(by=tp_err, ascending =False)
    url = "https://www.sec.gov/cgi-bin/viewer?action=view&cik={0}&accession_number={1}&xbrl_type=v#"
    f["url"] = f.apply(lambda x: url.format(x["cik"], x.name), axis=1)
    columns = []
    for c in f.columns:
        if c in set([tp, tp_err]):
            columns.append(c)
            continue
        if c.startswith("lc") or c.startswith("err"):
            continue
        columns.append(c)
        
    return f[columns], pd.DataFrame(descr, columns=["name","value"])

def find_misclassified_tags(structure, log, adsh, tp, nums):
    values = []
    
    for (_, _, _, liab) in to.enumerate_tags_basic(structure, tag="us-gaap:LiabilitiesAndStockHoldersEquity", chapter="bs"):
        for node in to.enumerate_tags_basic_leaf(liab):
            row=[adsh, node[4]+node[1], node[5], node[2]]
            if node[1] in nums.index:
                row.append(nums.loc[node[1]]["value"])    
            else:
                row.append(None)    
            index = (adsh, tp, node[0], node[1])
            if index not in log.index:
                row.append("?")
            else:            
                row.append(log.loc[index].iloc[0]["status"])
                
            values.append(row)
            
    return values

def find_missed_values(adsh, calc_val, real_val, nums):
    diff = abs(calc_val - real_val)
    
    miss_vals = []
    
    for tag, value in nums.iterrows():
        if abs(value["value"]) == diff:
            miss_vals.append([adsh, tag, value["value"]])   
            
    return miss_vals

def explore_errors(res, log, tp, alg):
    structures = read_reports_structure(res.index)
    miss_vals = []
    values = []
    for adsh, row in res.iterrows():
        nums = read_report_nums(adsh)
        v = find_misclassified_tags(
                    json.loads(structures.loc[adsh]["structure"]), 
                    log, adsh, alg, nums)        
        values.extend(v)
                
        
        v = find_missed_values(adsh, row[tp], 
                                    row["us-gaap:Liabilities"], nums)
        miss_vals.extend(v)
            
    
    return (pd.DataFrame(miss_vals, columns=["adsh", "tag", "value"]),
            pd.DataFrame(values, columns=["adsh", "tag", "leaf", "weight", "value", "class"]))

def one_error(alg, suffix, errors, reports, log, one_another):
    
    alg_sum_name = alg+"_"+suffix
    
    print("filter result errors...", end="")
    if one_another == 'first':
        res, descr = explore_one_type_errors(alg_sum_name, errors, reports)
    else:
        res, descr = explore_another_type_errors(alg_sum_name, errors, reports)
    print("ok")
    
    print("try to find misclassified tags and values...", end="")
    (m_values, values) = explore_errors(res, log, alg_sum_name, alg)
    print("ok")
    
    print("save results to excel...", end="")
    writer = pd.ExcelWriter('outputs/liab_' + alg_sum_name + one_another +'.xlsx', engine='xlsxwriter')
    workbook = writer.book
    # Add some cell formats.
    format1 = workbook.add_format({'num_format': '#,##0.00'})
    format2 = workbook.add_format({'num_format': '0%'})
    
    res.to_excel(writer, sheet_name="errors")
    descr.to_excel(writer, sheet_name="description")
    m_values.to_excel(writer, sheet_name="missed values")
    values.to_excel(writer, sheet_name="values")
    
    # Set the column width and format.
    worksheet = writer.sheets["errors"]
    worksheet.set_column('C:G', 18, format1)
    worksheet.set_column('J:J', 25, None)
    worksheet.set_column('A:A', 20, None)
    worksheet.set_column('H:H', None, format2)
    
    worksheet = writer.sheets["missed values"]
    worksheet.set_column('B:B', 20, None)
    worksheet.set_column('D:D', 18, format1)
    
    worksheet = writer.sheets["values"]
    worksheet.set_column('B:B', 20, None)
    worksheet.set_column('F:F', 18, format1)
    
    writer.save()
    print("ok")
    
algs = ["lcpc_leaf"]
suffs = ["sum", "leaf", "tops"]
      
print("load errors, classification log, report attributes...",end="")
#years = [2013,2014, 2015, 2016]
years = [2017]
reports = read_reports_attr(years)
errors = read_errors(years)
log = read_class_log(years)
print("ok")

for alg in algs:
    for suff in suffs:
        print(alg, suff,"...", end="")
        one_error(alg, suff, errors, reports, log, "second")
        print("ok")


        
    
