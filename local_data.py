# -*- coding: utf-8 -*-
"""
Created on Sun Apr 15 10:22:52 2018

@author: Asus
"""

import database_operations as do
import pandas as pd
import sys
import traceback
import json
import tree_operations as to
import classificators as cl

def liabilities_stat(fy):
    liab = {}
    bs_tags = {}
    try:
        con = do.OpenConnection("server")
        cur = con.cursor(dictionary=True)
        cur.execute("select * from reports where trusted=1 and fin_year >= %(fy)s",{"fy":fy})
        
        for i, r in enumerate(cur):
            structure = json.loads(r["structure"])
            for chapter_name,chapter in structure.items():
                if not cl.ChapterClassificator.match_balance_sheet(chapter_name):
                    continue
                for name, node in to.enumerate_tags(chapter, tag="us-gaap:LiabilitiesAndStockholdersEquity"):
                    for t, _ in to.enumerate_tags(node):
                        if t in bs_tags:
                            bs_tags[t]+=1
                        else:
                            bs_tags[t] = 1
                for name, node in to.enumerate_tags(chapter,tag="us-gaap:Liabilities"):
                    for t, _ in to.enumerate_tags(node):
                        if t in liab:
                            liab[t]+=1
                        else:
                            liab[t]=1
            print("\rPrecessed with {0}".format(i), end="")
        print()
        for t in liab:
            if t in bs_tags:
                bs_tags.pop(t)
    except:
        con.close()
        print(sys.exc_info())
        print(r["adsh"])
        
    sort = sorted(liab.items(), key=lambda x:x[1], reverse=True)    
    with open("liabilities_stat.csv","w") as f:
        for e in sort:
            f.write("{0}\t{1}\n".format(e[0], e[1]))
    sort = sorted(bs_tags.items(), key=lambda x:x[1], reverse=True)
    with open("liab_stockhold_stat.csv","w") as f:
        for e in sort:
            f.write("{0}\t{1}\n".format(e[0], e[1]))
            
       
def update_formula(root, weight, formula, cik, adsh):
    if root is None or root["children"] is None:
        return
        
    for child_name, child in root["children"].items():
        w = weight*child["weigth"]
        if child_name in formula:
            formula[child_name].append([cik, adsh, w])
        else:
            formula[child_name] = [[cik, adsh, w]]
        update_formula(child, w, formula, cik, adsh)

def income_formula(structure, formula, cik, adsh):
    for chapter_name, chapter in structure.items():
        if not cl.ChapterClassificator.match_statement_income(chapter_name):
            continue
        for _, node in to.enumerate_tags(chapter, tag="us-gaap:NetIncomeLoss"):
            update_formula(node, 1, formula, cik, adsh)
          

def income_formula_loop(fy):
    try:
        formula = {}
        con = do.OpenConnection("server")
        cur = con.cursor(dictionary=True)
        cur.execute("select * from reports where fin_year>=%(fy)s and trusted=1", {"fy":fy})
        for index, report in enumerate(cur):
            structure = json.loads(report["structure"])
            income_formula(structure, formula, report["cik"], report["adsh"])
            print("\rProcessed with {0}".format(index), end="")
        print()
        with open("income_formula.csv","w") as f:
            f.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format("tag","cik","adsh","weight","sign"))
            for index, (k,v) in enumerate(formula.items()):
                print("\rProcessed with {0}".format(index), end="")
                for elem in v:
                    sql_select = """
                        select value from mgnums 
                        where adsh=%(adsh)s 
                            and version = %(version)s
                            and tag = %(tag)s """
                    cur.execute(sql_select,{"adsh":elem[1], 
                                            "version":k.split(":")[0],
                                            "tag":k.split(":")[1]})
                    rows = cur.fetchall()
                    sign = 0
                    for r in rows:
                        if r["value"] >= 0: 
                            sign = 1
                        else:
                            sign = -1
                    f.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(k,elem[0],elem[1],elem[2], sign))
            print()
    except:
        raise
        
        
def income_stat(fy):
    try:
        
        con = do.OpenConnection("server")
        cur = con.cursor(dictionary=True)
        cur.execute("select * from reports where fin_year=%(fy)s", {"fy":fy})
        tags = {}
        for index, report in enumerate(cur):
            structure = json.loads(report["structure"])
            double_chapter = False
            for chapter_name, chapter in structure.items():
                if not cl.ChapterClassificator.match_statement_income(chapter_name):
                    continue
                for name, node in to.enumerate_tags(chapter):
                    if name not in tags:
                        tags[name] = 1
                    elif not double_chapter:
                        tags[name] +=1
                double_chapter = True
                
            print("\rProcessed with {0}".format(index), end="")
        print()                
        tags = sorted(tags.items(),key=lambda x:x[1], reverse=True)
        with open("income_stat.csv","w") as f:
            for name, stat in tags:
                f.write("{0}\t{1}\n".format(name, stat))
                
    except:
        con.close()
        print(sys.exc_info())
        traceback.print_tb(sys.exc_info()[2])
        
def tag_graph(fy):
    try:
        con = do.OpenConnection()
        cnx_write = do.OpenConnection()
        
        taggraph = do.Table("taggraph", cnx_write)
        cur_write = cnx_write.cursor()
        
        cur = con.cursor(dictionary=True)        
        cur.execute("select * from reports where fin_year=%(fy)s and trusted=1", {"fy":fy})
        
        for index, r in enumerate(cur):
            structure = json.loads(r["structure"])
            for chapter_name, chapter in structure.items():
                for name in chapter:
                    taggraph.write({"cik":0,"parent":1,"child":2,"weight":3,"fy":4},
                                   [r["cik"], chapter_name[:256], name, 1.0, fy], cur_write)
                for name, node in to.enumerate_tags(chapter):
                    if node["children"] is not None:
                        for child_name, child_node in node["children"].items():
                            taggraph.write({"cik":0,"parent":1,"child":2,"weight":3,"fy":4},
                                           [r["cik"], name, child_name, child_node["weight"], fy], cur_write)
                    
            print("\rprocessed with {0}".format(index), end="")
        print()
        taggraph.flush(cur_write)
        cnx_write.commit()
    except:
        con.close()
        info = sys.exc_info()
        print(info[0], info[1])
        traceback.print_tb(info[2])
        
def tag_graph_sign():
    sql_string="""
    select tg.*, n.sign from taggraph tg
    left outer join
    ( 
    	select r.cik, concat(version,':',tag) as tagname, fin_year,
    		case value>=0
    			when true then 1
                else -1
    		end as sign
    	from reports r, mgnums n
    	where n.adsh = r.adsh
    		and fin_year in (2013,2014,2015,2016,2017)
            and form='10-k'
    ) n
    on tg.child = tagname
    	and tg.cik = n.cik
        and tg.fy = n.fin_year
    """
    try:
        con = do.OpenConnection("server")
        cur = con.cursor()
        cur.execute(sql_string)
        columns = ["id", "cik","parent","child","fy","weight","sign"]
        df = pd.DataFrame(data=cur.fetchall(), columns=columns)
        df.set_index(["id"], inplace=True)
        df.to_csv("tag_graph_sign.csv", sep = "\t")                    
                
    except:
        con.close()
        raise

def look_income_formula():
    df = pd.read_csv("income_formula.csv", sep="\t")
#    dupl1 = df.drop_duplicates(subset=["tag", "weight"])
#    dupl1 = dupl1.sort_values(by=["tag"])
#    dupl2 = df.drop_duplicates(subset=["tag"])
#    dupl2 = dupl2.sort_values(by=["tag"])
#    dupl1.to_csv("income_dupl1.csv", sep="\t")
#    dupl2.to_csv("income_dupl2.csv", sep="\t")
    
    dupl = df.drop_duplicates(subset=["tag", "weight"])
    dupl = dupl.drop(["cik","adsh","sign"],axis=1)
    dupl = dupl.groupby(['tag']).size().reset_index(name='counts')
    dupl = dupl.sort_values(by=["counts"], ascending = False)
    dupl.to_csv("income_formula_err.csv")
    
    
income_formula_loop(2015)
look_income_formula()
    
    
    