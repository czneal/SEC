# -*- coding: utf-8 -*-
"""
Created on Sun Apr  1 15:04:37 2018

@author: Asus
"""

import data_mining as dm
import database_operations as do
import mg_params_calculations as pc
import classificators as cl
import log_file
import tree_operations as to
import sys
import json
import numpy as np
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
import pandas as pd

class LiabilitiesCalculator(object):
    def __init__(self, log=None, err_log=None):
        print("initialize LiabilitiesCalculator...", end="")
        self.liab = cl.LiabilititesClassificator()
        self.log = log
        if log is None:
            self.log = log_file.LogFile()
        self.err_log = err_log            
        if err_log is None:
            self.err_log = self.log
        print("ok")
        
    def calc_liabilities(self, fy, custom_l_tag):
        try:
            con = do.OpenConnection()
            cur = con.cursor(dictionary=True, buffered=True)
            cur.execute("select * from reports where fin_year=%(fy)s", {"fy":fy})
            params = do.Table("mgnums", con, buffer_size=1000)
            
            for report_index, report in enumerate(cur):
                liabilities = self.calc_report_liabilities(con, report["structure"], report["adsh"])                
                    
                params.write({"adsh":report["adsh"],
                              "tag":custom_l_tag,
                              "version":"mg",
                              "value":liabilities,
                              "uom":"usd",
                              "fy":fy,
                              "type":"I",
                              "ddate":report["period"]},
                              con.cursor())
                print("\rProcessed with {0}".format(report_index), end="")
            
            print()
            params.flush(cur)
            con.commit()                 
        except:
            self.err_log.write(sys.exc_info()[0])
            self.err_log.write(sys.exc_info()[1])
            self.err_log.write_tb(sys.exc_info()[2])
        finally:
            con.close()
            self.log.close()
            self.err_log.close()
            
        return
    
    def calc_report_liabilities(self, con, structure, adsh):
        structure = json.loads(structure)
                     
        cur_nums = con.cursor(dictionary=True)
        cur_nums.execute("select * from mgnums where adsh=%(adsh)s and uom='usd' and version<>'mg'", 
                         {"adsh":adsh})
        num_rows = cur_nums.fetchall()
        
        for (p, c, _, root) in to.enumerate_tags_basic(structure, 
                               tag="us-gaap:liabilitiesAndStockHoldersEquity", 
                               chapter="bs"):
            facts = {}
            facts_leaf = {}
            for (p, c, leaf) in to.enumerate_tags_parent_child_leaf(root):
                facts [c] = 0.0
                if leaf:                    
                    facts_leaf[c] = 0.0
                
                    
                
            for num in num_rows:
                tag_to_test = num["version"] + ":" + num["tag"]
                if tag_to_test == "us-gaap:Liabilities":
                    continue
            
            if tag_to_test in lshe:
                result = ""
                predict = self.liab.predict(num["tag"])
                if predict>0.8:
                    facts[tag_to_test] = num["value"]
                    result = "ok"
                else:
                    result = "fail"
                self.log.write("{0}\t{1}\t{2}\t{3}".format(adsh,tag_to_test,result,predict))
            
        liabilities = to.calculate_by_tree(facts, structure, chapter="bs")
        liabilities = list(liabilities.items())
        if len(liabilities)!=0:
            liabilities = liabilities[0][1]
        else:
            liabilities = None
        return liabilities
    
def fill_liabilities_table():
    try:
        con = do.OpenConnection(host="server")
        mg = dm.MatrixGetter()
        tags = ["mg_liabilities", "Liabilities", "LiabilitiesAndStockholdersEquity", "StockholdersEquity", "Assets"]
        
        print("getting data...", end="")       
        matrix, rows, columns = mg.get_matrix("USD", 2016, tags, con)
        columns_idx = {columns[i]:i for i in range(len(columns))}
        tags.insert(0, "LSHE_SHE")
        print("ok")
        
        table = pc.ReportTable("mgliabilities")
        table.create(con, tags)
        
        values = {t:0.0 for t in tags}
        
        print("fill in table...", end="")
        for i in range(matrix.shape[0]):
            values["LSHE_SHE"] = float(matrix[i, columns_idx["LiabilitiesAndStockholdersEquity"]] - matrix[i, columns_idx["StockholdersEquity"]])
                
            for j in range(matrix.shape[1]):
                values[columns[j]] = float(matrix[i,j])
            table.insert(rows[i], 2016, values, con)
            
        con.commit()
        con.close()
        print("ok")
    except:
        con.close()
        do.print_error()
        
def error_exploration(fy):
    try:
        con = do.OpenConnection(host="server")
        cur = con.cursor(dictionary=True)
        cur.execute("""select * from mgliabilities 
                    where fy=%(fy)s 
                        and Liabilities <> 0 
                        and mg_liabilities<>0
                        and assets>100000000
                        and lshe_she <> 0""", {"fy":fy})
        data = []
        for r in cur:
            data.append([r["mg_liabilities"], r["LSHE_SHE"], r["Liabilities"]])
            
        Y = np.array(data)
        print("algorithm r2_score {0}, mean_squared_error {1}".format(r2_score(Y[:,2], Y[:,0]), 
              mean_squared_error(Y[:,2], Y[:,0])))
        print("LSHE-SHE r2_score {0}, mean_squared_error {1}".format(r2_score(Y[:,2], Y[:,1]),
              mean_squared_error(Y[:,2], Y[:,1])))
    except:
        do.print_error()
    finally:
        con.close()
        
    return Y

class DifferentLiabilities(object):
    def __init__(self):
        self.cldict = {}
#        self.cldict["lcc_old"] = cl.LiabClassStub()
#        self.cldict["lcc_new"] = cl.LiabClassStub()
#        self.cldict["lcpc_leaf"] = cl.LiabClassStub()
#        self.cldict["lcpc_tree"] = cl.LiabClassStub()
        self.cldict["lcc_old"] = cl.LiabClassSingle("LbClf/liabilities_class_dict_v2018-05-24.csv",
                               "LbClf/liabilities_class_v2018-05-24.h5")
        self.cldict["lcc_new"] = cl.LiabClassSingle("LbClf/liabilities_class_dict_v2018-08-17.csv",
                               "LbClf/liabilities_class_v2018-09-12.h5")
        self.cldict["lcpc_leaf"] = cl.LiabClassPC("LbClf/liabilities_class_dict_v2018-08-17.csv",
                               "LbClf/liabilities_class_pch_v2018-08-17.h5")
        self.cldict["lcpc_tree"] = cl.LiabClassPC("LbClf/liabilities_class_dict_v2018-08-17.csv",
                               "LbClf/liabilities_class_pch_v2018-09-12.h5")
        
    def calc_report_liabilities(self, con, structure, adsh, log):
        structure = json.loads(structure)
        
        #fetch all facts for this report             
        cur = con.cursor(dictionary=True)
        cur.execute("select * from mgnums where adsh=%(adsh)s and uom='usd' and version<>'mg'", 
                         {"adsh":adsh})
        #make them searcheable
        nums = {r["version"].lower() + ":" + r["tag"].lower(): float(r["value"]) for r in cur}
        #make for each classifier its own facts list
        cl_facts = {name:{} for name in self.cldict}
        cl_facts_leaf = {name:{} for name in self.cldict}
        liabs = {}   
        #find "us-gaap:liabilitiesAndStockHoldersEquity"
        for (_, _, _, root) in to.enumerate_tags_basic(structure, 
                               tag="us-gaap:LiabilitiesAndStockHoldersEquity", 
                               chapter="bs"):            
            #go trough structure and test fact names
            for (p, c, leaf) in to.enumerate_tags_parent_child_leaf(root):
                if c == "us-gaap:Liabilities": continue
                if c.lower() not in nums: continue
            
                for cl_name, cl_obj in self.cldict.items():
                    result = ""
                    predict = cl_obj.predict(p, c)
                    if predict>0.8:
                        cl_facts[cl_name][c] = nums[c.lower()]
                        if leaf:
                            cl_facts_leaf[cl_name][c] = nums[c.lower()]
                        result = "ok"
                    else:
                        result = "fail"
                    log.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}".format(adsh, cl_name, p, c, result, predict))
            
            #calculate liabilities
            for index, cl_name in enumerate(self.cldict):
                liabs[cl_name + "_leaf"] = to.TreeSum.by_leafs(cl_facts_leaf[cl_name], root)
                liabs[cl_name + "_tops"] = to.TreeSum.by_tops(cl_facts[cl_name], root)
                liabs[cl_name + "_sum"] = sum(cl_facts_leaf[cl_name].values())
            
            add_tags = ["us-gaap:Liabilities", "us-gaap:Assets", 
                          "us-gaap:LiabilitiesAndStockHoldersEquity",
                          "us-gaap:StockholdersEquity"]
            for tag in add_tags:
                if tag.lower() in nums:
                    liabs[tag] = nums[tag.lower()]
                
                
            break
        return liabs
    
    def calc_liabilities(self, fy, log):
        try:
            con = do.OpenConnection()
            cur = con.cursor(dictionary=True, buffered=True)
            cur.execute("select * from reports where fin_year=%(fy)s", {"fy":fy})
            
            data = []
            for report_index, report in enumerate(cur):
                liabs = self.calc_report_liabilities(con, report["structure"], report["adsh"], log)                
                liabs["adsh"] = report["adsh"]
                liabs["cik"] = report["cik"]
                data.append(liabs)
                print("\rProcessed with {0}".format(report_index+1), end="")
            
            print()
            
        finally:
            con.close()
            
        return pd.DataFrame(data)