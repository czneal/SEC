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
        
    def calc_liabilities(self, fy):
        try:
            con = do.OpenConnection()
            cur = con.cursor(dictionary=True, buffered=True)
            cur.execute("select * from reports where fin_year=%(fy)s", {"fy":fy})
            params = do.Table("mgnums", con, buffer_size=100)
            
            for report_index, report in enumerate(cur):
                liabilities = self.calc_report_liabilities(con, report["structure"], report["adsh"])                
                    
                params.write({"adsh":0,"tag":1,"version":2,"value":3,"uom":4,"fy":5,"type":6,"ddate":7},
                             [report["adsh"],"mg_liabilities","mg",liabilities,
                              "usd", fy, "I", report["period"]],
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
        lshe = set()
        for chapter_name, chapter in structure.items():
            if not cl.ChapterClassificator.match_balance_sheet(chapter_name):
                continue
            for name, node in to.enumerate_tags(chapter,"us-gaap:LiabilitiesAndStockholdersEquity"):                        
                lshe.update([tag_name for tag_name, _ in to.enumerate_tags(node)])
                                
        cur_nums = con.cursor(dictionary=True)
        cur_nums.execute("select * from mgnums where adsh=%(adsh)s and uom='usd' and version<>'mg'", 
                         {"adsh":adsh})
        num_rows = cur_nums.fetchall()
        facts = {}        
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
        
#l = LiabilitiesCalculator(log=log_file.LogFile("lbclf/liab_test_log.csv", append=False))
#l.calc_liabilities(2016)
#fill_liabilities_table()

Y = error_exploration(2016)