# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 15:07:01 2018

@author: Asus
"""

import numpy as np
import mysql.connector
import database_operations as do
import json
import pandas as pd

class QueryGetter(object):        
    def iterator(self, cur, tags_seq):
        r = cur.fetchone()
        if r is None:
            return None, None
        adsh = r["adsh"]
        values = np.zeros((len(tags_seq),1))
        
        while r is not None:
            if adsh == r["adsh"]:
                values[tags_seq[r["tag"]]] = float(r["value"])
                r = cur.fetchone()
            else:
                yield adsh, values
                values = np.zeros((len(tags_seq),1))
                adsh = r["adsh"]          
        
        cur.close()
        
class MatrixGetter(object):
    def get_tag_list(self, uom, fy, con):
        tags = {}
        try:
            cur = con.cursor(dictionary=True)
            with open("Queries/mining_tags.sql") as f:
                sql_script = f.read()
            cur.execute(sql_script, {"fy":fy, "uom":uom})
            
            for r in cur:
                tags[r["tag"]] = r["cnt"]
        except mysql.connector.Error as e:
            print(e)
            tags = None
        return tags
    
    def get_matrix(self, uom, fy, tags, con):
        matrix = None
        columns = None
        rows = None
        try:
            data = []
            cur = con.cursor(dictionary=True)
            cur.execute("create temporary table req_tags (tag VARCHAR(256) CHARACTER SET utf8 not null, PRIMARY KEY (tag))")
            cur.executemany("insert into req_tags (tag) values (%s)", list((e,) for e in tags))
            
            with open("Queries/mining_data.sql") as f:
                sql_script = f.read()
            cur.execute(sql_script, {"fy":fy, "uom":uom})
            
            getter = QueryGetter()
            columns = [e for e in tags]
            tags_seq = {}
            for i in range(0,len(columns)):
                tags_seq[columns[i]] = i
                
            for adsh, values in getter.iterator(cur, tags_seq):
                data.append([adsh, values])
                
            cur.close()
                
            m = len(data)
            n = len(columns)
            matrix = np.zeros((m,n))
            rows = []
            for i in range(0,m):
                rows.append(data[i][0])
                for j in range(0,n):
                    matrix[i,j] = data[i][1][j]
                    
        except mysql.connector.Error as e:
            print(e)
                    
        return matrix, rows, columns

def mine():
    con = do.OpenConnection(host="95.31.1.243")
    mg = MatrixGetter()
    tags = mg.get_tag_list("USD", 2016, con)
    #tags_filtered = [t for t in tags]
    tags_filtered = []
    for t, c in tags.items():
        if c>10: tags_filtered.append(t)
            
            
    matrix, rows, columns = mg.get_matrix("USD", 2016, tags_filtered, con)
    con.close()
    
    np.save("LinearRegression/pre_data_10", matrix)
    with open("LinearRegression/pre_columns_10","w") as f:
        f.write(json.dumps(columns))
    with open("LinearRegression/pre_rows_10","w") as f:
        f.write(json.dumps(rows))