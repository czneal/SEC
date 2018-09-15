# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 15:07:01 2018

@author: Asus
"""

import numpy as np
import mysql.connector
import pandas as pd

class QueryGetter(object):        
    def iterator(self, cur, key_fields, tags):
        """iterator() -> numpy.ndarray(values(key_fields) + values(tags))
        cur - mysql.cursor orderd by adsh to database
        key_field - iteratable
        tags - iteratable
        if tag in tags missing in query than numpy.nan"""
        r = cur.fetchone()
        if r is None:
            return
        
        key_fields = set([name for name in key_fields])
        key_fields = key_fields.intersection(set([name for name in r]))
        
        header = {}        
        for i, v in enumerate(key_fields + tags):
            header[v.lower()] = i
        
        adsh = r["adsh"]
        values = np.empty((len(tags) + len(key_fields),1))
        values.fill(np.nan)
        
        while r is not None:
            if adsh == r["adsh"]:
                values[header[r["tag"].lower()]] = float(r["value"])
                r = cur.fetchone()
            else:
                for f in key_fields:
                    values[header[f.lower()]] = r[f]
                yield values
                values = np.empty((len(tags) + len(key_fields),1))
                values.fill(np.nan)
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
    
    def get_matrix(self, uom, fy, key_fields, tags, con):
        data = []
        cur = con.cursor(dictionary=True)
        cur.execute("create temporary table req_tags (tag VARCHAR(256) CHARACTER SET utf8 not null, PRIMARY KEY (tag))")
        cur.executemany("insert into req_tags (tag) values (%s)", list((e,) for e in tags))
        
        sql_script = open("Queries/mining_data.sql").read()
        cur.execute(sql_script, {"fy":fy, "uom":uom})
        getter = QueryGetter()
                        
        for values in getter.iterator(cur, tags, key_fields):
            data.append(values.tolist())
            
        cur.close()
        
        return pd.DataFrame(data, columns=key_fields + tags)