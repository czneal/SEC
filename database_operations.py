# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 11:14:23 2017

@author: Asus
"""

import json
import mysql.connector
import sys
import traceback
import pandas as pd
from settings import Settings

def OpenConnection(host=Settings.host()):
    hosts = {"server":"server", "remote":"95.31.1.243","localhost":"localhost"}
    
    return mysql.connector.connect(user="app", password="Burkina!7faso", 
                              host=hosts[host], database="reports",
                              ssl_ca = Settings.ssl_dir()+"ca.pem",
                              ssl_cert = Settings.ssl_dir()+"client-cert.pem",
                              ssl_key = Settings.ssl_dir()+"client-key.pem",
                              connection_timeout = 1000)

class Table(object):
    def __init__(self, name, con, db_name="reports", buffer_size = 1000):
        self.table_name = name
        self.fields = set()
        self.not_null_fields = set()
        self.primary_keys = set()
        
        cur = con.cursor(dictionary=True)
        cur.execute("show columns from " + self.table_name + " from " + db_name)
        for r in cur.fetchall():
            if r["Extra"] == "auto_increment":
                continue
            self.fields.add(r["Field"].lower())
            if r["Null"] == "NO":
                self.not_null_fields.add(r["Field"].lower())
            if r["Key"] == "UNI":
                self.primary_keys.add(r["Field"].lower())
        cur.execute("SHOW INDEX FROM " + self.table_name + \
                   " FROM " +db_name + " where non_unique = 0 and column_name <> 'id'")
        for r in cur.fetchall():
            self.primary_keys.add(r["Column_name"].lower())
            
        self.insert_command = self.__insert_command()
        self.data = []
        self.buffer_size = buffer_size
    
    def truncate(self, con):
        cur = con.cursor()
        cur.execute("truncate table " + self.table_name)
        con.commit()
        
    def __insert_command(self):
        insert = "insert into " + self.table_name + "("
        values = ""
        for f in self.fields:
            insert += f + ","
            values += "%(" + f + ")s,"
            
        insert = insert[:-1] + ")\n values (" + values[:-1] + ")\n"
        insert += "on duplicate key update\n"
        for f in self.fields.difference(self.primary_keys):
            insert += f+"=values(" + f + "),"
            
        return insert[:-1]
    
    def write(self, values, cur):
        if type(values) == type(dict()):
            values = [values]
            
        for value in values:
            for f in self.fields:
                if f not in values:
                    values[f] = None
                elif values[f] == '':
                    values[f] = None
                
            for f in self.not_null_fields:
                if value[f] is None:
                    return
            
        self.data.append(values)
        if len(self.data) >= self.buffer_size:
            self.flush(cur)
        
    def flush(self, cur):
        if len(self.data)>0:
            cur.executemany(self.insert_command, self.data)
            self.data.clear()
            
    def write_df(self, df, cur):
        df_with_none = df.where((pd.notnull(df)), None)
        header = list(df_with_none.index.names)
        header.extend(df_with_none.columns)
        header = {e.lower():i for i, e in enumerate(header)}
        for row in df_with_none.itertuples():
            if type(row[0]) == tuple:
                r = list(row[0])
            else:
                r = [row[0]]
            r.extend(row[1:])
            values = {}
            for h, hi in header.items():
                values[h] = r[hi] 
            self.write(values, cur)
        self.flush(cur)
        
class get_procedure(object):
    
   def run_it(self, tag):
       
       conn = OpenConnection()
       cursor = conn.cursor(dictionary=True)
       cursor.execute("""select * from mgparams where tag = %s""",(tag,))
       row = cursor.fetchone()
       params = json.loads(str(row["dependencies"]))
       procs = str(row["script"])
       
       i = 1
       for k in params: 
           params[k] = i*1000
           i += 1
       
       procs_as_function = """def calculate(params): 
           """+procs+"""
           return result"""                        
       exec(procs_as_function)
       retval = locals()["calculate"](params)
       
       return retval
   
def print_error():
    print(sys.exc_info())
    traceback.print_tb(sys.exc_info()[2])