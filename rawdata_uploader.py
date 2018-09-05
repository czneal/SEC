# -*- coding: utf-8 -*-
"""
Created on Thu Oct 05 22:41:01 2017

@author: Asus
"""

import mysql.connector
import sys
import database_operations as do

class ExcludedFields(object):
    def __init__(self, con):
        self.exc_fields = set()
        self.exc_values = {}
        
    def check(self, header, values):
        for f in self.exc_fields:
            if f in header:
                if values[header[f]] not in self.exc_values[f]:
                    return False
        return True
class NumsExcludedFields(ExcludedFields):
    def __init__(self, con):
        super().__init__(con)
        cur = con.cursor(dictionary=True)
        cur.execute("select distinct adsh from subs")
        self.exc_fields.add("adsh")
        self.exc_values["adsh"] = set(r["adsh"] for r in cur)
        
            
def upload_text_file_mysql(filename, table_name, truncate=False):
    f = open(filename)
    header = f.readline()
    header = header.replace("\n", "")
    header = header.split("\t")
    header = dict(zip(header, range(0, len(header))))        
    
    con = do.OpenConnection(host="server")
    batch_size = 10000
    total = 0
    table = do.Table(table_name, con, buffer_size=batch_size)
    exc_fields = None
    if table_name == "nums":
        exc_fields = NumsExcludedFields(con)
    
    try:        
        cur = con.cursor()
        
        if truncate:
            table.truncate(con)
        
        for l in f:
            total += 1
            
            values = l.split("\t")
            values = [v.replace("\n","").replace("\r","") for v in values]
            
            if exc_fields is not None:
                if not exc_fields.check(header, values):
                    continue
                
            table.write(header, values, cur)
            
            if total % batch_size == 0:                
                print("\r" + "Lines done: {0}".format(total), end="")
                con.commit()
                
        table.flush(cur)
        con.commit()
        
        print("\r" + "Lines done: {0}".format(total))
        con.commit()
    except:
        print("something went wrong!")
        print("Unexpected error:", sys.exc_info())
        print("Row:", total)
        con.rollback()
        con.close()
        
def upload_pre_file_mysql(filename):
    tag_file = open(filename)
    header = tag_file.readline()
    header = header.replace("\n", "")
    header = header.split("\t")
    
    columns = {header[i]:i for i in range(0,len(header))}
    
    insert_command = """insert into pre (adsh, tag, version, report, line, stmt, plabel, rfile, inpth)
                        values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        on duplicate key update 
                        report = values(report), line = values(line),
                        plabel = values(plabel),
                        rfile = values(rfile), inpth = values(inpth)"""
    
       
    con = do.OpenConnection(host="95.31.1.243")
    try:
        
        cur = con.cursor()
            
        batch_size = 10000
        batch_index = 0
        total_batch = 0
        batch = []
        for l in tag_file:
            batch_index += 1
            values = l.split("\t")
            values = [v.replace("\n","").replace("\r","") for v in values]
            values = (values[columns["adsh"]], values[columns["tag"]], values[columns["version"]],\
                           values[columns["report"]], values[columns["line"]], values[columns["stmt"]],\
                           values[columns["plabel"]], values[columns["rfile"]], values[columns["inpth"]])
            batch.append(values)
            if batch_index >= batch_size:                
                print("\r" + "Lines done: {0}".format(total_batch*10000), end="")
                total_batch += 1
                cur.executemany(insert_command, batch)
                batch = []
                batch_index = 0
                con.commit()
                
        if batch_index != 0:
            cur.executemany(insert_command, batch)
        print("\r" + "Lines done: {0}".format(total_batch*10000 + batch_index))
        con.commit()
    except mysql.connector.Error as err:
        con.rollback()
        con.close()
        print(err)
    except:
        print("something went wrong!")
        print("Unexpected error:", sys.exc_info()[0])
        con.rollback()
        con.close()

for year in range(2011,2012):
    print("Year:", year)
    for q in range(1,5):
        print("Quarter:", q)
        print("Subs")
        path = "RawData/" + str(year) + "q" + str(q) + "/"
        upload_text_file_mysql(path + "sub.txt", "subs")
        print("Tags")
        upload_text_file_mysql(path + "tag.txt", "tags")
        print("Nums")
        upload_text_file_mysql(path + "num.txt", "nums")
        print("Pre")
        upload_text_file_mysql(path + "pre.txt", "pre")

