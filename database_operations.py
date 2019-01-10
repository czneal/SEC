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

        for row in values:
            for f in self.fields:
                if f not in row:
                    row[f] = None
                elif row[f] == '':
                    row[f] = None

            for f in self.not_null_fields:
                if row[f] is None:
                    return

        self.data.extend(values)
        if len(self.data) >= self.buffer_size:
            self.flush(cur)

    def flush(self, cur):
        if len(self.data)>0:
            cur.executemany(self.insert_command, self.data)
            self.data.clear()

    def write_df(self, df, cur):
        df_with_none = df.where((pd.notnull(df)), None)
        df_with_none = df_with_none.reset_index()

        header = list(df_with_none.columns)
        for field in self.fields:
            if field not in header:
                return

        cur.executemany(self.insert_command, df_with_none.to_dict('records'))

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

def read_reports_attr(years):
    s = "("
    for y in years:
        s += "{0},".format(y)
    s = s[:-1] + ")"

    try:
        con = OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute("""select adsh, trusted,
                    	case structure
                    		when '{}' then 0
                            else 1
                        end as exist, company_name, c.cik
                    from reports r, companies c
                    where fin_year in """ + s + """
                        and c.cik = r.cik""" + Settings.select_limit())

        reports = pd.DataFrame(cur.fetchall())
        reports.set_index("adsh", inplace=True)
    finally:
        con.close()

    return reports

def read_report_structures(adshs):
    try:
        con = OpenConnection()
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
        con = OpenConnection("localhost")
        cur = con.cursor(dictionary=True)
        cur.execute("select concat(version,':',tag) as tag, value from mgnums where adsh = (%s)",(adsh,))
        df = pd.DataFrame(cur.fetchall(), columns=["tag","value"]).set_index("tag")
        df["value"] = df["value"].astype('float')
    finally:
        con.close()
    return df.sort_index()