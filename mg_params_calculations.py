# -*- coding: utf-8 -*-
"""
Created on Fri Dec 29 19:10:44 2017

@author: Asus
"""

import database_operations as do
import json
import mysql.connector
import os

class CalcProcedure(object):
    def __init__(self, script):
        self.script = "def calculate(params):\n"
        
        for l in script.split("\n"):
            self.script += "    " + l + "\n"
            
        self.script += "    return result"
        
    def run_it(self, params):
        exec(self.script)
        retval = locals()["calculate"](params)
        return retval

class CustomParams(object):
    def calculate(self, tag, params):
        return self.procs[tag][0].run_it(params)
    
    def iterator(self):
        for tag in self.procs:
            yield tag  

class StaticParams(CustomParams):
    
    def __init__(self, con, custom_dependencies):
        self.dependencies = []
        self.procs = {}#{tag:[proc, params as string]}
        
        deps = set()
        cur = con.cursor(dictionary=True)
        if custom_dependencies:
            cur.execute("select * from mgparams where class = 'static' and dependencies like '%mg_%'")
        else:
            cur.execute("select * from mgparams where class = 'static' and dependencies not like '%mg_%'")
            
        for r in cur:
            self.procs[r["tag"]] = [CalcProcedure(r["script"]), r["dependencies"]]
            dep = json.loads(r["dependencies"])
            for d in dep:
                deps.add(d)
        self.dependencies = [e for e in deps]
            
class DynamicParams(CustomParams):
    def __init__(self, con):
        self.dependencies = []
        self.procs = {}#{tag:[proc, params as string]}
        
        deps = set()
        cur = con.cursor(dictionary=True)
        cur.execute("select * from mgparams where class = 'dynamic'")
            
        for r in cur:
            self.procs[r["tag"]] = [CalcProcedure(r["script"]), r["dependencies"]]
            dep = json.loads(r["dependencies"])
            for d in dep:
                deps.add(d)
        self.dependencies = [e for e in deps]
        

class StaticParamsGetter(object):        
    def iterator(self, cur, dependencies):
        r = cur.fetchone()
        if r is None:
            return None, None       
        adsh = r["adsh"]
        values = {k:None for k in dependencies}
        
        while r is not None:
            if adsh == r["adsh"]:
                values[r["tag"]] = r["value"]
                r = cur.fetchone()
            else:
                yield adsh, values                
                values = {k:None for k in dependencies}
                adsh = r["adsh"]          
        
        cur.close()
class DynamicParamsGetter(object):
    def iterator(self, cur, tags):
        r = cur.fetchone()
        if r is None:
            return None, None
        
        adsh = r["adsh"]
        values = {k:None for k in tags}
        
        while r is not None:
            if adsh == r["adsh"]:
                values[r["tag"]] = [r["v0"], r["v1"]]
                r = cur.fetchone()
            else:
                yield adsh, values
                values = {k:None for k in tags}
                adsh = r["adsh"]
                
        cur.close()
        
class ParamsPreparator:
    def __init__(self, values, param_string):
        params = json.loads(param_string)
        for p in params:
            params[p] = values[p]
        self.params = params
        
class ParamsUpdater(object):
    def __init__(self):
        self.data = []
        
    def update(self, adsh, tag, fy, value, uom):
        if value is not None:
            self.data.append((adsh, tag, fy, value, uom))
                    
    def flush(self, con):
        if len(self.data) == 0:
            return
        cur = con.cursor()
        insert_cmd = """insert into mgnums (adsh, tag, fy, value, uom) values(%s,%s,%s,%s,%s)
            on duplicate key update value = values(value), uom = values(uom)"""
        cur.executemany(insert_cmd, self.data)
        con.commit()
        
class ReportTable(object):
    def __init__(self, name = "mgreporttable"):
        self.name = name
        
    def create(self, con, tags):        
        columns = ""
        mg_columns = ""
        for tag in tags:
            if tag.startswith("mg_"):
                mg_columns += "`"+tag+ "` decimal(24,4) DEFAULT NULL,"+os.linesep
            else:
                columns += "`"+tag+ "` decimal(24,4) DEFAULT NULL,"+os.linesep
            
        drop = "drop table if exists `" +self.name + "`;"
        
        create = """CREATE TABLE `""" + self.name + """` (
                      `adsh` varchar(20) NOT NULL,
                      `fy` int(11) NOT NULL,""" +\
                      mg_columns + \
                      columns + \
                      """PRIMARY KEY (`adsh`),
                      KEY `""" + self.name + """_fy` (`fy`),
                      CONSTRAINT `""" + self.name +"""_adsh` FOREIGN KEY (`adsh`) REFERENCES `reports` (`adsh`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
       
        cur = con.cursor()        
        cur.execute(drop)
        cur.execute(create)        
        con.commit()        
        
    def insert(self, adsh, fy, values, con):
        values["adsh"] = adsh
        values["fy"] = fy
        columns = " ("
        what = " values("
        for k in values:
            columns += k + ","
            what += "%(" + k + ")s,"
        columns = columns[:-1] + ")"
        what = what[:-1] + ")"
        cmd = "insert into " + self.name + columns + what
        
        cur = con.cursor()
        cur.execute(cmd, values)
        
class StaticQuery(object):
    def prepare(self, cur, dependencies):
        with open("Queries\param_selection_static.sql") as f:
            self.sql_script = f.read()
            
        cur.execute("create temporary table req_tags (tag VARCHAR(256) CHARACTER SET utf8 not null)")
        cur.execute("create temporary table req_mg_tags (tag VARCHAR(256) CHARACTER SET utf8 not null)")
        tags = []
        mg_tags = []
        for p in dependencies:
            if p.startswith("mg_"):
                mg_tags.append((p,))
            else:
                tags.append((p,))
        cur.executemany("insert into req_tags (tag) values (%s)", tags)
        cur.executemany("insert into req_mg_tags (tag) values (%s)", mg_tags)
        
    def execute(self, cur, where):
        cur.execute(self.sql_script, where)
        
        
def fill_mgreporttable(year, create_new):
    con = do.OpenConnection()
    write_con = do.OpenConnection()
    
    static_dep = StaticParams(con, True)
    static_no_dep = StaticParams(con, False)
    dynamic_dep = DynamicParams(con)
    columns = set(static_dep.dependencies + static_no_dep.dependencies + dynamic_dep.dependencies)
    columns.update(static_dep.iterator())
    columns.update(static_no_dep.iterator())
    columns.update(dynamic_dep.iterator())
    additional = {"NoninterestIncome", "NoninterestExpense", "Assets", "AssetsCurrent",\
                  "OtherAssetsNoncurrent", "WeightedAverageNumberOfSharesOutstandingBasic", "WeightedAverageNumberOfDilutedSharesOutstanding",\
                  "Liabilities", "LiabilitiesCurrent", "EffectiveIncomeTaxRateContinuingOperations"}
    columns.update(additional)
    columns = list(columns)
    
    table = ReportTable()
    
    query = StaticQuery()
    try:
        if create_new:
            table.create(write_con, columns)
            
        print("prepare query...", end = "")
        cur = con.cursor(dictionary=True)
        query.prepare(cur, columns)
        print("ok")
        
        print("executing select statements...", end="")
        query.execute(cur, {"fy":year})
        print("ok")
        
        print("process with result table...", end="")
        spg = StaticParamsGetter()
        for adsh, values in spg.iterator(cur, columns):
            table.insert(adsh, year, values, write_con)
            write_con.commit()
        
        print("ok")
        
    except mysql.connector.Error as e:
        print(e)    
    
    
def calculate_static_mg_params(year, custom_dependencies):
    
    con = do.OpenConnection()
    
    sp = StaticParams(con, custom_dependencies)
    updater = ParamsUpdater()
    query = StaticQuery()
    
    try:
        print("prepare tags table...", end="")
        cur = con.cursor(dictionary=True)
        query.prepare(cur, sp.dependencies)    
        print("ok")
        
        print("executing select statements...", end="")
        query.execute(cur, {"fy":year})
        print("ok")
        
        print("calculate mg parameteres...", end="")
        spg = StaticParamsGetter()
        for adsh, values in spg.iterator(cur, sp.dependencies):
            for tag in sp.iterator():
                pp = ParamsPreparator(values, sp.procs[tag][1])
                value = sp.calculate(tag, pp.params)
                updater.update(adsh, tag, year, value, "USD")
        print("ok")
        print("start update mg parameteres...", end="")
        updater.flush(con)
        print("ok")
        
    except mysql.connector.Error as e:
        print("Error: ", e)
    finally:
        con.close()

def calculate_dynamic_mg_params(year):    
    con = do.OpenConnection()
    
    dynamic = DynamicParams(con)
    updater = ParamsUpdater()
    
    with open("Queries\param_selection_dynamic.sql") as f:
        sql_script = f.read()
        
    try:
        print("prepare tags table...", end="")
        cur = con.cursor(dictionary=True)
        cur.execute("create temporary table req_tags1 (tag VARCHAR(256) CHARACTER SET utf8 not null)")
        cur.execute("create temporary table req_tags0 (tag VARCHAR(256) CHARACTER SET utf8 not null)")
        
        tags = list( (p,) for p in dynamic.dependencies )
        cur.executemany("insert into req_tags1 (tag) values (%s)", tags )
        cur.executemany("insert into req_tags0 (tag) values (%s)", tags )
        print("ok")
        
        print("executing select statements...", end="")
        cur.execute(sql_script, {"fy1":year, "fy0":(year-1)})
        print("ok")
        
        print("calculate dynamic mg parameteres...", end="")
        dpg = DynamicParamsGetter()
        for adsh, values in dpg.iterator(cur, dynamic.dependencies):
            for tag in dynamic.iterator():
                preparator = ParamsPreparator(values, dynamic.procs[tag][1])
                value = dynamic.calculate(tag, preparator.params)
                updater.update(adsh, tag, year, value, "USD")
        print("ok")
        print("start update mg parameteres...", end="")
        updater.flush(con)
        print("ok")
        
    except mysql.connector.Error as e:
        print("Error: ", e)
    finally:
        con.close()
        
def recalc_reporttable():
    for year in range(2010, 2017):
        print(str(year))
        print("calculate static parameteres with no custom dependenicies...")
        calculate_static_mg_params(year, False)    
        print("calculate static parameteres with custom dependenicies...")
        calculate_static_mg_params(year, True)
        print("calculate dynamic parameteres...")
        calculate_dynamic_mg_params(year)
    
    delete = True
    for year in range(2011,2017):    
        print("create mgreporttable...")
        print(year)
        if delete:
            fill_mgreporttable(year, delete)
            delete = False
        else:
            fill_mgreporttable(year, delete)
        








