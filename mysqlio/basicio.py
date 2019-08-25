# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 11:14:23 2017

@author: Asus
"""

import datetime
import mysql.connector # type :ignore
import pandas as pd # type: ignore
from contextlib import contextmanager
from typing import List, Optional

from settings import Settings
from exceptions import MySQLTypeError, MySQLSyntaxError
import queries as q

@contextmanager
def OpenConnection(host: str=Settings.host(), port: int=3306):
    hosts = {"server":"192.168.88.113", 
             "remote":"95.31.1.243",
             "localhost":"localhost"}
    if host == 'remote':
        port = 3456
    con = mysql.connector.connect(user="app", password="Burkina!7faso",
                              host=hosts[host], database="reports",
                              port=port,
                              ssl_ca = Settings.ssl_dir()+"ca.pem",
                              ssl_cert = Settings.ssl_dir()+"client-cert.pem",
                              ssl_key = Settings.ssl_dir()+"client-key.pem",
                              connection_timeout = 30)
    try:
        yield con
    finally:
        con.close()

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
    
    @staticmethod    
    def create(con: mysql.connector.connection,
               name: str,
               fields: list) -> None:
        
        cmd = 'drop table if exists `{}`;\n'.format(name)            
        cmd += "create table `{0}` (\n".format(name)
        columns = []
        primary = []
        try:
            for field in fields:
                column = '`{fname}` {ftype}{fsize} {not_null}'
                if issubclass(field['type'], str):
                    ftype = 'varchar'
                    fsize = '({})'.format(field['size'])
                elif issubclass(field['type'], int):
                    ftype = 'int'
                    fsize = '(11)'
                elif issubclass(field['type'], float):
                    ftype = 'decimal'
                    fsize = '(24,4)'
                elif issubclass(field['type'], datetime.date):
                    ftype = 'date'
                    fsize = ''
                else:
                    raise MySQLTypeError('Field type doesnt supported')
                
                if field['notnull']:
                    not_null = 'not null'
                else:
                    not_null = ''
                columns.append(column.format(fname=field['name'],
                                             ftype=ftype, 
                                             fsize=fsize, 
                                             not_null=not_null))
                
                if field['primary']:
                    primary.append('`{}`'.format(field['name']))            
        except KeyError as e:
            raise MySQLSyntaxError('Fields list is not acceptable, ' + str(e))
        
        cmd += ',\n'.join(columns)
        cmd += ',\nprimary key ({})\n'.format(', '.join(primary))
        cmd += ') ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;'
        
        cur = con.cursor()
        for result in cur.execute(cmd, multi=True):
            pass
        con.commit()

    def __insert_command(self, fields=None):
        if fields is None:
            fields = self.fields
        else:
            fields = self.fields.intersection(fields)
        insert = """insert into {0} ({1}) values({2}) on duplicate key update {3}"""
        columns = ','.join(''+f+'' for f in fields)
        values = ','.join(['%('+f+')s' for f in fields])
        on_dupl = ','.join([''+f+'=values('+f+')' for f in fields.difference(self.primary_keys)])

        insert = insert.format(self.table_name, columns, values, on_dupl)

        return insert

    def write(self, values, cur) -> bool:
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
                    return False

        self.data.extend(values)
        if len(self.data) >= self.buffer_size:
            self.flush(cur)
            
        return True

    def flush(self, cur):
        if len(self.data)>0:
            cur.executemany(self.insert_command, self.data)
            self.data.clear()

    def write_df(self, df, cur):
        if df is None or df.shape[0] == 0:
            return False

        df_with_none = df.where((pd.notnull(df)), None)
        df_with_none = df_with_none.reset_index()


        header = list(df_with_none.columns)
        for field in self.not_null_fields.union(self.primary_keys):
            if field not in header:
                return False

        #df_with_none.rename('`{}`'.format, axis='columns', inplace=True)

        if df.shape[0] <= self.buffer_size:
            cur.executemany(self.__insert_command(header), 
                            df_with_none.to_dict('records'))
        else:
            for i in range(0, int(df.shape[0]/self.buffer_size) + 1):
                cmd = self.__insert_command(header)
                bf = (df_with_none.iloc[i*self.buffer_size:(i+1)*self.buffer_size]
                                            .to_dict('records'))
                cur.executemany(cmd, bf)
        return True

class ReportWriter(object):
    def __init__(self, con):
        self.cntx_tbl = Table('raw_contexts', con)
        self.nums_tbl = Table('raw_nums', con)
        self.reps_tbl = Table('raw_reps', con)
            
    def write_raw_contexts(self, r, cur):
        df = r.cntx_df
        df['cik'] = r.rss['cik']
        df['adsh'] = r.rss['adsh']
        self.cntx_tbl.write_df(df, cur)
    
    def write_raw_facts(self, r, cur):
        df = r.facts_df
        df['cik'] = r.rss['cik']
        df['adsh'] = r.rss['adsh']
        self.nums_tbl.write_df(df, cur)
    
    def write_raw_report(self, r, cur):
        data = {'adsh':r.rss['adsh'],
                'cik':r.rss['cik'],
                'file_date':r.rss['file_date'],
                'file_link':r.file_link,
                'period_rss': r.rss['period'],
                'fy_rss': r.rss['fy'],
                'fye_rss': r.rss['fye'],
                'period_x': r.ddate,
                'fy_x': r.fy,
                'fye_x': r.fye,
                'structure': r.structure_dumps(),
                'form_type':r.rss['form_type'],
                'taxonomy':r.rss['us-gaap'],
                'period_dei':r.dei_edate}
        
        if 'edate' in r.true_dates:
            data['period'] = r.true_dates['edate']
        else:
            data['period'] = None
        if 'fy' in r.true_dates:
            data['fy'] = r.true_dates['fy']
        else:
            data['fy'] = None
        if 'fye' in r.true_dates:
            data['fye'] = r.true_dates['fye']
        else:
            data['fye'] = None
        
        self.reps_tbl.write(data, cur)
        self.reps_tbl.flush(cur)
    
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

def read_report_structures(adshs: List[str]) -> pd.DataFrame:
    """
    read reports table
    return pandas.DataFrame object with columns [adsh, structure, fy]
    if adshs list is empty return empty DataFrame
    """
    with OpenConnection() as con:         
        cur = con.cursor(dictionary=True)
        cur.execute(q.create_tmp_adsh_table)
        cur.executemany("insert into tmp_adshs (adsh) values (%s)", list((e,) for e in adshs))
        cur.execute("""select r.adsh as adsh, structure, r.fin_year as fy
                        from reports r, tmp_adshs a
                        where r.adsh = a.adsh""")
        df = pd.DataFrame(cur.fetchall(), columns=['adsh', 'structure', 'fy'])
        df.set_index("adsh", inplace=True)
    
    return df

def read_reports_nums(adshs: List[str]) -> pd.DataFrame:
    """
    read mgnums table
    return pd.DataFrame with columns [tag, version, value, fy, adsh, uom]
    for all report with adsh firld in adshs list
    if adshs list is empty return empty DataFrame
    """
    
    with OpenConnection() as con:        
        cur = con.cursor(dictionary=True)
        cur.execute(q.create_tmp_adsh_table)
        cur.executemany(q.insert_tmp_adsh, [(adsh,) for adsh in adshs])
        cur.execute("""select tag, version, value, fy, n.adsh, uom 
                       from mgnums n, tmp_adshs t 
                       where n.adsh = t.adsh""")
        df = (pd.DataFrame(cur.fetchall(), 
                          columns=['tag', 'version', 'value', 
                                   'fy', 'adsh', 'uom'])
                .astype({'value': float}))
        
        
    return df

def read_reports_by_cik(ciks: List[int],
                        columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    return pd.DataFrame with specified columns from reports table
    for all cik in ciks list
    if columns is None return all columns
    """
    if columns is None:
        c_names = 'r.*'
    else:
        c_names = ', '.join(['r.'+c for c in columns])
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute(q.create_tmp_cik_table)        
        cur.executemany(q.insert_tmp_cik,[(cik,) for cik in ciks])
        cur.execute("""select {c_names} from reports r, tmp_ciks t 
                       where r.cik=t.cik;""".format(c_names=c_names))
        return pd.DataFrame(cur.fetchall())
    
def getquery(query, dictionary=True):
    try:
        con = OpenConnection()
        cur = con.cursor(dictionary=dictionary)
        cur.execute(query)
        data = cur.fetchall()    
    except:
        raise
    finally:
        if 'con' in locals() and con: con.close()
                
    return data

def execquery(query):
    try:
        con = OpenConnection()
        cur = con.cursor()
        cur.execute(query)
        con.commit()        
    except:
        raise
    finally:
        if 'con' in locals() and con: con.close()
        
if __name__ == '__main__':
    df = read_report_structures(adshs=[''])
    
    