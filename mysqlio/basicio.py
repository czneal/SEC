# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 11:14:23 2017

@author: Asus
"""
import datetime
import re
from contextlib import contextmanager
from exceptions import MySQLSyntaxError, MySQLTypeError
from typing import Any, Dict, List, Optional, Set, Mapping, Union, cast

import mysql.connector  # type :ignore
import pandas as pd  # type: ignore

import queries as q
from settings import Settings

WHITE_LIST_TABLES = frozenset(
    ['companies', 'nasdaq', 
     'stocks_daily', 'stocks_shares', 'stocks_dividents',
     'mgnums', 'logs_parse',
     'siccodes', 'sec_forms', 'reports',
     'sec_xbrl_forms'])


@contextmanager
def OpenConnection(host: str = Settings.host(), port: int = 3306):
    con = open_connection(host, port)
    try:
        yield con
    finally:
        con.close()


def open_connection(host: str = Settings.host(),
                    port: int = 3306) -> mysql.connector.MySQLConnection:
    hosts = {"server": "192.168.88.113",
             "remote": "95.31.1.243",
             "localhost": "localhost",
             "mgserver": "192.168.188.149"}
    if host == 'remote':
        port = 3456
    con = mysql.connector.connect(
        user="app",
        password="Burkina!7faso",
        host=hosts[host],
        database="reports",
        port=port,
        ssl_ca=Settings.ssl_dir() +
        "ca.pem",
        ssl_cert=Settings.ssl_dir() +
        "client-cert.pem",
        ssl_key=Settings.ssl_dir() +
        "client-key.pem",
        connection_timeout=100)
    return con


def tryout(times, exc_cls, what, *args, **kwargs):
    for i in range(times):
        try:
            return what(*args, **kwargs)
        except exc_cls as e:
            if i + 1 == times:
                print('done {} tryouts'.format(times))
                raise e


class Table(object):
    def __init__(self, name, con, db_name="reports", buffer_size=1000):
        self.table_name = name
        self.fields = set()
        self.not_null_fields = set()
        self.primary_keys = set()

        cur = con.cursor(dictionary=True)
        cur.execute(
            "show columns from " +
            self.table_name +
            " from " +
            db_name)
        for r in cur.fetchall():
            if r["Extra"] == "auto_increment":
                continue
            self.fields.add(r["Field"].lower())
            if r["Null"] == "NO":
                self.not_null_fields.add(r["Field"].lower())
            if r["Key"] == "UNI":
                self.primary_keys.add(r["Field"].lower())
        cur.execute(
            "SHOW INDEX FROM " +
            self.table_name +
            " FROM " +
            db_name +
            " where non_unique = 0 and column_name <> 'id'")
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
        columns = ','.join('' + f + '' for f in fields)
        values = ','.join(['%(' + f + ')s' for f in fields])
        on_dupl = ','.join(
            ['' + f + '=values(' + f + ')'
             for f in fields.difference(self.primary_keys)])

        insert = insert.format(self.table_name, columns, values, on_dupl)

        return insert

    def write(self, values, cur) -> bool:
        if isinstance(values, type(dict())):
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
        if len(self.data) > 0:
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
            for i in range(0, int(df.shape[0] / self.buffer_size) + 1):
                cmd = self.__insert_command(header)
                bf = (df_with_none.iloc[i *
                                        self.buffer_size:(i +
                                                          1) *
                                        self.buffer_size] .to_dict('records'))
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
        data = {'adsh': r.rss['adsh'],
                'cik': r.rss['cik'],
                'file_date': r.rss['file_date'],
                'file_link': r.file_link,
                'period_rss': r.rss['period'],
                'fy_rss': r.rss['fy'],
                'fye_rss': r.rss['fye'],
                'period_x': r.ddate,
                'fy_x': r.fy,
                'fye_x': r.fye,
                'structure': r.structure_dumps(),
                'form_type': r.rss['form_type'],
                'taxonomy': r.rss['us-gaap'],
                'period_dei': r.dei_edate}

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
        cur.executemany(
            "insert into tmp_adshs (adsh) values (%s)", list(
                (e,) for e in adshs))
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
        c_names = ', '.join(['r.' + c for c in columns])
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute(q.create_tmp_cik_table)
        cur.executemany(q.insert_tmp_cik, [(cik,) for cik in ciks])
        cur.execute("""select {c_names} from reports r, tmp_ciks t
                       where r.cik=t.cik;""".format(c_names=c_names))
        return pd.DataFrame(cur.fetchall())


class MySQLTable(object):
    def __init__(self, 
                table_name: str, 
                con: mysql.connector.MySQLConnection,
                use_simple_insert: bool=False):
        if table_name not in WHITE_LIST_TABLES:
            raise Exception(
                'table name {0} is not white listed'.format(table_name))

        self.chunk_size = 4098
        self.name = table_name
        self.fields: Set[str] = set()
        self.fields_not_null: Set[str] = set()
        self.primary_keys: Set[str] = set()
        self.field_sizes: Mapping[str, List[Union[str, int]]] = {}
        self.fields_to_cut: Mapping[str, int] = {}

        types = re.compile(r'(\w+)\((\d+)\)', re.IGNORECASE)
        cur = con.cursor(dictionary=True)
        cur.execute("show columns from " + table_name + " from reports")
        for r in cur.fetchall():
            field_name = r["Field"].lower()
            if r["Extra"] == "auto_increment":
                continue
            self.fields.add(field_name)
            if r["Null"] == "NO":
                self.fields_not_null.add(field_name)
            if r["Key"] == "UNI":
                self.primary_keys.add(field_name)

            groups = types.findall(r['Type'])
            if groups:
                self.field_sizes[field_name] = [groups[0][0], int(groups[0][1])]
                if self.field_sizes[field_name][0] == 'varchar':
                    self.fields_to_cut[field_name] = cast(int, self.field_sizes[field_name][1])

        cur.execute(
            "show index from " +
            self.name +
            " from reports " +
            " where non_unique = 0 and column_name <> 'id'")
        for r in cur.fetchall():
            self.primary_keys.add(r["Column_name"].lower())

        if use_simple_insert:
            self.insert_command = simple_insert_command(
                table_name=self.name,
                fields=self.fields,
                fields_not_null=self.fields_not_null,
                primary_keys=self.primary_keys)
        else:    
            self.insert_command = insert_command(
                table_name=self.name,
                fields=self.fields,
                fields_not_null=self.fields_not_null,
                primary_keys=self.primary_keys)

    def set_insert_if(self, if_field:str) -> None:
        self.insert_command = update_command(
            table_name=self.name,
            fields=self.fields,
            fields_not_null=self.fields_not_null,
            primary_keys=self.primary_keys,
            if_field=if_field)

    def write(self, obj: Any, cur: mysql.connector.cursor.MySQLCursor) -> None:
        if isinstance(obj, pd.DataFrame):
            self.write_df(obj, cur)
        elif isinstance(obj, dict):
            self.write_row(obj, cur)
        elif isinstance(obj, list):
            self.write_row_list(obj, cur)
        else:
            raise MySQLTypeError(
                'unsupported type to write into MySQL table {}'.format(
                    type(obj)))
    
    def _prepare_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not self.fields_not_null.issubset(row.keys()):
            diff = self.fields_not_null.difference(row.keys())
            raise MySQLTypeError(f'fields {diff} have not null flag')
        
        data: Dict[str, Any] = {}
        for k, v in row.items():
            if k not in self.fields:
                continue
            if v is None or pd.isna(v):
                data[k] = None
                continue

            if k in self.fields_to_cut:
                data[k] = v[0: self.fields_to_cut[k]]
            else:
                data[k] = v
        return data

    def write_df(
            self,
            df: pd.DataFrame,
            cur: mysql.connector.cursor.MySQLCursor) -> None:
        if not self.fields.issubset(df.columns):
            raise MySQLTypeError(f'DataFrame should contain all table fields')
        
        row_list = df[list(self.fields)].to_dict('record')
        self.write_row_list(row_list, cur)

    def write_row(
            self, row: Dict[str, Any],
            cur: mysql.connector.cursor.MySQLCursor) -> None:
        
        data = self._prepare_row(row)
        cur.execute(self.insert_command, data)

    def write_row_list(
            self, row_list: List[Dict[str, Any]],
            cur: mysql.connector.cursor.MySQLCursor) -> None:
                
        for i in range(int(len(row_list)/self.chunk_size) + 1):            
            new_row_list = [self._prepare_row(row) 
                            for row in row_list[i*self.chunk_size: 
                                                (i + 1)*self.chunk_size]]
            cur.executemany(self.insert_command, new_row_list)

    def truncate(self, cur) -> None:
        cur.execute(f'truncate table `{self.name}`;')

def simple_insert_command(table_name: str,
                          fields: Set[str],
                          fields_not_null: Set[str],
                          primary_keys: Set[str]) -> str:
    insert = """insert into {0}\n({1})\nvalues({2})"""
    columns = ', '.join('' + f + '' for f in fields)
    values = ', '.join(['%(' + f + ')s' for f in fields])
    
    insert = insert.format(table_name, columns, values)

    return insert

def insert_command(table_name: str,
                   fields: Set[str],
                   fields_not_null: Set[str],
                   primary_keys: Set[str]) -> str:
    insert = """insert into {0}\n({1})\nvalues({2})\non duplicate key update\n{3}"""
    columns = ', '.join('' + f + '' for f in fields)
    values = ', '.join(['%(' + f + ')s' for f in fields])
    on_dupl = ',\n'.join(
        [f'{field}=values({field})'
         for field in fields.difference(primary_keys)])

    insert = insert.format(table_name, columns, values, on_dupl)

    return insert

def update_command(table_name: str,
                   fields: Set[str],
                   fields_not_null: Set[str],
                   primary_keys: Set[str],
                   if_field: str) -> str:
    if if_field not in fields_not_null:
        raise MySQLTypeError(f"if_field '{if_field}'' should be in fields_not_null set")

    update = """insert into {0}\n({1})\nvalues({2})\non duplicate key update\n{3}"""
    columns = ', '.join('' + f + '' for f in fields)
    values = ', '.join(['%(' + f + ')s' for f in fields])
    on_dupl = ',\n'.join(
        [f'{field} = if(values({if_field}) >= {if_field}, values({field}), {field})'
         for field in fields.difference(primary_keys)])

    update = update.format(table_name, columns, values, on_dupl)

    return update


if __name__ == '__main__':
    # update = update_command(table_name='companies',
    #                 fields=set(['company_name', 'cik', 'sic', 'updated']),
    #                 fields_not_null=set(['company_name', 'cik', 'sic', 'updated']),
    #                 primary_keys=set(['cik']),
    #                 date_field='updated')
    
    # insert = insert_command(table_name='companies',
    #                 fields=set(['company_name', 'cik', 'sic', 'updated']),
    #                 fields_not_null=set(['company_name', 'cik', 'sic', 'updated']),
    #                 primary_keys=set(['cik']))
    #print(update)
    #print(insert)

    with OpenConnection() as con:
        t = MySQLTable('logs_parse', con)
        cur = con.cursor(dictionary=True)

        # id, created, state, module, levelname, msg, extra
        row = {'created': '2019-12-02 18:47:23.000235', 
               'state': 'mpc.worker.Process-100000000000000000000000000', 
               'module': 'mpc', 
               'levelname': 'DEBUG', 
               'msg': 'configure worker' + ''.join(['-' for _ in range(0,100)]), 
               'extra': ''}
        t.write_row(row, cur)
        con.commit()

