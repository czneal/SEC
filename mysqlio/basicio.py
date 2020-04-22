# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 11:14:23 2017

@author: Asus
"""
import datetime
import re
from contextlib import contextmanager
from exceptions import MySQLSyntaxError, MySQLTypeError
from typing import (Any, Dict, List, Optional,
                    Set, Mapping, Union, cast, Iterable,
                    Iterator, Type)

import pandas as pd  # type: ignore
from settings import Settings
from utils import retry

#  'mysqldbw' backend:
# from mysqlio.mysqldbw import (open_connection,
#                               RptConnection,
#                               RptCursor,
#                               InternalError,
#                               Error,
#                               ProgrammingError,
#                               activate_test_mode)
# 'mysqlconw' backend:
from mysqlio.mysqlconw import (open_connection,
                               RptConnection,
                               RptCursor,
                               InternalError,
                               Error,
                               ProgrammingError,
                               activate_test_mode,
                               deactivate_test_mode)


WHITE_LIST_TABLES = frozenset(
    ['companies', 'nasdaq',
     'stocks_daily', 'stocks_shares', 'stocks_dividents',
     'stocks_index',
     'owners',
     'mgnums', 'logs_parse',
     'siccodes', 'sec_forms', 'reports',
     'sec_xbrl_forms', 'sec_shares', 'sec_shares_ticker',
     'indicators', 'ind_proc_info', 'ind_rest_info', 'ind_classified_pairs'])

WHITE_LIST_TABLES_TEST = frozenset([
    'simple_table', 'test_create_table'
])


@contextmanager
def OpenConnection(
        host: str = Settings.host(),
        port: int = 3306) -> Iterator[RptConnection]:
    con = open_connection(host, port)
    try:
        yield con
    finally:
        con.close()


retry_mysql_write = retry(5, InternalError)


class Table(object):
    def __init__(self, name, con, buffer_size=1000):
        self.table_name = name
        self.fields = set()
        self.not_null_fields = set()
        self.primary_keys = set()

        db_name = con.database

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

        # df_with_none.rename('`{}`'.format, axis='columns', inplace=True)

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


class MySQLTable(object):
    def __init__(self,
                 table_name: str,
                 con: RptConnection,
                 use_simple_insert: bool = False):
        if (table_name not in WHITE_LIST_TABLES and
                table_name not in WHITE_LIST_TABLES_TEST):
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
        cur.execute(
            "show columns from " +
            table_name +
            " from " +
            con.database)
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
                self.field_sizes[field_name] = [
                    groups[0][0], int(groups[0][1])]
                if self.field_sizes[field_name][0] == 'varchar':
                    self.fields_to_cut[field_name] = cast(
                        int, self.field_sizes[field_name][1])

        cur.execute(
            "show index from " +
            self.name +
            " from " + con.database +
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

    def set_insert_if(self, if_field: str) -> None:
        self.insert_command = update_command(
            table_name=self.name,
            fields=self.fields,
            fields_not_null=self.fields_not_null,
            primary_keys=self.primary_keys,
            if_field=if_field)

    def write(self, obj: Any, cur: RptCursor) -> None:
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

    def write_df(
            self,
            df: pd.DataFrame,
            cur: RptCursor) -> None:
        if not self.fields.issubset(df.columns):
            raise MySQLTypeError(f'DataFrame should contain all table fields')

        row_list = df[list(self.fields)].to_dict('record')
        self.write_row_list(row_list, cur)

    def write_row(
            self, row: Dict[str, Any],
            cur: RptCursor) -> None:

        data = self._prepare_row(row)
        cur.execute(self.insert_command, data)

    def write_row_list(
            self, row_list: List[Dict[str, Any]],
            cur: RptCursor) -> None:

        for i in range(int(len(row_list) / self.chunk_size) + 1):
            new_row_list = [self._prepare_row(row)
                            for row in row_list[i * self.chunk_size:
                                                (i + 1) * self.chunk_size]]
            cur.executemany(self.insert_command, new_row_list)

    def truncate(self, cur: RptCursor) -> None:
        cur.execute(f'truncate table `{self.name}`;')

    def update_row(
            self,
            row: Dict[str, Any],
            key_fields: Iterable[str],
            update_fields: Iterable[str],
            cur: RptCursor) -> None:
        data = self._prepare_row(row, check=False)
        cur.execute(simple_update_command(
            self.name,
            key_fields=key_fields,
            update_fields=update_fields),
            data)

    def _prepare_row(self, row: Dict[str, Any],
                     check: bool = True) -> Dict[str, Any]:
        if check and not self.fields_not_null.issubset(row.keys()):
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


FieldType = Union[Type[str], Type[int], Type[float], Type[datetime.date]]


class TableField(object):
    def __init__(self,
                 name: str,
                 ftype: FieldType,
                 notnull: bool = False,
                 primary: bool = False,
                 size: int = 0):
        self.name = name
        self.ftype = ftype
        self.notnull = notnull
        self.primary = primary
        self.size = size


def create_table(con: RptConnection,
                 name: str,
                 fields: List[TableField]):
    """
    create table with name = 'name'
    fields is a list TableField structures
    """

    commands: List[str] = []
    commands.append(f'drop table if exists `{name}`;\n')

    create = f"create table `{name}` (\n"

    columns = []
    primary = []

    for field in fields:
        column = '`{fname}` {ftype}{fsize} {not_null}'
        if issubclass(field.ftype, str):
            ftype = 'varchar'
            fsize = '({})'.format(field.size)
        elif issubclass(field.ftype, int):
            ftype = 'int'
            fsize = '(11)'
        elif issubclass(field.ftype, float):
            ftype = 'decimal'
            fsize = '(24,4)'
        elif issubclass(field.ftype, datetime.date):
            ftype = 'date'
            fsize = ''
        else:
            raise ValueError(f'field type: {field.ftype} doesnt supported')

        if field.notnull:
            not_null = 'not null'
        else:
            not_null = ''

        if field.primary:
            primary.append('`{}`'.format(field.name))
            not_null = 'not null'

        columns.append(column.format(fname=field.name,
                                     ftype=ftype,
                                     fsize=fsize,
                                     not_null=not_null))

    create += ',\n'.join(columns)
    create += ',\nprimary key ({})\n'.format(', '.join(primary))
    create += ') ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;'

    commands.append(create)

    cur = con.cursor()

    for cmd in commands:
        cur.execute(cmd)


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
        raise MySQLTypeError(
            f"if_field '{if_field}'' should be in fields_not_null set")

    update = """insert into {0}\n({1})\nvalues({2})\non duplicate key update\n{3}"""
    columns = ', '.join('' + f + '' for f in fields)
    values = ', '.join(['%(' + f + ')s' for f in fields])
    on_dupl = ',\n'.join(
        [f'{field} = if(values({if_field}) >= {if_field}, values({field}), {field})'
         for field in fields.difference(primary_keys)])

    update = update.format(table_name, columns, values, on_dupl)

    return update


def simple_update_command(table_name: str,
                          key_fields: Iterable[str],
                          update_fields: Iterable[str]) -> str:
    command = f'update `{table_name}`\n'
    update = ',\n  '.join([f'`{field}` = %({field})s'
                           for field in update_fields])
    where = '\n  and '.join(
        [f'`{field}` = %({field})s' for field in key_fields])

    return (command + 'set ' + update + '\nwhere ' + where)


if __name__ == '__main__':
    update = simple_update_command(
        table_name='companies',
        key_fields=['cik', 'sic'],
        update_fields={'company_name', 'updated'})
    print(update)

    # insert = insert_command(table_name='companies',
    #                 fields=set(['company_name', 'cik', 'sic', 'updated']),
    #                 fields_not_null=set(['company_name', 'cik', 'sic', 'updated']),
    #                 primary_keys=set(['cik']))
    # print(update)
    # print(insert)

    # with OpenConnection() as con:
    #     t = MySQLTable('logs_parse', con)
    #     cur = con.cursor(dictionary=True)

    #     # id, created, state, module, levelname, msg, extra
    #     row = {'created': '2019-12-02 18:47:23.000235',
    #            'state': 'mpc.worker.Process-100000000000000000000000000',
    #            'module': 'mpc',
    #            'levelname': 'DEBUG',
    #            'msg': 'configure worker' + ''.join(['-' for _ in range(0,100)]),
    #            'extra': ''}
    #     t.write_row(row, cur)
    #     con.commit()
