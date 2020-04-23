import unittest
import unittest.mock
import datetime
import pandas as pd
import numpy as np

from typing import Dict, Any

import mysqlio.basicio as do
from mysqlio.tests.dbtest import DBTestBase  # type: ignore


class TestConnection(DBTestBase):
    def test_connection(self):
        try:
            con = do.open_connection()
            cur = con.cursor(dictionary=True)
            cur.execute('select * from companies limit 10')
            data = cur.fetchall()
            con.close()
        except Exception:
            self.assertFalse(False, msg='connection cannot be open')

    def test_con_manager(self):
        try:
            with do.OpenConnection() as con:
                cur = con.cursor(dictionary=True)
                cur.execute('select * from companies limit 10')
                data = cur.fetchall()
                raise Exception()
        except Exception:
            self.assertTrue(con.con is None)


class TestMySQLTable(DBTestBase):
    @classmethod
    def setUpClass(TestMySQLTable):
        drop1 = """drop table if exists `simple_table`;"""
        drop2 = """drop table if exists `multikey_table`;"""
        query1 = """
            CREATE TABLE `simple_table` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `column1` varchar(45) DEFAULT NULL,
            `column2` int(11) NOT NULL,
            PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        query2 = """
            CREATE TABLE `multikey_table` (
            `column1` int(11) NOT NULL,
            `column2` varchar(30) NOT NULL,
            `date_col` date DEFAULT NULL,
            `column3` varchar(45) DEFAULT NULL,
            PRIMARY KEY (`column1`,`column2`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """
        with do.OpenConnection() as con:
            cur = con.cursor()
            cur.execute(drop1)
            cur.execute(drop2)
            cur.execute(query1)
            cur.execute(query2)
            con.commit()

    @classmethod
    def tearDownClass(DBTestBase):
        drop1 = """drop table if exists `simple_table`;"""
        drop2 = """drop table if exists `multikey_table`;"""
        with do.OpenConnection() as con:
            cur = con.cursor()
            cur.execute(drop1)
            cur.execute(drop2)
            con.commit()

    def test_simple_insert(self):
        with self.subTest('simple'):
            with do.OpenConnection() as con:
                cur = con.cursor(dictionary=True)

                table = do.MySQLTable(
                    'simple_table', con, use_simple_insert=True)
                table.truncate(cur)
                con.commit()

                row = {'column1': None, 'column2': 100}
                for i in range(100):
                    table.write_row(row, cur)
                con.commit()

                cur.execute('select * from simple_table')
                self.assertEqual(len(cur.fetchall()), 100)

                table.truncate(cur)
                con.commit()

    def test_prepare_row(self):
        with do.OpenConnection() as con:
            table = do.MySQLTable(
                'multikey_table', con, use_simple_insert=False)

        with self.subTest("null column doesn't appear"):
            row = {'column1': 1, 'column2': 'some text'}
            drow = table._prepare_row(row, check=True)

            self.assertEqual(drow,
                             {'column1': 1,
                              'column2': 'some text',
                              'column3': None,
                              'date_col': None})

        with self.subTest("replace nan to None"):
            row = {'column1': 1, 'column2': 'some text', 'column3': np.nan}
            drow = table._prepare_row(row, check=True)
            self.assertEqual(drow,
                             {'column1': 1,
                              'column2': 'some text',
                              'column3': None,
                              'date_col': None})

        with self.subTest("raise if not null field is None"):
            row = {'column1': 1, 'column2': None, 'column3': np.nan}
            self.assertRaises(ValueError, table._prepare_row, row)

        with self.subTest("raise if not null field is nan"):
            row = {'column1': 1, 'column2': np.nan, 'column3': np.nan}
            self.assertRaises(ValueError, table._prepare_row, row)

        with self.subTest("cut text field"):
            row = {'column1': 1,
                   'column2': '--'.join(['' for i in range(40)]),
                   'column3': np.nan}
            drow = table._prepare_row(row)
            self.assertTrue(len(row['column2']) > 30)
            self.assertEqual(len(drow['column2']), 30)

    def test_multikey_insert(self):
        with self.subTest("null column doesn't appear"):
            with do.OpenConnection() as con:
                cur = con.cursor(dictionary=True)

                table = do.MySQLTable(
                    'multikey_table', con, use_simple_insert=False)
                table.truncate(cur)
                con.commit()

                for i in range(100):
                    row = {'column1': i, 'column2': 'some text'}
                    table.write_row(row, cur)
                con.commit()

                cur.execute('select * from multikey_table')
                self.assertEqual(len(cur.fetchall()), 100)

                table.truncate(cur)
                con.commit()

        with self.subTest("insert DataFrame success"):
            with do.OpenConnection() as con:
                cur = con.cursor(dictionary=True)

                table = do.MySQLTable(
                    'multikey_table', con, use_simple_insert=False)
                table.truncate(cur)
                con.commit()

                df = pd.DataFrame([[1, 'aaaaa', 4], [2, 'bbbbb', 4]],
                                  columns=['column1', 'column2', 'buuuu'])
                table.write_df(df, cur)
                con.commit()

                cur.execute('select * from multikey_table')
                self.assertEqual(len(cur.fetchall()), 2)

                table.truncate(cur)
                con.commit()
        
        with self.subTest("insert DataFrame raise"):
            with do.OpenConnection() as con:
                cur = con.cursor(dictionary=True)

                table = do.MySQLTable(
                    'multikey_table', con, use_simple_insert=False)
                table.truncate(cur)
                con.commit()

                df = pd.DataFrame([[1, 'aaaa', 4], [2, np.nan, 4]],
                                  columns=['column1', 'column2', 'buuuu'])
                self.assertRaises(ValueError, table.write_df, df, cur)
                
                table.truncate(cur)
                con.commit()

    def check_result(
            self, cur: do.RptCursor, query: str, keys: Dict[str, Any],
            values: Dict[str, Any]) -> bool:
        cur.execute(query, keys)
        data = cur.fetchall()
        if len(data) != 1:
            return False

        row = data[0]
        for k, v in values.items():
            if k not in row:
                return False
            if row[k] != v:
                return False

        return True

    def test_insert_update(self):
        with do.OpenConnection() as con:
            cur = con.cursor(dictionary=True)

            table = do.MySQLTable('indicators', con, use_simple_insert=False)
            table.truncate(cur)
            con.commit()

            row1 = {
                'adsh': '11111',
                'name': 'mg_roe',
                'value': 1000.0,
                'fy': 2019,
                'cik': 100000000}
            row2 = {
                'adsh': '11111',
                'name': 'mg_roe',
                'value': 2000.0,
                'fy': 2020,
                'cik': 200000000}
            row3 = {
                'adsh': '11111',
                'name': 'mg_roe_average',
                'value': 4000.0,
                'fy': 2019,
                'cik': 300000000}

            row4 = {
                'adsh': '22222',
                'name': 'mg_roe',
                'value': 3000.0,
                'fy': 2018,
                'cik': 300000000}

            query = 'select * from indicators where adsh=%(adsh)s and name=%(name)s'
            with self.subTest(i=1):
                table.write_row(row1, cur)
                self.assertTrue(
                    self.check_result(
                        cur,
                        query,
                        keys={
                            k: v for k,
                            v in row1.items() if k == 'adsh' or k == 'name'},
                        values={
                            k: v for k,
                            v in row1.items() if k != 'adsh' and k != 'name'}))

            with self.subTest(i=2):
                table.write_row(row2, cur)
                self.assertTrue(
                    self.check_result(
                        cur,
                        query,
                        keys={
                            k: v for k,
                            v in row2.items() if k == 'adsh' or k == 'name'},
                        values={
                            k: v for k,
                            v in row2.items() if k != 'adsh' and k != 'name'}))

            with self.subTest(i=3):
                table.write_row(row3, cur)
                self.assertTrue(
                    self.check_result(
                        cur,
                        query,
                        keys={
                            k: v for k,
                            v in row3.items() if k == 'adsh' or k == 'name'},
                        values={
                            k: v for k,
                            v in row3.items() if k != 'adsh' and k != 'name'}))

            with self.subTest(i=4):
                table.write_row(row4, cur)
                self.assertTrue(
                    self.check_result(
                        cur,
                        query,
                        keys={
                            k: v for k,
                            v in row4.items() if k == 'adsh' or k == 'name'},
                        values={
                            k: v for k,
                            v in row4.items() if k != 'adsh' and k != 'name'}),
                    msg='row4 check fails')
                self.assertTrue(
                    self.check_result(
                        cur,
                        query,
                        keys={
                            k: v for k,
                            v in row3.items() if k == 'adsh' or k == 'name'},
                        values={
                            k: v for k,
                            v in row3.items() if k != 'adsh' and k != 'name'}),
                    msg='row3 check fails')
                self.assertTrue(
                    self.check_result(
                        cur,
                        query,
                        keys={
                            k: v for k,
                            v in row2.items() if k == 'adsh' or k == 'name'},
                        values={
                            k: v for k,
                            v in row2.items() if k != 'adsh' and k != 'name'}),
                    msg='row2 check fails')

            con.close()

    def test_insert_update_gte(self):
        with do.OpenConnection() as con:
            cur = con.cursor(dictionary=True)
            table = do.MySQLTable('companies', con)
            table.set_insert_if('updated')

            with self.subTest(msg='regular insert'):
                record: Dict[str, Any] = {'company_name': 'Test Company1',
                                          'cik': 100000000,
                                          'sic': 0,
                                          'updated': datetime.date(2011, 1, 1)}
                table.write_row(record, cur)

                # check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertEqual(
                    data[0]['company_name'],
                    record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertEqual(data[0]['sic'], record['sic'])
                self.assertEqual(data[0]['updated'], record['updated'])

            with self.subTest(msg='regular update on same updated'):
                record = {'company_name': 'Test Company2',
                          'cik': 100000000,
                          'sic': 100,
                          'updated': datetime.date(2011, 1, 1)}
                table.write_row(record, cur)

                # check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertEqual(
                    data[0]['company_name'],
                    record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertEqual(data[0]['sic'], record['sic'])
                self.assertEqual(data[0]['updated'], record['updated'])

            with self.subTest(msg='regular update on > updated'):
                record = {'company_name': 'Test Company3',
                          'cik': 100000000,
                          'sic': 200,
                          'updated': datetime.date(2011, 2, 1)}
                table.write_row(record, cur)

                # check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertEqual(
                    data[0]['company_name'],
                    record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertEqual(data[0]['sic'], record['sic'])
                self.assertEqual(data[0]['updated'], record['updated'])

            with self.subTest(msg='regular update on > updated'):
                record = {'company_name': 'Test Company3',
                          'cik': 100000000,
                          'sic': 0,
                          'updated': datetime.date(2011, 2, 1)}
                table.write_row(record, cur)

                # check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertEqual(
                    data[0]['company_name'],
                    record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertEqual(data[0]['sic'], 0)
                self.assertEqual(data[0]['updated'], record['updated'])

            with self.subTest(msg='regular update on < updated'):
                record = {'company_name': 'Test Company4',
                          'cik': 100000000,
                          'sic': 700,
                          'updated': datetime.date(2010, 2, 1)}
                table.write_row(record, cur)

                # check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertNotEqual(
                    data[0]['company_name'],
                    record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertNotEqual(data[0]['sic'], record['sic'])
                self.assertNotEqual(data[0]['updated'], record['updated'])

            con.rollback()
            con.close()

    def test_create(self):
        with do.OpenConnection() as con:
            fields = [
                do.TableField(
                    'id', int, notnull=True, primary=True),
                do.TableField(
                    'column1', str, size=20, primary=True, notnull=False),
                do.TableField(
                    'column2', str, size=30),
                do.TableField(
                    'date_col', datetime.date, notnull=True)]
            do.create_table(con, 'test_create_table', fields)

            table = do.MySQLTable('test_create_table', con)

            table.write_row(
                {'id': 1, 'column1': 'a', 'column2': None,
                 'date_col': datetime.date.today()},
                con.cursor())

            con.close()


class TestMySQLRetry(DBTestBase):
    def test_pass_raising(self):
        with unittest.mock.patch('mysqlio.basicio.RptCursor') as cur:
            with do.OpenConnection() as con:
                table = do.MySQLTable('companies', con)

                with self.subTest(msg='raise dead lock exception'):
                    cur.execute.side_effect = [do.InternalError(),
                                               do.InternalError(),
                                               do.InternalError(),
                                               do.InternalError(),
                                               True]
                    record = {'company_name': 'Test Company4',
                              'cik': 100000000,
                              'sic': 0,
                              'updated': datetime.date(2010, 2, 1)}
                    do.retry_mysql_write(table.write_row)(record, cur)

                con.close()

    def test_raising(self):
        with unittest.mock.patch('mysqlio.basicio.RptCursor') as cur:
            with do.OpenConnection() as con:
                table = do.MySQLTable('companies', con)

                with self.subTest(msg='raise Error(2006)'):
                    cur.execute.side_effect = do.Error(2006)
                    record = {'company_name': 'Test Company4',
                              'cik': 100000000,
                              'sic': 0,
                              'updated': datetime.date(2010, 2, 1)}
                    with self.assertRaises(do.Error):
                        table.write_row(record, cur)

                with self.subTest(msg='raise dead lock exception'):
                    cur.execute.side_effect = [do.InternalError(),
                                               do.InternalError(),
                                               do.InternalError(),
                                               do.InternalError(),
                                               do.InternalError(),
                                               do.InternalError(),
                                               do.InternalError()]
                    record = {'company_name': 'Test Company4',
                              'cik': 100000000,
                              'sic': 0,
                              'updated': datetime.date(2010, 2, 1)}
                    with self.assertRaises(do.InternalError):
                        do.retry_mysql_write(table.write_row)(record, cur)

                con.close()


if __name__ == '__main__':
    unittest.main()
