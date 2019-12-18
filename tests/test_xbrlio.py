# -*- coding: utf-8 -*-

import unittest
import unittest.mock as mock
import datetime
from mysql.connector.errors import InternalError, Error

from exceptions import XbrlException
from mysqlio.basicio import OpenConnection, MySQLTable
from mysqlio.xbrlfileio import ReportToDB


class TestInsertUpdate(unittest.TestCase):
    def test_write_company(self):
        writer = ReportToDB()
        cur = writer.cur

        with self.subTest(msg='regular insert'):
            record: Dict[str, Any] = {'company_name': 'Test Company1',
                                      'cik': 100000000,
                                      'sic': 0,
                                      'file_date': datetime.date(2011, 1, 1)}
            writer.write_company(record)

            # check
            cur.execute('select * from companies where cik=100000000;')
            data = cur.fetchall()
            self.assertEqual(len(data), 1)
            self.assertEqual(
                data[0]['company_name'],
                record['company_name'])
            self.assertEqual(data[0]['cik'], record['cik'])
            self.assertEqual(data[0]['sic'], record['sic'])
            self.assertEqual(data[0]['updated'], record['file_date'])

        with self.subTest(msg='regular update on same updated'):
            record = {'company_name': 'Test Company2',
                      'cik': 100000000,
                      'sic': 100,
                      'file_date': datetime.date(2011, 1, 1)}
            writer.write_company(record)

            # check
            cur.execute('select * from companies where cik=100000000;')
            data = cur.fetchall()
            self.assertEqual(len(data), 1)
            self.assertEqual(
                data[0]['company_name'],
                record['company_name'])
            self.assertEqual(data[0]['cik'], record['cik'])
            self.assertEqual(data[0]['sic'], record['sic'])
            self.assertEqual(data[0]['updated'], record['file_date'])

        with self.subTest(msg='regular update on > updated'):
            record = {'company_name': 'Test Company3',
                      'cik': 100000000,
                      'sic': 200,
                      'file_date': datetime.date(2011, 2, 1)}
            writer.write_company(record)

            # check
            cur.execute('select * from companies where cik=100000000;')
            data = cur.fetchall()
            self.assertEqual(len(data), 1)
            self.assertEqual(
                data[0]['company_name'],
                record['company_name'])
            self.assertEqual(data[0]['cik'], record['cik'])
            self.assertEqual(data[0]['sic'], record['sic'])
            self.assertEqual(data[0]['updated'], record['file_date'])

        with self.subTest(msg='regular update on > updated and None sic'):
            record = {'company_name': 'Test Company3',
                      'cik': 100000000,
                      'sic': None,
                      'file_date': datetime.date(2011, 2, 1)}
            writer.write_company(record)

            # check
            cur.execute('select * from companies where cik=100000000;')
            data = cur.fetchall()
            self.assertEqual(len(data), 1)
            self.assertEqual(
                data[0]['company_name'],
                record['company_name'])
            self.assertEqual(data[0]['cik'], record['cik'])
            self.assertEqual(data[0]['sic'], 0)
            self.assertEqual(data[0]['updated'], record['file_date'])

        with self.subTest(msg='regular update on < updated'):
            record = {'company_name': 'Test Company4',
                      'cik': 100000000,
                      'sic': 700,
                      'file_date': datetime.date(2010, 2, 1)}
            writer.write_company(record)

            # check
            cur.execute('select * from companies where cik=100000000;')
            data = cur.fetchall()
            self.assertEqual(len(data), 1)
            self.assertNotEqual(
                data[0]['company_name'],
                record['company_name'])
            self.assertEqual(data[0]['cik'], record['cik'])
            self.assertNotEqual(data[0]['sic'], record['sic'])
            self.assertNotEqual(data[0]['updated'], record['file_date'])

        writer.flush(commit=False)

    def test_write_company_raising(self):
        writer = ReportToDB()

        with mock.patch('mysql.connector.cursor.MySQLCursor.execute') as execute:
            with self.subTest(msg='raise XbrlException'):
                execute.side_effect = Error(2006)
                record = {'company_name': 'Test Company4',
                        'cik': 100000000,
                        'sic': 10,
                        'file_date': datetime.date(2010, 2, 1)}
                with self.assertRaises(XbrlException):
                    writer.write_company(record)

            with self.subTest(msg='raise dead lock exception'):
                execute.side_effect = [InternalError(),
                                        InternalError(),
                                        InternalError(),
                                        InternalError(),
                                        InternalError()]
                record = {'company_name': 'Test Company4',
                        'cik': 100000000,
                        'sic': 10,
                        'file_date': datetime.date(2010, 2, 1)}
                with self.assertRaises(XbrlException):
                    writer.write_company(record)
        writer.flush(commit=False)

if __name__ == '__main__':
    unittest.main()
