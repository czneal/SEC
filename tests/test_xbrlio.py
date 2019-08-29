# -*- coding: utf-8 -*-

import unittest, mock
import datetime
from mysql.connector.errors import InternalError, Error

from exceptions import XbrlException
from mysqlio.basicio import OpenConnection
from mysqlio.xbrlfileio import ReportToDB

class TestInsertUpdate(unittest.TestCase):
    def test_write_company(self):
        with OpenConnection() as con:
            cur = con.cursor(dictionary=True)
            
            with self.subTest(msg='regular insert'):                
                record = {'company_name': 'Test Company1',
                          'cik': 100000000, 
                          'sic': 0,
                          'file_date': datetime.date(2011, 1, 1)}
                ReportToDB.write_company(cur, record)
                
                #check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertEqual(data[0]['company_name'], record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertEqual(data[0]['sic'], record['sic'])
                self.assertEqual(data[0]['updated'], record['file_date'])
                
            with self.subTest(msg='regular update on same updated'):
                record = {'company_name': 'Test Company2',
                          'cik': 100000000, 
                          'sic': 100,
                          'file_date': datetime.date(2011, 1, 1)}
                ReportToDB.write_company(cur, record)
                
                #check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertEqual(data[0]['company_name'], record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertEqual(data[0]['sic'], record['sic'])
                self.assertEqual(data[0]['updated'], record['file_date'])
            
            with self.subTest(msg='regular update on > updated'):
                record = {'company_name': 'Test Company3',
                          'cik': 100000000, 
                          'sic': 200,
                          'file_date': datetime.date(2011, 2, 1)}
                ReportToDB.write_company(cur, record)
                
                #check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertEqual(data[0]['company_name'], record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertEqual(data[0]['sic'], record['sic'])
                self.assertEqual(data[0]['updated'], record['file_date'])
                
            with self.subTest(msg='regular update on > updated and None sic'):
                record = {'company_name': 'Test Company3',
                          'cik': 100000000, 
                          'sic': None,
                          'file_date': datetime.date(2011, 2, 1)}
                ReportToDB.write_company(cur, record)
                
                #check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertEqual(data[0]['company_name'], record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertEqual(data[0]['sic'], 0)
                self.assertEqual(data[0]['updated'], record['file_date'])
                
            with self.subTest(msg='regular update on < updated'):
                record = {'company_name': 'Test Company4',
                          'cik': 100000000, 
                          'sic': 700,
                          'file_date': datetime.date(2010, 2, 1)}
                ReportToDB.write_company(cur, record)
                
                #check
                cur.execute('select * from companies where cik=100000000;')
                data = cur.fetchall()
                self.assertEqual(len(data), 1)
                self.assertNotEqual(data[0]['company_name'], record['company_name'])
                self.assertEqual(data[0]['cik'], record['cik'])
                self.assertNotEqual(data[0]['sic'], record['sic'])
                self.assertNotEqual(data[0]['updated'], record['file_date'])
            
            with self.subTest(msg='raise XbrlException'):
                cur = mock.Mock()
                cur.execute.side_effect = Error(2006)
                record = {'company_name': 'Test Company4',
                          'cik': 100000000, 
                          'sic': 10,
                          'file_date': datetime.date(2010, 2, 1)}
                with self.assertRaises(XbrlException):
                    ReportToDB.write_company(cur, record)
            
            with self.subTest(msg='raise dead lock exception'):
                cur = mock.Mock()
                cur.execute.side_effect = InternalError()
                record = {'company_name': 'Test Company4',
                          'cik': 100000000, 
                          'sic': 10,
                          'file_date': datetime.date(2010, 2, 1)}
                with self.assertRaises(InternalError):
                    ReportToDB.write_company(cur, record)
            


if __name__ == '__main__':
    unittest.main()