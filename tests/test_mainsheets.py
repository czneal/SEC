# -*- coding: utf-8 -*-
"""
Created on Tue May 21 17:23:31 2019

@author: Asus
"""
import unittest
import classifiers.mainsheets as cl
import json

class TestParseHtml(unittest.TestCase):
    def test_select_ms(self):
        ms = cl.MainSheets()
        
        labels = ['CONSOLIDATED STATEMENT OF NET ASSETS',
                  'CONSOLIDATED BALANCE SHEET',
                  'CONSOLIDATED BALANCE SHEET (Parenthetical)',
                  'CONSOLIDATED STATEMENT OF CHANGES IN NET ASSETS',
                  'CONSOLIDATED STATEMENTS OF OPERATIONS',
                  'CONSOLIDATED STATEMENTS OF EQUITY (DEFICIT)',
                  'CONSOLIDATED STATEMENTS OF CASH FLOWS']
        s = """{"CONSOLIDATED BALANCE SHEET": "bs", "CONSOLIDATED STATEMENTS OF CASH FLOWS": "cf", "CONSOLIDATED STATEMENTS OF OPERATIONS": "is"}"""
        self.assertEqual(json.dumps(ms.select_ms(labels)), 
                         s, 
                         'Should be {0}'.format(s))
        
        labels = ['Consolidated Balance Sheets',
                  'Consolidated Balance Sheets Parentheticals',
                  'Consolidated Statements of Operations',
                  'Consolidated Statements of Stockholders Equity',
                  'Consolidated Statements of Cash Flows',
                  'Consolidated Statements of Cash Flows - Balances per Consolidated Balance Sheets']
        s = """{"Consolidated Balance Sheets": "bs", "Consolidated Statements of Cash Flows": "cf", "Consolidated Statements of Operations": "is"}"""
        self.assertEqual(json.dumps(ms.select_ms(labels)), 
                         s, 
                         'Should be {0}'.format(s))
        
        labels = ['CONSOLIDATED BALANCE SHEETS',
                  'CONSOLIDATED STATEMENTS OF INCOME/(LOSS) AND COMPREHENSIVE INCOME/(LOSS)',
                  'CONSOLIDATED STATEMENTS OF INCOME/(LOSS) AND COMPREHENSIVE INCOME/(LOSS) (Calc 2)',
                  'CONSOLIDATED STATEMENTS OF CASH FLOWS']
        s = json.loads("""{"CONSOLIDATED BALANCE SHEETS": "bs", "CONSOLIDATED STATEMENTS OF CASH FLOWS": "cf", "CONSOLIDATED STATEMENTS OF INCOME/(LOSS) AND COMPREHENSIVE INCOME/(LOSS)": "is"}""")
        self.assertDictEqual(ms.select_ms(labels), 
                         s, 
                         'Should be {0}'.format(s))
        
        s = """{"Consolidated Statements of Assets and Liabilities": "bs", "Consolidated Statements of Cash Flows": "cf", "Consolidated Statements of Operations": "is"}"""
        labels = ['Consolidated Statements of Assets and Liabilities',
                  'Consolidated Statements of Changes in Net Assets',
                  'Consolidated Statements of Cash Flows',
                  'Consolidated Statements of Operations']    
        self.assertEqual(json.dumps(ms.select_ms(labels)), 
                         s, 
                         'Should be {0}'.format(s))
        
        s = """{"Consolidated Balance Sheets": "bs", "Consolidated Statements Of Cash Flows": "cf", "Consolidated Statements Of Operations And Comprehensive Income (Loss)": "is"}"""
        labels = ['Consolidated Balance Sheets',
                  'Consolidated Balance Sheets (Parenthetical)',
                  'Consolidated Statements Of Operations And Comprehensive Income (Loss)',
                  'Consolidated Statements Of Changes In Shareholders'' Equity',
                  'Consolidated Statements Of Changes In Shareholders'' Equity (Parenthetical)',
                  'Consolidated Statements Of Cash Flows']
        
        self.assertEqual(json.dumps(ms.select_ms(labels)), 
                         s, 
                         'Should be {0}'.format(s))
        
        s = """{"CONSOLIDATED BALANCE SHEETS": "bs", "CONSOLIDATED STATEMENTS OF CASH FLOWS": "cf", "CONSOLIDATED STATEMENTS OF OPERATIONS AND COMPREHENSIVE INCOME (LOSS)": "is"}"""
        labels = ['CONSOLIDATED BALANCE SHEETS',
                  'CONSOLIDATED BALANCE SHEETS (Parenthetical)',
                  'CONSOLIDATED STATEMENTS OF OPERATIONS AND COMPREHENSIVE INCOME (LOSS)',
                  'CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME (LOSS)',
                  'CONSOLIDATED STATEMENTS OF CASH FLOWS',
                  'CONSOLIDATED STATEMENTS OF STOCKHOLDERS’ EQUITY']
        
        self.assertEqual(json.dumps(ms.select_ms(labels)), 
                         s, 
                         'Should be {0}'.format(s))    
        
        s = """{"Consolidated Balance Sheets": "bs", "Consolidated Statements of Cash Flows": "cf", "Consolidated Statements of Operations": "is"}"""
        labels = ['Consolidated Statements of Operations',
                  'Consolidated Statements of Operations Consolidated Statements of Operations (Parenthetical)',
                  'Consolidated Statements of Comprehensive Income Statement',
                  'Consolidated Balance Sheets',
                  'Consolidated Balance Sheets (Parenthetical)',
                  'Consolidated Statements of Shareholders'' Equity',
                  'Consolidated Statements of Shareholders'' Equity Consolidated Statements of Shareholders'' Equity (Parenthetical)',
                  'Consolidated Statements of Cash Flows',
                  "Consolidated Statements of Cash Flows Consolidated Statement of Cash Flows, Supplemental Information"]
        self.assertEqual(json.dumps(ms.select_ms(labels)), 
                         s, 
                         'Should be {0}'.format(s))
        
        s = """{}"""
        labels = ["Consolidated  Balance  Sheets",
                "Consolidated  Balance  Sheets (Parenthetical)",
                "Consolidated Statements of Operations and Comprehensive Income",
                "Consolidated Statements of Retained Earnings",
                "Consolidated Statements of Stockholders Equity",
                "Consolidated Statements of Stockholders Equity (Parenthetical)",
                "Consolidated Statements of Cash Flows"]
        msl = ms.select_ms(labels)
        self.assertEqual(len(msl), 3)
        self.assertEqual(msl[labels[0]], 'bs')
        self.assertEqual(msl[labels[2]], 'is')
        self.assertEqual(msl[labels[-1]], 'cf')
        
        labels = ["CONSOLIDATED BALANCE SHEET (Unaudited)",
                    "CONSOLIDATED BALANCE SHEET (Parenthetical)",
                    "CONSOLIDATED STATEMENTS OF INCOME (Unaudited)",
                   "CONSOLIDATED STATEMENTS OF COMPRENENSIVE INCOME (LOSS) (Unaudited)",
                    "CONSOLIDATED STATEMENTS OF CHANGES IN SHAREHOLDERS' EQUITY",
                    "CONSOLIDATED STATEMENTS OF CASH FLOWS (Unaudited)"
                    ]
        msl = ms.select_ms(labels)
        self.assertEqual(len(msl), 3)
        self.assertEqual(msl[labels[0]], 'bs')
        self.assertEqual(msl[labels[2]], 'is')
        self.assertEqual(msl[labels[5]], 'cf')
        
        labels = ["Consolidated Statements Of Income And Comprehensive Income",
                    "Consolidated Balance Sheets",
                    "Consolidated Statements Of Cash Flows",
                    "Consolidated Balance Sheets (Parenthetical)",
                    "Consolidated Statements Of Changes In Shareholders' Equity",
                    "Consolidated Statements of Comprehensive Income",
                    "Consolidated Statements of Comprehensive Income (Parenthetical)",
                    "Consolidated Statements Of Changes In Shareholders' Equity (Parentheticals)",
                    "Severance, Lease Terminations And Transition Costs (Narrative) (Details)",
                    "Subsequent Events (Narrative)(Details)",
                    "Subsequent Events"
                ]
        msl = ms.select_ms(labels,[100, 100, 100, 10, 10, 10, 10, 10, 10, 10, 10])
        
        self.assertEqual(len(msl), 3)
        self.assertEqual(msl[labels[1]], 'bs')
        self.assertEqual(msl[labels[0]], 'is')
        self.assertEqual(msl[labels[2]], 'cf')
        
        labels = ['CONSOLIDATED BALANCE SHEETS (Parenthetical)',
                'CONSOLIDATED BALANCE SHEETS (LP cube)',
                'CONSOLIDATED STATEMENTS OF CASH FLOWS (LP cube)',
                'CONSOLIDATED STATEMENTS OF OPERATIONS (LP cube)',
                'CONSOLIDATED STATEMENTS OF COMPREHENSIVE LOSS (LP cube)',
                'CONSOLIDATED STATEMENTS OF CASH FLOWS',
                'CONSOLIDATED BALANCE SHEETS',
                'CONSOLIDATED STATEMENTS OF OPERATIONS',
                'CONSOLIDATED STATEMENTS OF COMPREHENSIVE LOSS'
                ]
        priority = [10, 36, 68, 56, 15, 63, 35, 48, 10]
        msl = ms.select_ms(labels, priority)
        answer = {'CONSOLIDATED BALANCE SHEETS': 'bs', 'CONSOLIDATED STATEMENTS OF CASH FLOWS': 'cf', 'CONSOLIDATED STATEMENTS OF OPERATIONS': 'is'}
        self.assertDictEqual(msl, answer)
        
if __name__ == '__main__':
    unittest.main()
#    labels = ['Consolidated Balance Sheets',
#                  'Consolidated Balance Sheets (Parenthetical)',
#                  'Consolidated Statements Of Operations And Comprehensive Income (Loss)',
#                  'Consolidated Statements Of Changes In Shareholders'' Equity',
#                  'Consolidated Statements Of Changes In Shareholders'' Equity (Parenthetical)',
#                  'Consolidated Statements Of Cash Flows']
#    ms = cl.MainSheets()
#    s = json.dumps(ms.select_ms(labels))
    
    