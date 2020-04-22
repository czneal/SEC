# -*- coding: utf-8 -*-

import unittest
from itertools import product
from typing import Tuple, Dict

from algos.calc import Validator, calc_fact, calc_chapter
from algos.calc import calc_from_dim

import datetime as dt
from utils import add_app_dir
from tests.helper import read_report
from xbrlxml.xbrlfile import XbrlFile

from algos.tests.resource_chapters import make_chapters


class TestCalc(unittest.TestCase):
    def open_xbrlfile(self) -> Tuple[XbrlFile, Dict[str, Dict[str, str]]]:

        adsh = "0000092122-19-000006"
        dm, record, r = read_report(adsh)
        self.assertTrue(r)

        contexts = {
            'bs': {
                "roleuri": "http://southerncompany.com/role/ConsolidatedBalanceSheets",
                "context": "FI2018Q4"},
            'is': {
                "roleuri": "http://southerncompany.com/role/ConsolidatedStatementsOfIncome",
                "context": "FD2018Q4YTD"},
            'cf': {
                "roleuri": "http://southerncompany.com/role/ConsolidatedStatementsOfCashFlows",
                "context": "FD2018Q4YTD"}}
        return dm.xbrlfile, contexts

    def test_validator(self):
        answers = [True, True, True, True, False, True, False, False]
        for index, (value, value_sum, none_sum_err) in enumerate(
            product([None, 100],
                    [None, 200],
                    [True, False])):
            with self.subTest(value=value,
                              value_sum=value_sum,
                              none_sum_err=none_sum_err):
                err = Validator(0.02, none_sum_err,
                                none_val_err=False)
                v = err.check(value, value_sum, 'tag')
                self.assertEqual(answers[index], v)

        with self.subTest(value=100, value_sum=101):
            self.assertTrue(err.check(100, 101, 'tag'))
        with self.subTest(value=0, value_sum=0):
            self.assertTrue(err.check(100, 101, 'tag'))
        with self.subTest(value=0, value_sum=0):
            self.assertFalse(err.check(100, 103, 'tag'))

    def test_calc_one_node(self):
        err = Validator(0.02, none_sum_err=True,
                        none_val_err=False)
        chapters = make_chapters()
        n0 = chapters['bs'].nodes['us-gaap:NodeName0']
        n4 = chapters['bs'].nodes['us-gaap:NodeName4']
        facts = {'us-gaap:NodeName1': 10,
                 'us-gaap:NodeName2': 20,
                 'us-gaap:NodeName3': 30,
                 'us-gaap:NodeName4': 40}
        with self.subTest(err='None'):
            self.assertEqual(calc_fact(node=n4, facts=facts, err=None),
                             facts['us-gaap:NodeName4'])
        with self.subTest(node='leaf node'):
            self.assertEqual(calc_fact(node=n4, facts=facts, err=err),
                             facts['us-gaap:NodeName4'])
        with self.subTest(node='us-gaap:NodeName0'):
            self.assertEqual(calc_fact(node=n0, facts=facts, err=err,
                                       repair=True),
                             -10)
            self.assertTrue('us-gaap:NodeName0' in facts)
            self.assertEqual(facts['us-gaap:NodeName0'], -10)
        with self.subTest(facts='{}'):
            self.assertTrue(calc_fact(node=n0,
                                      facts={},
                                      err=err) is None)
        with self.subTest(facts='children is None'):
            self.assertTrue(calc_fact(node=n0,
                                      facts={'us-gaap:NodeName0': 10},
                                      err=err) == 10)

    def test_calc_structure(self):
        chapter = make_chapters()['bs']
        facts = {'us-gaap:NodeName1': 10,
                 'us-gaap:NodeName2': 20,
                 'us-gaap:NodeName3': 30,
                 'us-gaap:NodeName4': 40}
        values = calc_chapter(chapter, facts, err=None, repair=True)
        self.assertDictEqual(values, {'us-gaap:NodeName0': -10})

    def test_calc_from_dim(self):
        xbrlfile, contexts = self.open_xbrlfile()

        with self.subTest(test='us-gaap:LeveragedLeasesBalanceSheetInvestmentInLeveragedLeasesNet'):
            pres = xbrlfile.pres[contexts['bs']['roleuri']]
            v = calc_from_dim(
                name='us-gaap:LeveragedLeasesBalanceSheetInvestmentInLeveragedLeasesNet',
                context=contexts['bs']['context'],
                pres=pres,
                contexts=xbrlfile.contexts,
                dfacts=xbrlfile.dfacts)
            self.assertEqual(v.shape[0], 0)
            self.assertSetEqual(
                set(v.columns),
                {'name', 'tag', 'version', 'sdate', 'edate', 'uom', 'value'},
            )

        with self.subTest(test='us-gaap:CostOfGoodsAndServicesSold'):
            pres = xbrlfile.pres[contexts['is']['roleuri']]
            v = calc_from_dim(name='us-gaap:CostsAndExpenses',
                              context=contexts['is']['context'],
                              pres=pres,
                              contexts=xbrlfile.contexts,
                              dfacts=xbrlfile.dfacts)
            self.assertEqual(v.shape[0], 1)
            self.assertEqual(v.iloc[0]['value'], 17715000000)
            self.assertSetEqual(
                set(v.columns),
                {'name', 'tag', 'version', 'sdate', 'edate', 'uom', 'value'})

        with self.subTest(test='us-gaap:Liabilities'):
            pres = xbrlfile.pres[contexts['bs']['roleuri']]
            v = calc_from_dim(name='us-gaap:Liabilities',
                              context=contexts['bs']['context'],
                              pres=pres,
                              contexts=xbrlfile.contexts,
                              dfacts=xbrlfile.dfacts)
            self.assertEqual(v.shape[0], 1)
            self.assertEqual(v.iloc[0]['value'], 68758000000.0)
            self.assertSetEqual(
                set(v.columns),
                {'name', 'tag', 'version', 'sdate', 'edate', 'uom', 'value'})


if __name__ == '__main__':
    unittest.main()
