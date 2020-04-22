# -*- coding: utf-8 -*-
"""
Created on Thu May 16 17:30:10 2019

@author: Asus
"""

import unittest
import json
import datetime as dt

from bs4 import BeautifulSoup

import xbrlhtml.parse_html as ht
import urltools
from utils import str2date


@unittest.skip('not needed for a while')
class TestParseHtml(unittest.TestCase):
    def test_convert2money(self):
        self.assertEqual(
            ht.convert2money('$  1,123,23.3'),
            112323.3, 'Should be 112323.3')
        self.assertEqual(
            ht.convert2money('$ 17,351'),
            17351, 'Should be 17351')
        self.assertEqual(ht.convert2money(
            '(1,405)	'), -1405, 'Should be -1405')
        self.assertEqual(ht.convert2money(
            '$ (2,131.) '), -2131, 'Should be -2131')

    def test_find_multiplier(self):
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1126294/000112629413000006/R5.htm'),
            'lxml')
        self.assertEqual(
            ht.find_multiplier(bs),
            1000000.0,
            'Should be 1000000.0')

        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1143921/000114392113000013/R2.htm'),
            'lxml')
        self.assertEqual(ht.find_multiplier(bs), 1000.0, 'Should be 1000.0')

        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1325964/000155335014000315/R2.htm'),
            'lxml')
        self.assertEqual(ht.find_multiplier(bs), 1.0, 'Should be 1.0')

    def test_find_dates_pos(self):
        s = {
            1: "2012-12-31",
            2: "2012-12-31",
            3: "2012-12-14",
            4: "2012-12-31",
            5: "2012-12-31",
            6: "2011-12-31",
            7: "2012-12-14",
            8: "2011-12-31",
            9: "2010-12-31",
            10: "2009-12-31",
            11: "2011-12-31",
            12: "2011-12-31",
            13: "2012-12-31",
            14: "2012-12-31",
            15: "2012-12-31",
            16: "2011-12-31",
            17: "2011-12-31",
            18: "2011-12-31",
            19: "2012-12-31",
            20: "2012-12-31",
            21: "2012-12-31",
            22: "2012-12-31",
            23: "2011-12-31",
            24: "2011-12-31",
            25: "2011-12-31",
            26: "2011-12-31"}
        for k, v in s.items():
            s[k] = str2date(v)

        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1126294/000112629413000006/R5.htm'),
            'lxml')
        dates = ht.find_dates_pos(bs)
        self.assertDictEqual(dates, s)

        s = {1: "2013-06-30", 2: "2012-12-31"}
        for k, v in s.items():
            s[k] = str2date(v)
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1325964/000155335014000315/R2.htm'),
            'lxml')
        dates = ht.find_dates_pos(bs)
        self.assertDictEqual(dates, s)

        s = {
            1: "2011-12-31",
            2: "2010-12-31",
            3: "2009-12-31",
            4: "2011-12-31",
            5: "2011-11-27",
            6: "2010-12-31",
            7: "2009-12-31"}
        for k, v in s.items():
            s[k] = str2date(v)
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/27430/000110465912022046/R3.htm'),
            'lxml')
        dates = ht.find_dates_pos(bs)
        self.assertDictEqual(dates, s)

        s = {1: "2018-12-31", 3: "2017-12-31", 5: "2016-12-31"}
        for k, v in s.items():
            s[k] = str2date(v)
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/14930/000001493019000054/R2.htm'),
            'lxml')
        dates = ht.find_dates_pos(bs)
        self.assertDictEqual(dates, s)

        s = {
            2: "2018-12-29",
            3: "2018-09-29",
            4: "2018-06-30",
            5: "2018-03-31",
            6: "2017-12-30",
            7: "2017-09-30",
            8: "2017-07-01",
            9: "2017-04-01",
            10: "2018-12-29",
            12: "2017-12-30",
            14: "2016-12-31"}
        for k, v in s.items():
            s[k] = str2date(v)
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/2488/000000248819000011/R2.htm'),
            'lxml')
        dates = ht.find_dates_pos(bs)
        self.assertDictEqual(dates, s)

        s = {1: "2017-12-31", 2: "2018-12-31", 3: "2017-05-31"}
        for k, v in s.items():
            s[k] = str2date(v)
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1413891/000121390019004884/R6.htm'),
            'lxml')
        dates = ht.find_dates_pos(bs)
        self.assertDictEqual(dates, s)

    def test_find_facts(self):
        s = [
            {"tag": "AssetsCurrent", "pos": 1, "value": 2539000000.0},
            {"tag": "AssetsCurrent", "pos": 2, "value": 1908000000.0},
            {"tag": "AssetsCurrent", "pos": 4, "value": 1519000000.0},
            {"tag": "AssetsCurrent", "pos": 5, "value": 727000000.0},
            {
                "tag": "AssetsCurrent", "pos": 6, "value": 4176000000.0}, {
                "tag": "AssetsCurrent", "pos": 8, "value": 1968000000.0}, {
                "tag": "AssetsCurrent", "pos": 11, "value": 2465000000.0}, {
                "tag": "AssetsCurrent", "pos": 12, "value": 1001000000.0}, {
                "tag": "Assets", "pos": 1, "value": 7506000000.0}, {
                "tag": "Assets", "pos": 2, "value": 3335000000.0}, {
                "tag": "Assets", "pos": 4, "value": 3461000000.0}, {
                "tag": "Assets", "pos": 5, "value": 2403000000.0}, {
                "tag": "Assets", "pos": 6, "value": 12269000000.0}, {
                "tag": "Assets", "pos": 8, "value": 7665000000.0}, {
                "tag": "Assets", "pos": 11, "value": 6589000000.0}, {
                "tag": "Assets", "pos": 12, "value": 4478000000.0}, {
                "tag": "LiabilitiesCurrent", "pos": 1, "value": 1166000000.0}, {
                "tag": "LiabilitiesCurrent", "pos": 2, "value": 72000000.0}, {
                "tag": "LiabilitiesCurrent", "pos": 4, "value": 719000000.0}, {
                "tag": "LiabilitiesCurrent", "pos": 5, "value": 218000000.0}, {
                "tag": "LiabilitiesCurrent", "pos": 6, "value": 1944000000.0}, {
                "tag": "LiabilitiesCurrent", "pos": 8, "value": 73000000.0}, {
                "tag": "LiabilitiesCurrent", "pos": 11, "value": 1454000000.0}, {
                "tag": "LiabilitiesCurrent", "pos": 12, "value": 391000000.0}, {
                "tag": "LiabilitiesNoncurrent", "pos": 1, "value": 5927000000.0}, {
                "tag": "LiabilitiesNoncurrent", "pos": 2, "value": 2850000000.0}, {
                "tag": "LiabilitiesNoncurrent", "pos": 4, "value": 1730000000.0}, {
                "tag": "LiabilitiesNoncurrent", "pos": 5, "value": 659000000.0}, {
                "tag": "LiabilitiesNoncurrent", "pos": 6, "value": 5208000000.0}, {
                "tag": "LiabilitiesNoncurrent", "pos": 8, "value": 2475000000.0}, {
                "tag": "LiabilitiesNoncurrent", "pos": 11, "value": 1170000000.0}, {
                "tag": "LiabilitiesNoncurrent", "pos": 12, "value": 160000000.0}, {
                "tag": "Liabilities", "pos": 1, "value": 7093000000.0}, {
                "tag": "Liabilities", "pos": 2, "value": 2922000000.0}, {
                "tag": "Liabilities", "pos": 4, "value": 2449000000.0}, {
                "tag": "Liabilities", "pos": 5, "value": 877000000.0}, {
                "tag": "Liabilities", "pos": 6, "value": 7152000000.0}, {
                "tag": "Liabilities", "pos": 8, "value": 2548000000.0}, {
                "tag": "Liabilities", "pos": 11, "value": 2624000000.0}, {
                "tag": "Liabilities", "pos": 12, "value": 551000000.0}, {
                "tag": "LiabilitiesAndStockholdersEquity", "pos": 1, "value": 7506000000.0}, {
                "tag": "LiabilitiesAndStockholdersEquity", "pos": 2, "value": 3335000000.0}, {
                "tag": "LiabilitiesAndStockholdersEquity", "pos": 6, "value": 12269000000.0}, {
                "tag": "LiabilitiesAndStockholdersEquity", "pos": 8, "value": 7665000000.0}]

        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1126294/000112629413000006/R5.htm'),
            'lxml')
        facts = ht.find_facts(bs,
                              ['Assets',
                               'Liabilities',
                               'LiabilitiesAndStockholdersEquity',
                               'AssetsCurrent',
                               'AssetsNoncurrent',
                               'LiabilitiesCurrent',
                               'LiabilitiesNoncurrent'])
        s = sorted(s, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        facts = sorted(
            facts, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        self.assertSequenceEqual(facts, s)

        s = [
            {
                "tag": "AssetsCurrent", "pos": 1, "value": 2402908.0}, {
                "tag": "AssetsCurrent", "pos": 2, "value": 3026854.0}, {
                "tag": "Assets", "pos": 1, "value": 3244808.0}, {
                    "tag": "Assets", "pos": 2, "value": 3816374.0}, {
                        "tag": "Liabilities", "pos": 1, "value": 122176.0}, {
                            "tag": "Liabilities", "pos": 2, "value": 155328.0}, {
                                "tag": "LiabilitiesAndStockholdersEquity", "pos": 1, "value": 3244808.0}, {
                                    "tag": "LiabilitiesAndStockholdersEquity", "pos": 2, "value": 3816374.0}]
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1325964/000155335014000315/R2.htm'),
            'lxml')
        facts = ht.find_facts(bs,
                              ['Assets',
                               'Liabilities',
                               'LiabilitiesAndStockholdersEquity',
                               'AssetsCurrent',
                               'AssetsNoncurrent',
                               'LiabilitiesCurrent',
                               'LiabilitiesNoncurrent'])
        s = sorted(s, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        facts = sorted(
            facts, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        self.assertSequenceEqual(facts, s)

        s = [
            {
                "tag": "Assets", "pos": 1, "value": 1085225000.0}, {
                "tag": "Liabilities", "pos": 1, "value": 911284000.0}, {
                "tag": "LiabilitiesAndStockholdersEquity", "pos": 1, "value": 1085225000.0}, {
                    "tag": "Assets", "pos": 2, "value": 1087621000.0}, {
                        "tag": "Liabilities", "pos": 2, "value": 918112000.0}, {
                            "tag": "LiabilitiesAndStockholdersEquity", "pos": 2, "value": 1087621000.0}]
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1143921/000114392113000013/R2.htm'),
            'lxml')
        facts = ht.find_facts(bs,
                              ['Assets',
                               'Liabilities',
                               'LiabilitiesAndStockholdersEquity',
                               'AssetsCurrent',
                               'AssetsNoncurrent',
                               'LiabilitiesCurrent',
                               'LiabilitiesNoncurrent'])
        s = sorted(s, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        facts = sorted(
            facts, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        self.assertSequenceEqual(facts, s)

        """
        Income Statement
        """

        s = [{"tag": "InterestExpense", "pos": 1, "value": -212156.0},
             {"tag": "InterestExpense", "pos": 2, "value": -130922.0},
             {"tag": "InterestExpense", "pos": 3, "value": -545285.0},
             {"tag": "NetIncomeLoss", "pos": 1, "value": -3912326.0},
             {"tag": "NetIncomeLoss", "pos": 2, "value": -4556538.0},
             {"tag": "NetIncomeLoss", "pos": 3, "value": -32328365.0}]
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1325964/000155335014000315/R4.htm'),
            'lxml')
        facts = ht.find_facts(bs, ['Revenues', 'InterestExpense',
                                   'ProfitLoss', 'NetIncomeLoss'])
        s = sorted(s, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        facts = sorted(
            facts, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        self.assertSequenceEqual(facts, s)

        s = [
            {"tag": "Revenues", "pos": 1, "value": 73000000.0},
            {"tag": "Revenues", "pos": 2, "value": 2564000000.0},
            {"tag": "Revenues", "pos": 3, "value": 3614000000.0},
            {"tag": "Revenues", "pos": 4, "value": 2270000000.0},
            {"tag": "Revenues", "pos": 5, "value": 77000000.0},
            {"tag": "Revenues", "pos": 6, "value": 2594000000.0},
            {"tag": "Revenues", "pos": 7, "value": 2938000000.0},
            {"tag": "Revenues", "pos": 8, "value": 2105000000.0},
            {"tag": "Revenues", "pos": 9, "value": 28000000.0},
            {"tag": "Revenues", "pos": 10, "value": 989000000.0},
            {"tag": "Revenues", "pos": 11, "value": 1347000000.0},
            {"tag": "Revenues", "pos": 12, "value": 1704000000.0},
            {"tag": "InterestExpense", "pos": 1, "value": -8000000.0},
            {"tag": "InterestExpense", "pos": 2, "value": -330000000.0},
            {"tag": "InterestExpense", "pos": 3, "value": -379000000.0},
            {"tag": "InterestExpense", "pos": 4, "value": -253000000.0},
            {"tag": "InterestExpense", "pos": 5, "value": -3000000.0},
            {"tag": "InterestExpense", "pos": 6, "value": -75000000.0},
            {"tag": "InterestExpense", "pos": 7, "value": -93000000.0},
            {"tag": "InterestExpense", "pos": 8, "value": -200000000.0},
            {"tag": "InterestExpense", "pos": 13, "value": -3000000.0},
            {"tag": "InterestExpense", "pos": 14, "value": -70000000.0},
            {"tag": "InterestExpense", "pos": 15, "value": -88000000.0},
            {"tag": "InterestExpense", "pos": 16, "value": -200000000.0},
            {"tag": "InterestExpense", "pos": 17, "value": 0.0},
            {"tag": "InterestExpense", "pos": 18, "value": -1000000.0},
            {"tag": "InterestExpense", "pos": 19, "value": -1000000.0},
            {"tag": "InterestExpense", "pos": 20, "value": -3000000.0},
            {"tag": "InterestExpense", "pos": 21, "value": 0.0},
            {"tag": "InterestExpense", "pos": 22, "value": -5000000.0},
            {"tag": "InterestExpense", "pos": 23, "value": -5000000.0},
            {"tag": "InterestExpense", "pos": 24, "value": 0.0},
            {"tag": "InterestExpense", "pos": 25, "value": 0.0},
            {"tag": "InterestExpense", "pos": 26, "value": -3000000.0},
            {"tag": "InterestExpense", "pos": 27, "value": -4000000.0},
            {"tag": "InterestExpense", "pos": 28, "value": 0.0},
            {"tag": "ProfitLoss", "pos": 1, "value": -72000000.0},
            {"tag": "ProfitLoss", "pos": 2, "value": -414000000.0},
            {"tag": "ProfitLoss", "pos": 3, "value": -189000000.0},
            {"tag": "ProfitLoss", "pos": 4, "value": -233000000.0},
            {"tag": "ProfitLoss", "pos": 5, "value": -3000000.0},
            {"tag": "ProfitLoss", "pos": 6, "value": -80000000.0},
            {"tag": "ProfitLoss", "pos": 7, "value": 16000000.0},
            {"tag": "ProfitLoss", "pos": 8, "value": -396000000.0},
            {"tag": "ProfitLoss", "pos": 9, "value": 1000000.0},
            {"tag": "ProfitLoss", "pos": 10, "value": 31000000.0},
            {"tag": "ProfitLoss", "pos": 11, "value": 105000000.0},
            {"tag": "ProfitLoss", "pos": 12, "value": -781000000.0}]
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1126294/000112629413000006/R2.htm'),
            'lxml')
        facts = ht.find_facts(bs, ['Revenues', 'InterestExpense',
                                   'ProfitLoss', 'NetIncomeLoss'])
        s = sorted(s, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        facts = sorted(
            facts, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))

        self.assertSequenceEqual(facts, s)

        s = [{"tag": "InterestExpense", "pos": 2, "value": 6170000.0},
             {"tag": "NetIncomeLoss", "pos": 2, "value": 3793000.0},
             {"tag": "InterestExpense", "pos": 1, "value": 633000.0},
             {"tag": "InterestExpense", "pos": 3, "value": 2778000.0},
             {"tag": "NetIncomeLoss", "pos": 1, "value": 529000.0},
             {"tag": "NetIncomeLoss", "pos": 2, "value": 3793000.0},
             {"tag": "NetIncomeLoss", "pos": 3, "value": 968000.0}]
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/1143921/000114392113000013/R4.htm'),
            'lxml')
        facts = ht.find_facts(bs, ['Revenues', 'InterestExpense',
                                   'ProfitLoss', 'NetIncomeLoss'])
        s = sorted(s, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        facts = sorted(
            facts, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))

        self.assertSequenceEqual(facts, s)

        s = [{"tag": "InterestExpense", "pos": 2, "value": -29000000.0},
             {"tag": "InterestExpense", "pos": 3, "value": -30000000.0},
             {"tag": "InterestExpense", "pos": 4, "value": -31000000.0},
             {"tag": "InterestExpense", "pos": 5, "value": -31000000.0},
             {"tag": "InterestExpense", "pos": 6, "value": -31000000.0},
             {"tag": "InterestExpense", "pos": 7, "value": -31000000.0},
             {"tag": "InterestExpense", "pos": 8, "value": -32000000.0},
             {"tag": "InterestExpense", "pos": 9, "value": -32000000.0},
             {"tag": "InterestExpense", "pos": 10, "value": -121000000.0},
             {"tag": "InterestExpense", "pos": 12, "value": -126000000.0},
             {"tag": "InterestExpense", "pos": 14, "value": -156000000.0},
             {"tag": "NetIncomeLoss", "pos": 2, "value": 38000000.0},
             {"tag": "NetIncomeLoss", "pos": 3, "value": 102000000.0},
             {"tag": "NetIncomeLoss", "pos": 4, "value": 116000000.0},
             {"tag": "NetIncomeLoss", "pos": 5, "value": 81000000.0},
             {"tag": "NetIncomeLoss", "pos": 6, "value": -19000000.0},
             {"tag": "NetIncomeLoss", "pos": 7, "value": 61000000.0},
             {"tag": "NetIncomeLoss", "pos": 8, "value": -42000000.0},
             {"tag": "NetIncomeLoss", "pos": 9, "value": -33000000.0},
             {"tag": "NetIncomeLoss", "pos": 10, "value": 337000000.0},
             {"tag": "NetIncomeLoss", "pos": 12, "value": -33000000.0},
             {"tag": "NetIncomeLoss", "pos": 14, "value": -498000000.0}]
        bs = BeautifulSoup(
            urltools.fetch_urlfile(
                'https://www.sec.gov/Archives/edgar/data/2488/000000248819000011/R2.htm'),
            'lxml')
        facts = ht.find_facts(bs, ['Revenues', 'InterestExpense',
                                   'ProfitLoss', 'NetIncomeLoss'])
        s = sorted(s, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))
        facts = sorted(
            facts, key=lambda x: '{0}-{1}'.format(x['tag'], x['pos']))

        self.assertSequenceEqual(facts, s)

    def test_find_reports(self):
        s = {"bs": "/Archives/edgar/data/27430/000110465912022046/R4.htm",
             "cf": "/Archives/edgar/data/27430/000110465912022046/R3.htm",
             "is": "/Archives/edgar/data/27430/000110465912022046/R2.htm",
             "se": "/Archives/edgar/data/27430/000110465912022046/R6.htm"}
        reps = ht.find_reports(cik=27430, adsh='0001104659-12-022046')
        self.assertDictEqual(reps, s, 'Should be: {0}'.format(s))

        s = {"bs": "/Archives/edgar/data/1143921/000114392113000013/R2.htm",
             "cf": "/Archives/edgar/data/1143921/000114392113000013/R7.htm",
             "is": "/Archives/edgar/data/1143921/000114392113000013/R4.htm",
             "se": "/Archives/edgar/data/1143921/000114392113000013/R6.htm"}
        reps = ht.find_reports(cik=1143921, adsh='0001143921-13-000013')
        self.assertDictEqual(reps, s, 'Should be: {0}'.format(s))

        s = {"bs": "/Archives/edgar/data/1126294/000112629413000006/R5.htm",
             "cf": "/Archives/edgar/data/1126294/000112629413000006/R7.htm",
             "is": "/Archives/edgar/data/1126294/000112629413000006/R2.htm",
             "se": "/Archives/edgar/data/1126294/000112629413000006/R8.htm"}
        reps = ht.find_reports(cik=1126294, adsh='0001126294-13-000006')
        self.assertDictEqual(reps, s, 'Should be: {0}'.format(s))

        s = {"bs": "/Archives/edgar/data/1702780/000162828019002370/R2.htm",
             "cf": "/Archives/edgar/data/1702780/000162828019002370/R8.htm",
             "is": "/Archives/edgar/data/1702780/000162828019002370/R4.htm",
             "se": "/Archives/edgar/data/1702780/000162828019002370/R7.htm"}
        reps = ht.find_reports(cik=1702780, adsh='0001628280-19-002370')
        self.assertEqual(reps, s, 'Should be: {0}'.format(s))

        s = {"bs": "/Archives/edgar/data/1095073/000109507318000008/R2.htm",
             "cf": "/Archives/edgar/data/1095073/000109507318000008/R7.htm",
             "is": "/Archives/edgar/data/1095073/000109507318000008/R4.htm",
             "se": "/Archives/edgar/data/1095073/000109507318000008/R5.htm"}
        reps = ht.find_reports(cik=1095073, adsh='0001095073-18-000008')
        self.assertEqual(reps, s, 'Should be: {0}'.format(s))


if __name__ == '__main__':
    unittest.main()
