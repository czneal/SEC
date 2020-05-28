# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 12:39:50 2019

@author: Asus
"""
import datetime as dt
import os
import unittest
from typing import Set

import lxml

from utils import str2date
from xbrlxml.xbrlfileparser import (Context, Fact, Unit, XbrlCleanUp,
                                    XbrlParser, default_decimals, to_decimals,
                                    to_float, xbrltrans)


"""
Не парсит TextBlock z:/sec/2015/02/0001308606-0001308606-15-000029.zip
solved - XMLParser(recover=True)

в dei есть элемент с пустым текстом z:/sec/2015/02/0000074208-0000074208-15-000013.zip
solved - if block in parse_dei

в footnotes есть элемент с пустым текстом z:/sec/2015/02/0001163165-0001193125-15-059281.zip
solved - if block in parser_footnote

"""


def make_absolute(path: str) -> str:
    dir_name = os.path.dirname(__file__)
    return os.path.join(dir_name, path)


class TestFact(unittest.TestCase):
    def test_name(self):
        f = Fact()
        f.version = 'us-gaap'
        f.tag = 'Assets'
        self.assertEqual(f.name(), 'us-gaap:Assets')

    def test_key(self):
        f = Fact()
        f.version = 'us-gaap'
        f.tag = 'Assets'
        f.context = 'somecontext'
        self.assertSequenceEqual(f.key(), ('us-gaap:Assets', 'somecontext'))

    def test_asdict(self):
        f = Fact()
        f.version = 'us-gaap'
        f.tag = 'Assets'
        f.context = 'somecontext'
        f.decimals = 10
        f.value = 1000000000.00
        f.unitid = 'usd'
        f.factid = 'Fact01'
        j = {'name': 'us-gaap:Assets',
             'decimals': 10,
             'unitid': 'usd',
             'value': 1000000000.00,
             'context': 'somecontext',
             'factid': 'Fact01',
             'tag': 'Assets',
             'version': 'us-gaap'}
        self.assertDictEqual(f.asdict(), j)

    def simple_fact(self) -> Fact:
        f = Fact()
        f.version = 'us-gaap'
        f.tag = 'Assets'
        f.context = 'somecontext'
        f.decimals = 10
        f.value = 1000000000.00
        f.unitid = 'usd'
        f.factid = 'Fact01'
        return f

    def test_update(self):
        with self.subTest('do update'):
            f = self.simple_fact()
            fu = self.simple_fact()
            fu.decimals = 8
            fu.value = 2000000.0
            f.update(fu)
            self.assertTrue(f.value == 2000000.0 and
                            f.decimals == 8)

        with self.subTest(i='do not update'):
            f = self.simple_fact()
            fu = self.simple_fact()
            f.update(fu)
            self.assertTrue(f.value == 1000000000.0 and
                            f.decimals == 10)

        with self.subTest(i='null value'):
            f = self.simple_fact()
            fu = self.simple_fact()
            fu.value = None
            f.update(fu)
            self.assertTrue(f.value == 1000000000.0 and
                            f.decimals == 10)

    def test_to_float(self):
        with self.subTest(i='normal'):
            self.assertEqual(1000.21, to_float('1000.21 '))

        with self.subTest(i='None'):
            self.assertEqual(None, to_float(None))

        with self.subTest(i='empty string'):
            self.assertEqual(None, to_float(''))

        with self.subTest(i='string'):
            self.assertEqual(None, to_float('asdad'))

    def test_to_decimals(self):
        with self.subTest(i='normal'):
            self.assertEqual(10, to_decimals('10'))

        with self.subTest(i='normal'):
            self.assertEqual(10, to_decimals('-10'))

        with self.subTest(i='value is None'):
            self.assertEqual(default_decimals, to_decimals(None))

        with self.subTest(i='value not numeric'):
            self.assertEqual(default_decimals, to_decimals('abrakadabra')
                             )

    def test_equal(self):
        with self.subTest(i='equal'):
            f1 = self.simple_fact()
            f2 = self.simple_fact()
            self.assertTrue(f1 == f2)
        with self.subTest(i='not equal in name'):
            f1 = self.simple_fact()
            f2 = self.simple_fact()
            f2.tag = 'Liabilities'
            self.assertTrue(f1 != f2)
        with self.subTest(i='not equal in context'):
            f1 = self.simple_fact()
            f2 = self.simple_fact()
            f2.context = 'anothercontext'
            self.assertTrue(f1 != f2)


class TestContext(unittest.TestCase):
    def simple_context(self):
        c = Context()
        c.contextid = 'context1'
        c.edate = dt.date(2019, 1, 1)
        c.entity = '0000001'
        return c

    def dim_context(self):
        c = self.simple_context()
        c.dim.append('FirstAxis')
        c.member.append('FirstAxisMember')
        c.dim.append('SecondAxis')
        c.member.append('SecondAxisMember')
        return c

    def test_asdictdim(self):
        with self.subTest(i='dim == 0'):
            c = self.simple_context()
            j = [{'context': 'context1',
                  'sdate': None,
                  'edate': dt.date(2019, 1, 1),
                  'dim': None, 'member': None}]
            self.assertListEqual(c.asdictdim(), j, msg=c.asdictdim())
        with self.subTest(i='dim == 2'):
            c = self.dim_context()
            j = [
                {'context': 'context1', 'sdate': None, 'edate': dt.date(
                    2019, 1, 1),
                 'dim': None, 'member': None},
                {'context': 'context1', 'sdate': None, 'edate': dt.date(
                    2019, 1, 1),
                 'dim': 'FirstAxis', 'member': 'FirstAxisMember'},
                {'context': 'context1', 'sdate': None, 'edate': dt.date(
                    2019, 1, 1),
                 'dim': 'SecondAxis', 'member': 'SecondAxisMember'}]
            self.assertListEqual(c.asdictdim(), j, msg=c.asdictdim())
        with self.subTest(i='dim == 0 and sdate=2018-01-01'):
            c = self.simple_context()
            c.sdate = dt.date(2018, 1, 1)
            j = [
                {'context': 'context1', 'sdate': dt.date(2018, 1, 1),
                 'edate': dt.date(2019, 1, 1),
                 'dim': None, 'member': None}]
            self.assertListEqual(c.asdictdim(), j, msg=c.asdictdim())

    def test_asdict(self):
        with self.subTest(i='dim == 0'):
            c = self.simple_context()
            j = {
                'context': 'context1',
                'sdate': None,
                'edate': dt.date(
                    2019,
                    1,
                    1)}
            self.assertDictEqual(c.asdict(), j, msg=c.asdict())

        with self.subTest(i='dim == 2'):
            c = self.dim_context()
            j = {
                'context': 'context1',
                'sdate': None,
                'edate': dt.date(
                    2019,
                    1,
                    1)}
            self.assertDictEqual(c.asdict(), j, msg=c.asdict())
        with self.subTest(i='dim == 0 and sdate=2018-01-01'):
            c = self.simple_context()
            c.sdate = dt.date(2018, 1, 1)
            j = {
                'context': 'context1', 'sdate': dt.date(
                    2018, 1, 1), 'edate': dt.date(
                    2019, 1, 1)}
            self.assertDictEqual(c.asdict(), j, msg=c.asdict())

    def test_isinstant(self):
        with self.subTest(i='instant'):
            c = self.simple_context()
            self.assertTrue(c.isinstant())
        with self.subTest(i='not instant'):
            c = self.simple_context()
            c.sdate = dt.date(2018, 1, 1)
            self.assertTrue(not c.isinstant())

    def test_isdimentional(self):
        with self.subTest(i='dimentional'):
            c = self.simple_context()
            self.assertTrue(not c.isdimentional())
        with self.subTest(i='not dimentional'):
            c = self.dim_context()
            self.assertTrue(c.isdimentional())

    def test_issuccessor(self):
        with self.subTest(i='successor'):
            c = self.dim_context()
            c.member[2] = 'us-gaap:SuccessorMember'
            self.assertTrue(c.issuccessor())
        with self.subTest(i='not successor'):
            c = self.dim_context()
            self.assertTrue(not c.issuccessor())

    def test_isparent(self):
        with self.subTest(i='parent'):
            c = self.dim_context()
            c.member[2] = 'us-gaap:ParentCompanuMember'
            self.assertTrue(c.isparent())
        with self.subTest(i='not successor'):
            c = self.dim_context()
            self.assertTrue(not c.isparent())


class TestUnit(unittest.TestCase):
    def simple_unit(self):
        u = Unit('isousd', 'usd')
        return u

    def ratio_unit(self):
        u = Unit('usdpershare', 'usd', 'share')
        return u

    def test_unitstr(self):
        with self.subTest(i='simple unit'):
            u = self.simple_unit()
            self.assertEqual(u.unitstr(), 'usd')
            self.assertEqual(str(u), 'usd')
        with self.subTest(i='ratio unit'):
            u = self.ratio_unit()
            self.assertEqual(u.unitstr(), 'usd/share')
            self.assertEqual(str(u), 'usd/share')


class TestXbrlParser(unittest.TestCase):
    def test_parsedei(self):
        self.maxDiff = None
        parser = XbrlParser()
        with self.subTest(i='aal-20181231.xml'):
            filename = make_absolute('res/xbrlparser/aal-20181231.xml')
            root = lxml.etree.parse(filename).getroot()
            # root = lxml.etree.parse('resources/gen-20121231.xml').getroot()
            units = parser.parse_units(root)
            dei = parser.parse_dei(root, units)
            jn = {
                "fye": [
                    ("--12-31",
                     "FD2018Q4YTD"),
                    ("--12-31",
                     "FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember")],
                "period": [
                    (str2date("2018-12-31"),
                     "FD2018Q4YTD"),
                    (str2date("2018-12-31"),
                     "FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember")],
                "shares": [
                    (449055548.0,
                     "I2019Q1Feb20"),
                    (1000.0,
                     "I2019Q1Feb20_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember")],
                "fy": [
                    (2018,
                     "FD2018Q4YTD"),
                    (2018,
                     "FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember")],
                "cik": [
                    (6201,
                     "FD2018Q4YTD"),
                    (4515,
                     "FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember")],
                "us_gaap": "2018-01-31",
                "company_name": [
                    ('American Airlines Group Inc.',
                     'FD2018Q4YTD'),
                    ('AMERICAN AIRLINES INC',
                     'FD2018Q4YTD_srt_ConsolidatedEntitiesAxis_srt_SubsidiariesMember')]}
            self.assertDictEqual(jn, dei.__dict__)

        with self.subTest(i='gen-20121231.xml'):
            filename = make_absolute('res/xbrlparser/gen-20121231.xml')
            root = lxml.etree.parse(filename).getroot()
            units = parser.parse_units(root)
            dei = parser.parse_dei(root, units)
            jn = {
                "fye": [
                    ("--12-31",
                     "D2012Q4YTD")],
                "period": [
                    (str2date("2012-12-31"),
                     "D2012Q4YTD")],
                "shares": [
                    (1.0,
                     "I2012Q4")],
                "fy": [
                    (2012,
                     "D2012Q4YTD")],
                "cik": [
                    (1140761,
                     "D2012Q4YTD_dei_LegalEntityAxis_gen_GenonAmericasGenerationLlcMember"),
                    (1138258,
                     "D2012Q4YTD_dei_LegalEntityAxis_gen_GenonMidAtlanticLlcMember"),
                    (1126294,
                     "D2012Q4YTD")],
                "us_gaap": "2012-01-31",
                "company_name": [
                    ('GenOn Energy, Inc.',
                     'D2012Q4YTD'),
                    ('GENON AMERICAS GENERATION LLC',
                     'D2012Q4YTD_dei_LegalEntityAxis_gen_GenonAmericasGenerationLlcMember'),
                    ('GENON MID-ATLANTIC, LLC',
                     'D2012Q4YTD_dei_LegalEntityAxis_gen_GenonMidAtlanticLlcMember')]}
            self.assertDictEqual(jn, dei.__dict__)

        with self.subTest(i='udr-20141231.xml (e.text is None == True)'):
            filename = make_absolute('res/xbrlparser/udr-20141231.xml')
            root = lxml.etree.parse(filename).getroot()
            units = parser.parse_units(root)
            dei = parser.parse_dei(root, units)
            jn = {
                "fye": [
                    ("--12-31",
                     "FD2014Q4YTD")],
                "period": [
                    (str2date("2014-12-31"),
                     "FD2014Q4YTD"),
                    (str2date("2014-12-31"),
                     "FD2014Q4YTD_dei_LegalEntityAxis_udr_UnitedDominionRealityLPMember")],
                "shares": [
                    (258765713,
                     "I2015Q1SD")],
                "fy": [
                    (2014,
                     "FD2014Q4YTD")],
                "cik": [
                    (74208,
                     "FD2014Q4YTD"),
                    (1018254,
                     "FD2014Q4YTD_dei_LegalEntityAxis_udr_UnitedDominionRealityLPMember")],
                "us_gaap": "2014-01-31",
                "company_name": [
                    ('UDR, Inc.',
                     'FD2014Q4YTD'),
                    ('United Dominion Realty L.P.',
                     'FD2014Q4YTD_dei_LegalEntityAxis_udr_UnitedDominionRealityLPMember')]}

            self.assertDictEqual(jn, dei.__dict__)

    def test_parse_facts(self):
        parser = XbrlParser()
        with self.subTest(i='cop-20141231.xml'):
            filename = make_absolute('res/xbrlparser/cop-20141231.xml')
            root = lxml.etree.parse(filename).getroot()
            facts = parser.parse_facts(root)
            self.assertEqual(len(facts), 3000)
            f1 = {'tag': 'EntityCommonStockSharesOutstanding',
                  'version': 'dei', 'value': 1231461668.0,
                  'context': 'AS_OF_Jan31_2015_Entity_0001163165',
                  'unitid': 'shares', 'decimals': 0, 'factid': 'ID_5'}
            f2 = {
                'tag': 'EntityPublicFloat',
                'version': 'dei',
                'value': 105400000000.0,
                'context': 'AS_OF_Dec31_2014_Entity_0001163165',
                'unitid': 'USD',
                'decimals': 8,
                'factid': 'ID_6'}
            self.assertDictEqual(facts[0].__dict__, f1)
            self.assertDictEqual(facts[1].__dict__, f2)

            ids: Set[str] = set()
            err: Set[str] = set()
            for f in facts:
                if f.factid in ids:
                    err.add(f.factid)
                ids.add(f.factid)

            self.assertTrue(len(err) == 0, msg=f'duplicate fact ids {err}')

    def test_parse_units(self):
        parser = XbrlParser()
        with self.subTest(i='cop-20141231.xml'):
            filename = make_absolute('res/xbrlparser/cop-20141231.xml')
            root = lxml.etree.parse(filename).getroot()
            units = parser.parse_units(root)

            self.assertEqual(len(units), 8)

    def test_parsefootnotes(self):
        parser = XbrlParser()
        with self.subTest(i='cop-20141231.xml (fn.text is None == True)'):
            filename = make_absolute('res/xbrlparser/cop-20141231.xml')
            root = lxml.etree.parse(filename).getroot()
            fn = parser.parse_footnotes(root)
            self.assertEqual(len(fn), 196)

    def test_parse_textblocks(self):
        parser = XbrlParser()
        with self.subTest(i='aag-20131231.xml'):
            filename = make_absolute('res/xbrlparser/aag-20131231.xml')
            root = lxml.etree.parse(filename).getroot()
            text_blocks = [
                'ScheduleOfShareBasedCompensationStockOptionsActivityTableTextBlock',
                'ScheduleOfShareBasedCompensationActivityTableTextBlock',
                'ScheduleOfShareBasedCompensationSharesAuthorizedUnderStockOptionPlansByExercisePriceRangeTable',
                'ScheduleOfShareBasedCompensationStockOptionsAndStockAppreciationRightsAwardActivityTableTextBlock']
            data = parser.parse_textblocks(root, text_blocks)
            self.assertEqual(
                len(data
                    [('ScheduleOfShareBasedCompensationActivityTableTextBlock',
                      'D2013Q4YTD_dei_LegalEntityAxis_aag_AmericanAirlinesIncMember')]),
                27719)
            self.assertEqual(
                len(
                    data[
                        ('ScheduleOfShareBasedCompensationStockOptionsAndStockAppreciationRightsAwardActivityTableTextBlock',
                         'D2013Q4YTD')]),
                59382)
            self.assertEqual(
                len(data
                    [('ScheduleOfShareBasedCompensationActivityTableTextBlock',
                      'D2013Q4YTD')]),
                27767)

        with self.subTest(i='ba-20131231.xml'):
            filename = make_absolute('res/xbrlparser/ba-20131231.xml')
            root = lxml.etree.parse(filename).getroot()
            text_blocks = [
                'ScheduleOfShareBasedCompensationStockOptionsActivityTableTextBlock',
                'ScheduleOfShareBasedCompensationActivityTableTextBlock',
                'ScheduleOfShareBasedCompensationSharesAuthorizedUnderStockOptionPlansByExercisePriceRangeTable',
                'ScheduleOfShareBasedCompensationStockOptionsAndStockAppreciationRightsAwardActivityTableTextBlock']
            data = parser.parse_textblocks(root, text_blocks)
            self.assertEqual(len(data[('ScheduleOfShareBasedCompensationStockOptionsActivityTableTextBlock',
                                       'FD2013Q4YTD')]), 25042)


class TestXbrlCleanUp(unittest.TestCase):
    def test_transform(self):
        parser = XbrlParser()

        with self.subTest(i='cop-20141231.xml'):
            filename = make_absolute('res/xbrlparser/aal-20181231.xml')
            root = lxml.etree.parse(filename).getroot()

            facts = parser.parse_facts(root)
            fn = parser.parse_footnotes(root)

            new_facts = xbrltrans.transform_facts(facts)
            new_fn = xbrltrans.transform_fn(new_facts, fn)

            self.assertEqual(len(facts), 3135)
            self.assertEqual(len(new_facts), 3124)
            self.assertEqual(len(fn), 6)
            self.assertEqual(len(new_fn), 6)

    def test_clean(self):
        parser = XbrlParser()
        xbrl_clean = XbrlCleanUp()
        with self.subTest(i='cop-20141231.xml'):
            filename = make_absolute('res/xbrlparser/aal-20181231.xml')
            root = lxml.etree.parse(filename).getroot()

            facts = parser.parse_facts(root)
            units = parser.parse_units(root)
            contexts = parser.parse_contexts(root)
            fn = parser.parse_footnotes(root)

            new_facts = xbrltrans.transform_facts(facts)
            new_fn = xbrltrans.transform_fn(new_facts, fn)

            f, u, c, _ = xbrl_clean.cleanup(
                new_facts, units, contexts, new_fn)

            self.assertEqual(len(facts), 3135)
            self.assertEqual(len(f), 3088)
            self.assertEqual(len(new_facts), 3124)
            self.assertEqual(len(units), 14)
            self.assertEqual(len(u), 4)
            self.assertEqual(len(contexts), 1223)
            self.assertEqual(len(c), 1223)
            self.assertEqual(len(fn), 6)
            self.assertEqual(len(fn), 6)


if __name__ == '__main__':
    unittest.main()
