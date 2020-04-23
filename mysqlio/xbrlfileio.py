# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 18:28:09 2019

@author: Asus
"""
import sys
import atexit
import pandas as pd
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Tuple, Optional

import mysqlio.basicio as do
import queries as q
import logs

from mysqlio.writers import MySQLWriter
from xbrlxml.xbrlexceptions import XbrlException
from xbrlxml.dataminer import SharesDataMiner, DataMiner
from xbrlxml.xbrlrss import FileRecord
from utils import remove_root_dir, retry

ReportTuple = Tuple[Dict[str, Any],
                    Dict[str, Any],
                    Optional[pd.DataFrame],
                    Optional[pd.DataFrame]]


class ReportToDB(MySQLWriter):
    def __init__(self):
        MySQLWriter.__init__(self)

        self.retrys = 20

        self.reports = do.MySQLTable('reports', self.con)
        self.nums = do.MySQLTable('mgnums', self.con)
        self.companies = do.MySQLTable('companies', self.con)
        self.companies.set_insert_if('updated')
        self.shares = do.MySQLTable('sec_shares', self.con)

    def write(self, obj: Optional[ReportTuple]) -> bool:
        if obj is None:
            return False

        logger = logs.get_logger(name=__name__)
        try:
            logger.set_state(state={'state': obj[1]['adsh']})

            do.retry_mysql_write(self.write_company)(obj[0])
            do.retry_mysql_write(self.write_report)(obj[1])
            do.retry_mysql_write(self.write_nums)(obj[2])
            do.retry_mysql_write(self.write_shares)(obj[3])
            self.flush()
            logger.info('report data has been writen')
            logger.revoke_state()
            return True
        except do.InternalError:
            logger.error(msg='mysql super dead lock', exc_info=True)
        except Exception:
            logger.error(msg='unexpected error', exc_info=True)

        logger.revoke_state()
        return False

    def write_report(self, report: Dict[str, Any]):
        self.reports.write(report, self.cur)

    def write_nums(self, facts: Optional[pd.DataFrame]):
        if facts is None:
            return

        self.nums.write(facts, self.cur)

    def write_company(self, company: Dict[str, Any]) -> None:
        self.companies.write(company, self.cur)

    def write_shares(self, shares_facts: Optional[pd.DataFrame]) -> None:
        if shares_facts is None:
            return

        self.shares.write(shares_facts, self.cur)


class ShareTickerRelation(MySQLWriter):
    def __init__(self):
        MySQLWriter.__init__(self)

        self.sec_shares_ticker = do.MySQLTable('sec_shares_ticker', self.con)

    def write(self, row_list: List[Dict[str, str]]) -> None:
        self.sec_shares_ticker.write(row_list, self.cur)


def records_to_mysql(records: List[Tuple[FileRecord, str]]) -> None:
    row_list: List[Dict[str, Any]] = []
    for (record, zip_filename) in records:
        row_list.append({
            'cik': record.cik,
            'adsh': record.adsh,
            'file_link': remove_root_dir(zip_filename),
            'filed': record.file_date,
            'period': record.period,
            'fy': record.fy,
            'record': str(record),
            'company_name': record.company_name
        })
    with do.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        table = do.MySQLTable('sec_xbrl_forms', con)
        table.write(row_list, cur)
        con.commit()
