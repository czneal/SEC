# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 18:28:09 2019

@author: Asus
"""
import json
import sys
import atexit
import pandas as pd
import mysql.connector
from mysql.connector.errors import InternalError, Error
from mysql.connector.cursor import MySQLCursor  # type: ignore
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Tuple, Optional

import mysqlio.basicio as do
import queries as q
import algos.xbrljson
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
        self.shares = do.MySQLTable('sec_shares', self.con)
        self.companies.set_insert_if('updated')

    def write(self, obj: Optional[ReportTuple]) -> bool:
        if obj is None:
            return False

        logger = logs.get_logger(name=__name__)
        try:
            retry(self.retrys, InternalError)(self.write_company)(obj[0])
            retry(self.retrys, InternalError)(self.write_report)(obj[1])
            retry(self.retrys, InternalError)(self.write_nums)(obj[2])
            retry(self.retrys, InternalError)(self.write_shares)(obj[3])
            self.flush()
            logger.info('report data has been writen')
            return True
        except InternalError:
            logger.error(msg='mysql super dead lock', exc_info=True)
        except Exception:
            logger.error(msg='unexpected error', exc_info=True)

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
