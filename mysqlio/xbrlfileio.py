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
from typing import Any, Dict, List, Tuple

import mysqlio.basicio as do
import queries as q
import algos.xbrljson
import logs

from xbrlxml.xbrlexceptions import XbrlException
from xbrlxml.dataminer import SharesDataMiner, DataMiner
from xbrlxml.xbrlrss import FileRecord
from utils import remove_root_dir, retry


class ReportWriter(metaclass=ABCMeta):
    @abstractmethod
    def write(self,
              record: FileRecord,
              miner: DataMiner) -> bool:
        pass

    @abstractmethod
    def flush(self, commit: bool = True) -> None:
        pass


class ReportToDB(ReportWriter):
    def __init__(self):
        self.retrys = 20
        self.con: mysql.connector.MySQLConnection = do.open_connection()
        self.cur: MySQLCursor = self.con.cursor(dictionary=True)
        atexit.register(self.flush)

        self.reports = do.MySQLTable('reports', self.con)
        self.nums = do.MySQLTable('mgnums', self.con)
        self.companies = do.MySQLTable('companies', self.con)
        self.shares = do.MySQLTable('sec_shares', self.con)
        self.companies.set_insert_if('updated')

        atexit.register(self.close)

    def write(self, record: FileRecord, miner: DataMiner) -> bool:
        logger = logs.get_logger(name=__name__)
        try:
            retry(self.retrys, InternalError)(self.write_company)(record)
            retry(self.retrys, InternalError)(self.write_report)(record, miner)
            retry(self.retrys, InternalError)(self.write_nums)(record, miner)
            retry(self.retrys, InternalError)(self.write_shares)(record, miner)
            self.commit()
            logger.info('report data has been writen')
            return True
        except InternalError:
            logger.error(msg='mysql super dead lock', exc_info=True)
        except Exception:
            logger.error(msg='unexpected error', exc_info=True)

        return False

    def commit(self) -> None:
        if self.con.is_connected():
            self.con.commit()

    def close(self) -> None:
        if self.con.is_connected():
            self.con.close()

    def flush(self, commit: bool = True) -> None:
        try:
            if commit:
                self.commit()
            self.close()
        except Exception:
            logger = logs.get_logger(name=__name__)
            logger.error(
                'unexpected error while flushing to database',
                exc_info=True)

    def _dumps_structure(self, miner):
        structure = {}
        for sheet, roleuri in miner.sheets.mschapters.items():
            xsd_chapter = miner.xbrlfile.schemes['xsd'].get(roleuri, None)
            calc_chapter = (miner.xbrlfile
                            .schemes['calc']
                            .get(roleuri, {"roleuri": roleuri}))
            structure[sheet] = {
                'label': xsd_chapter.label,
                'chapter': calc_chapter
            }

        return json.dumps(structure, cls=algos.xbrljson.ForDBJsonEncoder)

    def _dums_contexts(self, miner):
        return json.dumps(miner.extentions)

    def write_report(self, record: FileRecord, miner: DataMiner):
        file_link = remove_root_dir(miner.zip_filename)
        report = {'adsh': miner.adsh,
                  'cik': miner.cik,
                  'period': miner.xbrlfile.period,
                  'period_end': miner.xbrlfile.fye,
                  'fin_year': miner.xbrlfile.fy,
                  'taxonomy': miner.xbrlfile.dei['us-gaap'],
                  'form': record.form_type,
                  'quarter': 0,
                  'file_date': record.file_date,
                  'file_link': file_link,
                  'trusted': 1,
                  'structure': self._dumps_structure(miner),
                  'contexts': self._dums_contexts(miner)
                  }
        self.reports.write(report, self.cur)

    def write_nums(self, record: FileRecord, miner: DataMiner):
        if miner.numeric_facts is None:
            return

        self.nums.write(miner.numeric_facts, self.cur)

    def write_company(self, record: FileRecord) -> None:
        company = {'company_name': record.company_name,
                   'sic': record.sic,
                   'cik': record.cik,
                   'updated': record.file_date}

        self.companies.write(company, self.cur)

    def write_shares(self, record: FileRecord, miner: DataMiner) -> None:
        if miner.shares_facts is None:
            return

        self.shares.write(miner.shares_facts, self.cur)


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
