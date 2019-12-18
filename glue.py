# -*- coding: utf-8 -*-
import datetime as dt
from itertools import product
from typing import List

import firms.fetch as f
import logs
import mysqlio.firmsio as fio
from exceptions import XbrlException
from firms.tickers import attach
from xbrlxml.xbrlrss import XBRLEnumerator, MySQLEnumerator
from mysqlio.xbrlfileio import records_to_mysql
from xbrldown.download import download_rss, download_and_save


def update_xbrl_sec_forms(years: List[int], months: List[int]) -> None:
    logger = logs.get_logger(name=__name__)
    logger.set_state(state={'state': update_xbrl_sec_forms.__name__})
    try:
        for y, m in product(years, months):
            logger.info(f'download rss file for year: {y} and month: {m}')
            download_rss(y, m)

        logger.info('read SEC XBRL forms')
        rss = XBRLEnumerator(years, months)
        records = rss.filing_records()
        logger.info(f'read {len(records)} record(s)')

        logger.info('write SEC XBRL forms')
        records_to_mysql(records)

    except Exception:
        logger.error('unexpected error', exc_info=True)
    finally:
        logger.revoke_state()


def update_sec_forms(years: List[int], months: List[int]) -> None:
    quarters = set([int((m + 2) / 3) for m in months])
    logger = logs.get_logger(name=__name__)
    logger.set_state(state={'state': update_sec_forms.__name__})

    for y, q in product(years, quarters):
        try:
            logger.info(f'get crawler file for year: {y}, quarter: {q}')
            forms = f.get_sec_forms(y, q)

            logger.info(
                f'write sec forms for year: {y}, quarter: {q} to database')
            fio.write_sec_forms(forms)
        except Exception:
            logger.error('error while writing sec forms', exc_info=True)

    logger.revoke_state()


def update_companies_nasdaq() -> None:
    logger = logs.get_logger(name=__name__)
    logger.set_state(state={'state': update_companies_nasdaq.__name__})

    try:
        logger.info(f'update companies and attach nasdaq symbols')

        logger.info('get new companies cik')
        new_companies = fio.get_new_companies()
        ciks = list(new_companies['cik'].unique())

        logger.info('get info for new {0} companies from SEC site'.format(len(ciks)))
        companies = f.companies_search_mpc(ciks)
        companies['updated'] = dt.date.today()

        logger.info('write new companies to database')
        fio.write_companies(companies)

        logger.info('attach nasdaq symbols')
        nasdaq = attach()

        logger.info('write nasdaq symbols to database')
        fio.write_nasdaq(nasdaq)

    except Exception as e:
        logger.error('unexpected error', exc_info=True)

    logger.revoke_state()


def download_report_files(method: str, after: dt.date) -> None:
    logger = logs.get_logger(name=__name__)
    logger.set_state(state={'state': download_report_files.__name__})
    try:
        logger.info('get new XBRL reports')
        rss = MySQLEnumerator()
        rss.set_filter_method(method=method, after=after)
        records = rss.filing_records()

        logger.info(f'try to download {len(records)} new XBRL reports')
        for (record, filename) in records:
            try:
                download_and_save(record.cik, record.adsh, filename)
            except (XbrlException, FileExistsError) as e:
                logger.error(str(e))
            except Exception:
                logger.error(
                    f'unexpected error {record.adsh}, {filename}',
                    exc_info=True)
        logger.info(f'downloaded {len(records)} new XBRL reports')
    except Exception:
        logger.error('unexpected error', exc_info=True)

    logger.revoke_state()


if __name__ == '__main__':
    logs.configure('file', level=logs.logging.INFO)

    update_xbrl_sec_forms(years=[2019], months=[m for m in range(1, 13)])
    update_sec_forms(years=[2019], months=[m for m in range(1, 13)])
    download_report_files('new', dt.date(2019, 1, 1))
    update_companies_nasdaq()
