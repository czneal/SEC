# -*- coding: utf-8 -*-
import datetime as dt
from exceptions import XbrlException
from itertools import product
from typing import List, Tuple, Dict, Set

import firms.fetch as f
import logs
import mysqlio.firmsio as fio
import xbrlxml.shares as shares
from firms.tickers import attach
from mysqlio.xbrlfileio import records_to_mysql
from xbrldown.download import download_and_save, download_rss
from xbrlxml.xbrlrss import MySQLEnumerator, XBRLEnumerator


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

        logger.info(
            'get info for new {0} companies from SEC site'.format(
                len(ciks)))
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


def attach_sec_shares_ticker() -> None:
    logger = logs.get_logger(name=__name__)
    logger.set_state({'state': attach_sec_shares_ticker.__name__})
    logger.info(f'try to find tickers for share members')
    try:
        true_shares: List[Dict[str, str]] = []

        shares_reader = shares.MySQLShares()
        reports_reader = shares.MySQLReports()

        # ciks = shares_reader.fetch_nasdaq_ciks()
        ciks = [63754]
        logger.info(f'begin with {len(ciks)} filers')

        not_logging: Dict[str, Set] = {'cik': {68622}, 'adsh': {
            '0001558370-15-000135', '0001047469-15-001510', '0001193125-14-116022'}}
        for cik in ciks:
            tickers = shares.process_tickers(shares_reader.fetch_tickers(cik))
            for adsh in reports_reader.fetch_adshs(cik):
                sec_shares = shares.process_sec_shares(
                    shares_reader.fetch_sec_shares(adsh))

                if not shares.join_sec_stocks_tickers(
                        sec_shares, tickers,
                        cik, shares_reader):
                    if (cik not in not_logging['cik'] and
                            adsh not in not_logging['adsh']):
                        logger.set_state(state={'state': str(adsh)})
                        logger.warning(str(tickers))
                        logger.warning(str(sec_shares))
                        logger.revoke_state()
                else:
                    for shares_list in sec_shares.values():
                        for share in shares_list:
                            if share.ticker != '':
                                true_shares.append(
                                    {
                                        'adsh': adsh,
                                        'member': share.member,
                                        'ticker': share.ticker,
                                        'name': 'dei:EntityCommonStockSharesOutstanding'})

        logger.info(f'end with {len(ciks)} filers')

        shares_reader.close()
        reports_reader.close()

        logger.info(f'write tickers into sec_shares, begin')
        shares_writer = shares.ShareTickerRelation()
        shares_writer.write(true_shares)
        shares_writer.flush()
        shares_writer.close()
        logger.info(f'write tickers into sec_shares, end')
    except Exception:
        logger.error('unexpected error', exc_info=True)


if __name__ == '__main__':
    logs.configure('file', level=logs.logging.INFO)

    update_xbrl_sec_forms(years=[2020], months=[m for m in range(1, 13)])
    update_sec_forms(years=[2020], months=[m for m in range(1, 13)])
    download_report_files('all', dt.date(2013, 1, 1))
    attach_sec_shares_ticker()
