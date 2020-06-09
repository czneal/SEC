import datetime as dt
from typing import Any, List, Optional, Set, Tuple, Type, Dict, Callable
from functools import partial

import logs
import mysqlio.basicio as do
import mysqlio.xbrlfileio as xio
import xbrlxml.dataminer as dm

from xbrlxml.xbrlfileparser import TextBlocks
from abstractions import Worker, Writer
from mpc import MpcManager
from settings import Settings
from utils import ProgressBar, remove_root_dir
from xbrlxml.xbrlrss import FileRecord, MySQLEnumerator


class XBRLLogsCleaner(xio.MySQLWriter):
    def __init__(self):
        super().__init__()

    def clean(self, adsh: str):
        self.cur.execute('delete from xbrl_logs where state=%s', (adsh,))
        self.flush()

    def write(self, obj):
        pass


class Parser(Worker):
    def __init__(self,
                 miner_cls: Type[dm.DataMiner]):
        self.miner = miner_cls()
        self.logger = logs.get_logger(name=__name__)
        self.xbrl_logs_cleaner = XBRLLogsCleaner()

    def feed(self, job: Tuple[FileRecord, str]) -> Optional[xio.ReportTuple]:
        (record, filename) = job

        self.logger.set_state(state={'state': record.adsh},
                              extra={'file_link': remove_root_dir(filename)})
        self.xbrl_logs_cleaner.clean(adsh=record.adsh)

        if not self.miner.feed(record, filename):
            self.logger.revoke_state()
            return None

        try:
            company = dm.prepare_company(record)
            report = dm.prepare_report(self.miner, record)
            nums = dm.prepare_nums(self.miner)
            shares = dm.prepare_shares(self.miner)
        except Exception:
            self.logger.error(
                'error while prepare report to write',
                exc_info=True)
            self.logger.revoke_state()
            return None

        self.logger.revoke_state()

        return (company, report, nums, shares)

    def flush(self):
        pass


class TextBlocksParser(Worker):
    def __init__(self):
        self.miner = dm.TextBlocksDataMiner()
        self.logger = logs.get_logger('text_block_parser')

    def feed(self, job: Tuple[FileRecord, str]) -> List[Dict[str, Any]]:
        (record, filename) = job

        self.logger.set_state(state={'state': record.adsh},
                              extra={'file_link': remove_root_dir(filename)})
        # self.xbrl_logs_cleaner.clean(adsh=record.adsh)

        blocks: List[Dict[str, Any]] = []
        try:
            if not self.miner.feed(record, filename):
                raise Exception('report is not read')

            blocks = dm.prepare_text_blocks(
                adsh=record.adsh,
                text_blocks=self.miner.xbrlfile.text_blocks)
        except Exception:
            self.logger.error(
                'error while prepare report to write',
                exc_info=True)

        self.logger.revoke_state()
        return blocks

    def flush(self):
        pass


def configure_worker() -> Worker:
    return Parser(miner_cls=dm.NumericDataMiner)


def configure_writer() -> Writer:
    return xio.ReportToDB()


def conf_text_block_worker() -> Worker:
    return TextBlocksParser()


def conf_text_block_writer() -> Writer:
    return xio.TextBlocksWriter()


def parse_mpc(method: str, after: dt.date, n_procs: int = Settings.n_proc()):
    manager = MpcManager('mysql', level=logs.logging.INFO)
    logger = logs.get_logger()
    logger.set_state(state={'state': 'sec_parse'})

    try:
        rss = MySQLEnumerator()
        rss.set_filter_method(method, after)
        records = rss.filing_records()

        logger.info(msg=f'start to parse {len(records)} reports')
        manager.start(to_do=records,
                      configure_writer=configure_writer,
                      configure_worker=configure_worker,
                      n_procs=n_procs)
        logger.info(msg=f'finish to parse {len(records)} reports')
    except Exception:
        logger.error('unexpected error', exc_info=True)
    logger.revoke_state()


def parse(method: str,
          after: dt.date,
          cnf_worker: Callable[[], Worker],
          cnf_writer: Callable[[], Writer],
          adsh: str = '') -> None:
    logger = logs.get_logger(__name__)
    logger.set_state(state={'state': 'sec_parse'})

    worker = cnf_worker()
    writer = cnf_writer()

    try:
        rss = MySQLEnumerator()
        rss.set_filter_method(method=method, after=after, adsh=adsh)
        records = rss.filing_records()

        logger.info(msg=f'start to parse {len(records)} reports')
        pb = ProgressBar()
        pb.start(len(records))

        for record in records:
            obj = worker.feed(record)
            writer.write(obj)

            pb.measure()
            print('\r' + pb.message(), end='')
        print()

        worker.flush()
        writer.flush()
        logger.info(msg=f'finish to parse {len(records)} reports')

    except Exception:
        logger.error('unexpected error', exc_info=True)

    logger.revoke_state()


# def reparse():
#     with do.OpenConnection() as con:
#         cur = con.cursor()
#         cur.execute('truncate table xbrl_logs')
#         cur.execute('truncate table reports')
#         cur.execute('truncate table mgnums')
#         con.commit()

#     parse_mpc(method='all', after=dt.date(2013, 1, 1))
#     glue.attach_sec_shares_ticker()

if __name__ == '__main__':
    logs.configure('file', level=logs.logging.INFO)
    parse(method='all',
          after=dt.date(2019, 1, 1),
          cnf_worker=conf_text_block_worker,
          cnf_writer=conf_text_block_writer)

    # reparse()
    pass
