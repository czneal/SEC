import datetime as dt
from typing import Any, List, Optional, Set, Tuple, Type

import logs
import mysqlio.basicio as do
import mysqlio.xbrlfileio as xio
import xbrlxml.dataminer as dm
from abstractions import Worker, Writer
from mpc import MpcManager
from settings import Settings
from utils import ProgressBar, remove_root_dir
from xbrlxml.xbrlrss import FileRecord, MySQLEnumerator


class Parser(Worker):
    def __init__(self,
                 miner_cls: Type[dm.DataMiner]):
        self.miner = miner_cls()
        self.logger = logs.get_logger(name=__name__)

    def feed(self, job: Tuple[FileRecord, str]) -> Optional[xio.ReportTuple]:
        (record, filename) = job

        self.logger.set_state(state={'state': record.adsh},
                              extra={'file_link': remove_root_dir(filename)})

        if not self.miner.feed(record.__dict__, filename):
            self.logger.revoke_state()
            return None

        company = dm.prepare_company(self.miner, record)
        report = dm.prepare_report(self.miner, record)
        nums = dm.prepare_nums(self.miner)
        shares = dm.prepare_shares(self.miner)

        self.logger.revoke_state()

        return (company, report, nums, shares)

    def flush(self):
        pass


def configure_worker() -> Worker:
    return Parser(miner_cls=dm.NumericDataMiner)


def configure_writer() -> Writer:
    return xio.ReportToDB()


def parse_mpc(method: str, after: dt.date, n_procs: int = Settings.n_proc()):
    manager = MpcManager('mysql', level=logs.logging.INFO)
    logger = logs.get_logger()
    try:
        rss = MySQLEnumerator()
        rss.set_filter_method(method, after)
        records = rss.filing_records()

        logger.set_state(state={'state': 'sec_parse'})
        logger.info(msg=f'start to parse {len(records)} reports')
        manager.start(to_do=records,
                      configure_writer=configure_writer,
                      configure_worker=configure_worker,
                      n_procs=n_procs)
        logger.info(msg=f'finish to parse {len(records)} reports')
    except Exception:
        logger.error('unexpected error', exc_info=True)
    logger.revoke_state()


def parse(method: str, after: dt.date, adsh: str = '') -> None:
    logger = logs.get_logger(__name__)
    worker = configure_worker()
    writer = configure_writer()

    try:
        rss = MySQLEnumerator()
        rss.set_filter_method(method=method, after=after, adsh=adsh)
        records = rss.filing_records()

        logger.set_state(state={'state': 'sec_parse'})
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
        logger.info(msg=f'finish to parse {len(records)} reports')
    except Exception:
        logger.error('unexpected error', exc_info=True)


if __name__ == '__main__':
    logs.configure('file', level=logs.logging.INFO)
    parse('explicit', dt.date(2013, 1, 1), adsh='0000065984-13-000050')
    #parse_mpc('new', dt.date(2013, 1, 1))
