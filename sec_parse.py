import datetime as dt
from typing import List, Type, Set, Any, Tuple

import mysqlio.basicio as do
import logs
from mpc import MpcManager
from abstractions import Worker, WriterProxy, Writer
from xbrlxml.dataminer import NumericDataMiner, DataMiner
from xbrlxml.xbrlrss import MySQLEnumerator, FileRecord
from mysqlio.xbrlfileio import ReportToDB, ReportWriter
from utils import remove_root_dir, ProgressBar


class Parser(Worker):
    def __init__(self,
                 miner_cls: Type[DataMiner],
                 writer_cls: Type[ReportWriter]):
        self.miner = miner_cls()
        self.writer = writer_cls()
        self.logger = logs.get_logger(name=__name__)

    def feed(self, job: Tuple[FileRecord, str]) -> str:
        (record, filename) = job
        self.logger.set_state(state={'state': record.adsh},
                              extra={'file_link': remove_root_dir(filename)})

        if self.miner.feed(record.__dict__, filename):
            self.writer.write(record, self.miner)

        self.logger.revoke_state()
        return 'done'

    def flush(self):
        self.writer.flush()


def configure_worker() -> Worker:
    return Parser(miner_cls=NumericDataMiner,
                  writer_cls=ReportToDB)


def configure_writer() -> Writer:
    return WriterProxy()


def parse_mpc(method: str, after: dt.date):
    manager = MpcManager('mysql', level=logs.logging.INFO)
    logger = logs.get_logger()
    try:
        rss = MySQLEnumerator()
        rss.set_filter_method(method, after)
        records = rss.filing_records()

        logger.set_state(state={'state': 'sec_parse'})
        logger.info(msg=f'start to parse {len(records)} reports')
        manager.start(to_do=records[0:100],
                      configure_writer=configure_writer,
                      configure_worker=configure_worker,
                      n_procs=6)
        logger.info(msg=f'finish to parse {len(records)} reports')
    except Exception:
        logger.error('unexpected error', exc_info=True)
    logger.revoke_state()


def parse(method: str, after: dt.date) -> None:
    logs.configure('mysql', level=logs.logging.INFO)
    logger = logs.get_logger(__name__)
    worker = configure_worker()

    try:
        rss = MySQLEnumerator()
        rss.set_filter_method(method=method, after=after)
        records = rss.filing_records()

        logger.set_state(state={'state': 'sec_parse'})
        logger.info(msg=f'start to parse {len(records)} reports')
        pb = ProgressBar()
        pb.start(len(records))

        for record in records[0:10]:
            worker.feed(record)
            pb.measure()
            print('\r' + pb.message(), end='')
        print()

        worker.flush()
        logger.info(msg=f'finish to parse {len(records)} reports')
    except Exception:
        logger.error('unexpected error', exc_info=True)


if __name__ == '__main__':
    # parse('new', dt.date(2019, 1, 1))
    parse_mpc('new', dt.date(2019, 1, 1))
