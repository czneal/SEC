import pytz
import datetime
from typing import List, cast
import logs

import mysqlio.basicio as do
from mpc import MpcManager
from firms.tickers import stock_data, StockData
from mysqlio.writers import PandasWriter, StocksWriter
from abstractions import Worker, Writer, JobType, WriterProxy


class StocksWorker(Worker):
    def __init__(self):
        pass

    def feed(self, job: JobType) -> StockData:
        logger = logs.get_logger(name=__name__)
        logger.info('request for ticker {}'.format(str(job)))
        return stock_data(cast(str, job))

    def flush(self):
        pass


def off_hours(tm: datetime.datetime = datetime.datetime.now()) -> bool:
    eastern = pytz.timezone('US/Eastern')
    now_ny = tm.astimezone(eastern)
    if datetime.time(8, 0, 0) <= now_ny.time() <= datetime.time(20, 15, 0):
        return False
    return True


def configure_worker() -> Worker:
    import logging
    logging.getLogger(name='urllib3').setLevel(logging.WARNING)

    return StocksWorker()


def configure_writer() -> Writer:
    return StocksWriter()


if __name__ == '__main__':

    if not off_hours():
        print('you should run this script in off hours of nasdaq')
        exit()

    with do.OpenConnection() as con:
        cur = con.cursor()
        cur.execute('select ticker from nasdaq')
        tickers: List[str] = [cast(str, ticker)
                              for (ticker,) in cur.fetchall()]

    manager = MpcManager('file', level=logs.logging.INFO)
    logger = logs.get_logger()
    logger.info(
        msg='download daily tickers from nasdaq to stocks_shares and stock_daily tables')
    logger.info(msg='start to download {0} tickers'.format(len(tickers)))
    manager.start(to_do=tickers,
                  configure_writer=configure_writer,
                  configure_worker=configure_worker,
                  n_procs=6)
    logger.info(msg='finish to download {0} tickers'.format(len(tickers)))
