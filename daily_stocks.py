import pytz
import datetime
from typing import List, cast, Tuple
import pandas as pd
import logs

import mysqlio.basicio as do
from mpc import MpcManager
from firms.tickers import stock_data, StockData, historical_data, historical_dividents
from mysqlio.writers import StocksWriter
from abstractions import Worker, Writer, JobType, WriterProxy
from settings import Settings


class StocksWorker(Worker):
    def __init__(self):
        pass

    def feed(self,
             job: JobType) -> Tuple[StockData,
                                    pd.DataFrame,
                                    pd.DataFrame]:
        logger = logs.get_logger(name=__name__)
        ticker = str(job)
        logger.debug(f'request for ticker {ticker}')
        stocks = stock_data(ticker)
        h_stocks = historical_data(ticker, days=7)
        h_div = historical_dividents(ticker)
        return (stocks, h_stocks, h_div)

    def flush(self):
        pass


def off_hours(tm: datetime.datetime = datetime.datetime.now()) -> bool:
    eastern = pytz.timezone('US/Eastern')
    now_ny = tm.astimezone(eastern)
    if now_ny.weekday() in (5, 6):
        return True

    if datetime.time(8, 0, 0) <= now_ny.time() <= datetime.time(20, 15, 0):
        return False
    return True


def configure_worker() -> Worker:
    logs.logging.getLogger(name='urllib3').setLevel(logs.logging.WARNING)

    return StocksWorker()


def configure_writer() -> Writer:
    return StocksWriter()


def main():
    if not off_hours():
        print('you should run this script in off hours of nasdaq')
        exit()

    with do.OpenConnection() as con:
        cur = con.cursor()
        cur.execute('select ticker from nasdaq')
        tickers: List[str] = [cast(str, ticker)
                              for (ticker,) in cur.fetchall()]

    manager = MpcManager('mysql', level=logs.logging.INFO)
    logger = logs.get_logger('daily_stocks')
    logger.set_state(state={'state': 'daily_stocks'})
    logger.info(
        msg='download daily tickers from nasdaq to stocks_shares and stock_daily tables')
    logger.info(msg='start to download {0} tickers'.format(len(tickers)))
    manager.start(to_do=tickers[:10],
                  configure_writer=configure_writer,
                  configure_worker=configure_worker,
                  n_procs=Settings.n_proc_nasdaq())
    logger.info(msg='finish to download {0} tickers'.format(len(tickers)))
    logger.revoke_state()


if __name__ == '__main__':
    # do.activate_test_mode()

    main()

    # logs.configure('file', level=logs.logging.DEBUG)
    # worker = configure_worker()
    # writer = configure_writer()

    # obj = worker.feed('X')
    # writer.write(obj)
    # writer.flush()
