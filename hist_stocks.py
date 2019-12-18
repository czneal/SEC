from typing import Tuple, List, cast

import pandas as pd

import firms.tickers as t
import logs
import mysqlio.basicio as do
from abstractions import Worker, JobType, Writer
from mpc import MpcManager
from mysqlio.writers import HistoricalStocksWriter


class HistoricalStocksWorker(Worker):
    def __init__(self):
        pass

    def feed(self, job: JobType) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return (t.historical_data(cast(str, job), days=60),
                t.historical_dividents(cast(str, job)))

    def flush(self):
        pass


def configure_worker() -> Worker:
    import logging
    logging.getLogger(name='urllib3').setLevel(logging.WARNING)

    return HistoricalStocksWorker()


def configure_writer() -> Writer:
    return HistoricalStocksWriter()


if __name__ == '__main__':
    with do.OpenConnection() as con:
        cur = con.cursor()
        cur.execute('select ticker from nasdaq')
        tickers: List[str] = [cast(str, ticker)
                              for (ticker,) in cur.fetchall()]

    manager = MpcManager('file', level=logs.logging.INFO)
    logger = logs.get_logger(__name__)
    logger.set_state(state={'state': __file__})
    logger.info(
        msg='download historical data to tables stocks_daily and stocks_dividents')
    logger.info(msg='start to download {0} tickers'.format(len(tickers)))
    manager.start(to_do=tickers,
                  configure_writer=configure_writer,
                  configure_worker=configure_worker,
                  n_procs=8)
    logger.info(msg='finish to download {0} tickers'.format(len(tickers)))
    logger.revoke_state()
