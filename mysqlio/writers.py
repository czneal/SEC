import pandas as pd
import typing
import mysql.connector.errors as mysql_err

from abstractions import Writer

import mysqlio.basicio as do
import mysqlio.stocksio as sio
import firms.tickers as tic
import utils
import logs


class PandasWriter(Writer):
    def __init__(self, filename: str, *args, **kvargs):
        self.data: typing.List[typing.Dict[str, typing.Any]] = []
        self.filename = filename

    def write(self, obj: tic.StockData) -> None:
        self.data.append(obj)

    def flush(self) -> None:
        pd.DataFrame(self.data).to_csv(self.filename, index=False)


class StocksWriter(Writer):
    def __init__(self, *args, **kwargs):
        self.con = do.open_connection()
        self.cur = self.con.cursor(dictionary=True)
        self.stocks_daily = do.MySQLTable('stocks_daily', self.con)
        self.stocks_shares = do.MySQLTable('stocks_shares', self.con)

    def write(self, obj: tic.StockData) -> None:
        row = obj
        logger = logs.get_logger(name=__name__)

        if row.get('trade_date', None) is None:
            logger.warning(msg="ticker {} doesn't exist".format(row['ticker']))
            return

        if row.get('shares', None) is not None:
            utils.retry(
                5, mysql_err.InternalError)(
                sio.write_stocks_shares)(
                data=row, cur=self.cur, table=self.stocks_shares)
        else:
            logger.warning(msg="for ticker {0} shares doesn't exist".format(row['ticker']))

        utils.retry(
            5, mysql_err.InternalError)(
            self.stocks_daily.write_row)(
            row, self.cur)
        self.con.commit()

    def flush(self) -> None:
        try:
            self.con.commit()
            self.con.close()
        except Exception:
            pass


class HistoricalStocksWriter(Writer):
    def __init__(self, *args, **kwargs):
        self.con = do.open_connection()
        self.cur = self.con.cursor(dictionary=True)
        self.stocks_daily = do.MySQLTable('stocks_daily', self.con)
        self.stocks_dividents = do.MySQLTable('stocks_dividents', self.con)

    def write(self, obj: typing.Tuple[pd.DataFrame, pd.DataFrame]) -> None:
        stocks = obj[0].where((pd.notnull(obj[0])), None).reset_index()
        dividents = obj[1].where((pd.notnull(obj[1])), None).reset_index()

        utils.retry(
            5, mysql_err.InternalError)(
            self.stocks_daily.write_df)(
            stocks, self.cur)

        utils.retry(
            5, mysql_err.InternalError)(
            self.stocks_dividents.write_df)(
            dividents, self.cur)

        self.con.commit()

    def flush(self) -> None:
        try:
            self.con.commit()
            self.con.close()
        except Exception:
            pass


if __name__ == "__main__":
    pass
