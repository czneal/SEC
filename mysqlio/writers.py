import pandas as pd
import typing
import atexit

from mysqlio.basicio import retry_mysql_write
from abstractions import Writer

import mysqlio.basicio as do
import mysqlio.stocksio as sio
import firms.tickers as tic
import utils
import logs


class PandasWriter(Writer):
    def __init__(self, filename: str, *args, **kwargs):
        self.data: typing.List[typing.Dict[str, typing.Any]] = []
        self.filename = filename

    def write(self, obj: tic.StockData) -> None:
        self.data.append(obj)

    def flush(self) -> None:
        pd.DataFrame(self.data).to_csv(self.filename, index=False)


class MySQLWriter(Writer):
    def __init__(self):
        self.n_retry: int = 20
        self.con = do.open_connection()
        self.cur = self.con.cursor(dictionary=True)
        atexit.register(self.close)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass

    def flush(self) -> None:
        try:
            self.con.commit()
        except Exception:
            pass

    def table(self,
              table_name: str,
              use_simple_insert: bool = False) -> do.MySQLTable:
        return do.MySQLTable(table_name=table_name,
                             con=self.con,
                             use_simple_insert=use_simple_insert)

    def write_to_table(self, table: do.MySQLTable, data) -> None:
        retry_mysql_write(
            table.write)(
            data, self.cur)

    def execute_in(self, query: str,
                   in_seq: typing.Iterable[typing.Any]) -> None:
        """
        query must be some thing like 'delete from table where field in (__in__)'
        """
        params = tuple([e for e in in_seq])
        if not params:
            return

        query = query.replace('__in__', ', '.join(
            ['%s' for i in range(len(params))]))
        self.cur.execute(query, params)


class StocksWriter(MySQLWriter):
    def __init__(self, *args, **kwargs):
        MySQLWriter.__init__(self)
        self.stocks_daily = do.MySQLTable('stocks_daily', self.con)
        self.stocks_shares = do.MySQLTable('stocks_shares', self.con)
        self.nasdaq = do.MySQLTable('nasdaq', self.con)
        self.hist_writer = HistoricalStocksWriter()

    def write(
            self, obj: typing.Tuple
            [tic.StockData, pd.DataFrame, pd.DataFrame]) -> None:
        row = obj[0]
        logger = logs.get_logger(name=__name__)

        if row.get('trade_date', None) is None:
            logger.warning(msg="ticker {} doesn't exist".format(row['ticker']))
            return

        if row.get('shares', None) is not None:
            retry_mysql_write(
                sio.write_stocks_shares)(
                data=row, cur=self.cur, table=self.stocks_shares)
        else:
            logger.warning(
                msg="for ticker {0} shares doesn't exist".format(
                    row['ticker']))

        if not(row['ttype'] == '' or row['ttype'] is None):
            retry_mysql_write(
                self.nasdaq.update_row)(
                    row=row,
                    update_fields=['ttype'],
                    cur=self.cur)

        retry_mysql_write(
            self.stocks_daily.write_row)(
            row, self.cur)
        self.con.commit()

        self.hist_writer.write((obj[1], obj[2]))

    def flush(self) -> None:
        MySQLWriter.flush(self)
        self.hist_writer.flush()


class HistoricalStocksWriter(MySQLWriter):
    def __init__(self, *args, **kwargs):
        MySQLWriter.__init__(self)
        self.stocks_daily = do.MySQLTable('stocks_daily', self.con)
        self.stocks_dividents = do.MySQLTable('stocks_dividents', self.con)

    def write(self, obj: typing.Tuple[pd.DataFrame, pd.DataFrame]) -> None:
        if obj[0] is None or obj[1] is None:
            return

        stocks = obj[0].where((pd.notnull(obj[0])), None).reset_index()
        dividents = obj[1].where((pd.notnull(obj[1])), None).reset_index()

        retry_mysql_write(
            self.stocks_daily.write_df)(
            stocks, self.cur)

        retry_mysql_write(
            self.stocks_dividents.write_df)(
            dividents, self.cur)

        self.con.commit()


if __name__ == "__main__":
    data = tic.stock_data('AAPL')
    w = StocksWriter()
    w.write((data, None, None))
    w.flush()
