# -*- coding: utf-8 -*-
import pandas as pd

from typing import Dict
from mysql.connector.cursor import MySQLCursor  # type: ignore

from mysqlio.basicio import OpenConnection
from mysqlio.basicio import Table, MySQLTable
from firms.tickers import stock_data, StockData


def write_stocks_daily_df(stock: pd.DataFrame) -> None:
    """
    write data into stocks_daily table
    DataFrame stock should contain
    columns [ticker, trade_date, open, high, low, close, volume]
    """
    columns = [
        'ticker',
        'trade_date',
        'open',
        'high',
        'low',
        'close',
        'volume']
    mapping: Dict[str, str] = {}
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)

        stocks_daily = Table(name='stocks_daily', con=con)
        stocks_daily.write_df(
            df=stock[columns].rename(
                columns=mapping), cur=cur)

        con.commit()


def write_stocks(data: StockData) -> None:
    with OpenConnection() as con:
        table = Table('stocks_daily', con)
        cur = con.cursor(dictionary=True)
        table.write(data, cur)
        table.flush(cur)
        con.commit()


def write_stocks_shares(data: StockData,
                        cur: MySQLCursor,
                        table: MySQLTable) -> None:
    select = """select * from stocks_shares
                where ticker = %(ticker)s
                    and trade_date < %(trade_date)s
                order by trade_date desc
                limit 1"""
    cur.execute(select, {'ticker': data['ticker'],
                         'trade_date': data['trade_date']})
    row = cur.fetchone()
    if row and row['shares'] == data['shares']:
        return

    table.write_row(data, cur)
