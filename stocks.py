# -*- coding: utf-8 -*-
import datetime as dt
import pandas as pd

from typing import List, Optional, Dict, Union, cast

from utils import ProgressBar
from firms.tickers import historical_data, historical_dividents
from firms.tickers import stock_data, StockData
from mysqlio.basicio import Table, OpenConnection


def load_historical_data(
        tickers: List[str],
        todate: Optional[dt.date],
        years: int) -> None:
    with OpenConnection() as con:
        stocks = Table(name='stocks_daily', buffer_size=5000, con=con)
        cur = con.cursor(dictionary=True)
        pb = ProgressBar()
        pb.start(len(tickers))
        for ticker in tickers:
            data = historical_data(ticker=ticker, todate=todate, years=years)
            data['ticker'] = ticker
            data = data.rename(columns={'date': 'trade_date'})

            stocks.write_df(data, cur)
            stocks.flush(cur)
            con.commit()

            pb.measure()
            print('\r' + ticker + ': ' + pb.message(), end='')
        print()


def load_historical_dividents(tickers: List[str]) -> None:
    with OpenConnection() as con:
        div = Table(name='stocks_dividents', buffer_size=1000, con=con)
        cur = con.cursor(dictionary=True)
        pb = ProgressBar()
        pb.start(len(tickers))
        for ticker in tickers:
            data = historical_dividents(ticker=ticker)
            data['ticker'] = ticker
            data = data.rename(columns={'paymentDate': 'payment_date',
                                        'recordDate': 'record_date',
                                        'declarationDate': 'declaration_date',
                                        'exOrEffDate': 'ex_eff_date'})
            div.write_df(data, cur)
            div.flush(cur)
            con.commit()

            pb.measure()
            print('\r' + ticker + ': ' + pb.message(), end='')
        print()


def fetch_daily_data(
        tickers: List[str]) -> List[StockData]:
    pb = ProgressBar()
    pb.start(len(tickers))
    data: List[StockData] = []

    for ticker in tickers:
        info = stock_data(ticker)
        info['ticker'] = ticker
        info['date'] = dt.datetime.now().date()
        data.append(info)
        pb.measure()
        print('\r' + pb.message(), end='')
    print()

    return data


def write_daily_data(data: List[StockData]) -> None:
    df = pd.DataFrame(data)
    df.to_csv('outputs/daily_data.csv', index=False)


if __name__ == '__main__':
    with OpenConnection() as con:
        cur = con.cursor()
        cur.execute('select ticker from nasdaq')
        tickers: List[str] = [cast(str, ticker)
                              for (ticker,) in cur.fetchall()]

    data = fetch_daily_data(tickers)
    write_daily_data(data)

    # with OpenConnection() as con:
    #     cur = con.cursor()
    #     cur.execute("""select ticker from nasdaq
    #                     where ticker not in
    #                     (select distinct ticker from stocks_daily)
    #                     order by market_cap desc limit 2000;""")
    #     stock_tickers = cur.fetchall()
    #     cur.execute("""select ticker from nasdaq
    #                     where ticker not in
    #                     (select distinct ticker from stocks_dividents)
    #                     order by market_cap desc limit 2000;""")
    #     div_tickers = cur.fetchall()
    # tickers = [t for (t,) in stock_tickers]
    # load_historical_data(tickers=tickers[:], todate=None, years=5)
    #    tickers = [t for (t,) in div_tickers]
    #    load_historical_dividents(tickers=tickers[:])
