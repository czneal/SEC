import unittest

import mysqlio.stocksio as sio
import mysqlio.basicio as do

from mysqlio.tests.dbtest import DBTestBase  # type: ignore


class TestStocksIO(DBTestBase):
    stock_data_list = [{'ticker': 'AAPL',
                        'trade_date': '2019-10-31',
                        'market_cap': 1124191216800,
                        'shares': 4519180000,
                        'ttype': 'stock.com'},
                       {'ticker': 'AAPL',
                        'trade_date': '2019-11-01',
                        'market_cap': 1156096627600,
                        'shares': 4519180000,
                        'ttype': 'stock.com'},
                       {'ticker': 'AAPL',
                        'trade_date': '2019-11-11',
                        'market_cap': 1180842106400,
                        'shares': 4443265001,
                        'ttype': 'stock.com'},
                       {'ticker': 'AAPL',
                        'trade_date': '2019-11-11',
                        'market_cap': 1180842106400,
                        'shares': 4443265000,
                        'ttype': 'stock.com'}]

    def test_write_stocks_shares(self):
        with do.OpenConnection() as con:
            table = do.MySQLTable(table_name='stocks_shares', con=con)
            cur = con.cursor(dictionary=True)
            cur.execute("""delete from stocks_shares where ticker='aapl'
                           and trade_date between '2019-10-31' and '2019-11-11'""")

            # insert into empty table
            sio.write_stocks_shares(self.stock_data_list[0], cur, table)
            cur.execute(
                "select * from stocks_shares where ticker='aapl' and trade_date='2019-10-31'")
            rows = cur.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['shares'], 4519180000)

            # insert into nonempty with same shares
            sio.write_stocks_shares(self.stock_data_list[1], cur, table)
            cur.execute(
                "select * from stocks_shares where ticker='aapl' and trade_date='2019-11-01'")
            rows = cur.fetchall()
            self.assertEqual(len(rows), 0)

            # insert into nonempty with new shares
            sio.write_stocks_shares(self.stock_data_list[2], cur, table)
            cur.execute(
                "select * from stocks_shares where ticker='aapl' and trade_date='2019-11-11'")
            rows = cur.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['shares'], 4443265001)

            # update shares
            sio.write_stocks_shares(self.stock_data_list[3], cur, table)
            cur.execute(
                "select * from stocks_shares where ticker='aapl' and trade_date='2019-11-11'")
            rows = cur.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['shares'], 4443265000)

            con.rollback()


if __name__ == '__main__':
    unittest.main()
