import unittest
import datetime

from firms.futils import stock_type, date_from_timestamp


class TestUtils(unittest.TestCase):
    def test_stock_type(self):
        s = stock_type(
            'American Depositary Shares (Each representing 1 Common Share)')
        self.assertEqual(s, 'share.adr.com')

    def test_date_from_timestamp(self):
        self.assertEqual(
            date_from_timestamp(
                'DATA AS OF Oct 31, 2019 5:57 AM ET - PRE-MARKET'),
            datetime.date(2019, 10, 31))
        self.assertEqual(
            date_from_timestamp(
                'CLOSED AT 4:00 PM ET ON Oct 30, 2019'),
            datetime.date(2019, 10, 30))
        self.assertEqual(
            date_from_timestamp(
                'CLOSED AT 4:00 PM ET ON Oct 1, 2019'),
            datetime.date(2019, 10, 1))


if __name__ == '__main__':
    unittest.main()
