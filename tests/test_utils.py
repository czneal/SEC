# -*- coding: utf-8 -*-

import unittest

import utils
import datetime
class TestStr2Date(unittest.TestCase):
    def test_str2date(self):
        datestrs = ['2012-12-20', '2012/12/20', '2012-12-200sdf']
        dates = [datetime.date(2012, 12, 20), datetime.date(2012, 12, 20), datetime.date(2012, 12, 20)]
        for datestr, date in zip(datestrs,dates):
            self.assertEqual(utils.str2date(datestr, 'ymd'), date)
            
    def test_calculate_fy_fye(self):
        dates = [datetime.date(2012, 12, 20), 
                 datetime.date(2015, 2, 27), 
                 datetime.date(2020, 7, 1)]
        answers = [(2012, '1220'), (2014, '0227'), (2020, '0701')]
        for d, (fy_a, fye_a) in zip(dates, answers):
            fy, fye = utils.calculate_fy_fye(d)
            with self.subTest(i=d):
                self.assertEqual(fy, fy_a)
                self.assertEqual(fye, fye_a)
if __name__ == "__main__":
    unittest.main()
