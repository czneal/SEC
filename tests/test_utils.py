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
                
class TestPathFuncs(unittest.TestCase):
    def test_remove_common_path(self):
        cases = [['c:/docs/', 'c:/docs/victor/home', 'victor/home'],
                 ['c:/docs', 'c:/doCs/victor/home', 'victor/home'],
                 ['c:/docs', 'c:/do/victor/home', 'c:/do/victor/home'],
                 ['c:/doc', 'c:/doCs/victor/home', 'c:/docs/victor/home'],
                 ['c:/doc', '/victor/home', '/victor/home'],
                 ['c:\\doc', 'victor\\home', 'victor/home'],
                 ['c:\\docs\\', 'c:\\docs/victor\\home', 'victor/home'],
                 ['c:/docs/file.py', 'c:/docs/victor/home', 'victor/home']]
        for case in cases:
            self.assertEqual(utils.remove_common_path(case[0], case[1]),
                            case[2])

if __name__ == "__main__":
    unittest.main()
