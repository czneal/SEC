# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:58:11 2019

@author: Asus
"""

import datetime as dt
import calendar

class ProgressBar(object):
    def __init__(self):
        self.total = 0
        self.one_step = dt.timedelta()
        self.n = 0
        self.before = dt.datetime.now()
        
    def start(self, total):
        self.__init__()
        self.total = total
                
    def measure(self):
        delta = dt.datetime.now() - self.before
        self.before = dt.datetime.now()
        self.one_step = (self.one_step*self.n + delta)/(self.n+1)
        self.n += 1
        
    def message(self):
        s2 = str(self.one_step*(self.total-self.n)).split('.')[0]
        s3 = str(self.one_step).split('.')[0]
        return 'processed {0} of {1}, time remains: {2}, time per step: {3}'.format(
                self.n, self.total, s2, s3)
        
        
def correct_date(y, m, d):
    (_, last) = calendar.monthrange(y, m)
    if d > last: d = last
    return dt.date(y,m,d)

def str2date(datestr):
    try:
        datestr = datestr.replace('-','').replace('/','')
        return dt.date(int(datestr[0:4]), 
                       int(datestr[4:6]), 
                       int(datestr[6:8]))
    except:
        return None
