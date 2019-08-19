# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:58:11 2019

@author: Asus
"""

import datetime as dt
import calendar
import re
import os
from lxml import etree #type: ignore

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

def periodend(fy, m, d):
    if m <= 6: 
        return correct_date(fy + 1, m, d) 
    else:
        return correct_date(fy, m, d)
    
def calculate_fy_fye(period: dt.date):
    fye = str(period.month).zfill(2) + str(period.day).zfill(2)
    if period.month > 6:
        fy = period.year
    else:
        fy = period.year - 1
    
    return(fy, fye)

def str2date(datestr, pattern='ymd'):
    if isinstance(datestr, dt.date):
        return datestr
    if isinstance(datestr, dt.datetime):
        return datestr
    if datestr is None:
        return None
    
    assert isinstance(datestr, str)
    patterns = [re.compile('.*\d{4}-\d{2}-\d{2}.*'),
                re.compile('.*\d{4}/\d{2}/\d{2}.*'),
                re.compile('.*\d{8}.*'),
                re.compile('.*\d{2}/\d{2}/\d{4}.*')]
    assert sum([p.match(datestr) is not None for p in patterns]) > 0
    assert pattern in {'ymd', 'mdy'}
    
    retval = None
    try:
        for p in patterns:
            dd = p.search(datestr)
            if dd is not None:
                break
        
        datestr = dd.group(0).replace('-','').replace('/','')
        if pattern == 'ymd':
            retval = dt.date(int(datestr[0:4]), 
                           int(datestr[4:6]), 
                           int(datestr[6:8]))
        if pattern == 'mdy':
            retval = dt.date(int(datestr[4:8]), 
                           int(datestr[0:2]), 
                           int(datestr[2:4]))
    except:        
        pass
    
    return retval

def opensmallxmlfile(file):
    if file is None:
        return None
    
    root = None
    try:
        root = etree.parse(file).getroot()
    except:
        file.close()
        
    return root

def openbigxmlfile(file):
    if file is None:
        return None
            
    root = None
    try:
        #xmlparser = etree.XMLParser(recover=True)
        xmlparser = etree.XMLParser(huge_tree =True)
        tree = etree.parse(file, parser=xmlparser)
        root = tree.getroot()
    except:
        file.close()
        
    return root

def class_for_name(module_name, class_name):
    # load the module, will raise ImportError if module cannot be loaded
    m = __import__(module_name, globals(), locals(), class_name)
    # get the class, will raise AttributeError if class cannot be found
    c = getattr(m, class_name)
    return c

def retry(retry, exception_class):
    def decorator(function):
        def wrapper(*args, **kwargs):
            for i in range(retry):
                try:
                    return function(*args, **kwargs)            
                except exception_class as e:
                    if i + 1 == retry:
                        print('done {} tryouts'.format(retry))
                        raise e            
        return wrapper
    return decorator

def clear_dir(target_dir):
    for [root, dirs, filenames] in os.walk(target_dir):
        for filename in filenames:
            os.remove(os.path.join(root, filename))

if __name__ == '__main__':
    clear_dir('z:/sec/tmp')