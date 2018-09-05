# -*- coding: utf-8 -*-
"""
Created on Fri May 25 16:52:04 2018

@author: Asus
"""

import os
import traceback
import datetime as dt

class LogFile(object):
    def __init__(self, filename=None, append=True):
        self.log_file = None
        if filename is not None:        
            if os.path.exists(filename) and append:
                self.log_file = open(filename,"a")            
            else:
                self.log_file = open(filename, "w")
            
        self.write("session timestamp {0}".format(dt.date.today()))
        
    def write(self, info, end=os.linesep):
        if self.log_file is None:
            print(info, end = end)
        else:
            self.log_file.write(str(info)+end)
            self.log_file.flush()
            
    def close(self):
        if self.log_file is not None:
            self.log_file.close()
            
    def write_tb(self, tb):        
        traceback.print_tb(tb, file=self.log_file)        
        