# -*- coding: utf-8 -*-
"""
Created on Fri May 25 16:52:04 2018

@author: Asus
"""

import os
import traceback
import datetime as dt
import io
import warnings
import sys

class LogFile(object):
    def __init__(self, filename=None, append=True, timestamp=True):
        self.log_file = None
        if filename is not None:
            if os.path.exists(filename) and append:
                self.log_file = open(filename, "a")
            else:
                self.log_file = open(filename, "w")

        if timestamp and self.log_file is not None:
            self.write("session timestamp {0}".format(dt.datetime.now()))

    def write(self, info, end='\n'):
        self.writemany(info=info, end = end)
            
    def writemany(self, *args, info, end='\n'):
        f = '\t'.join(['{'+str(i)+'}' for i in range(len(args))])
        if f == '':
            f = '{' + str(len(args)) + '}'
        else:
            f += '\t{' + str(len(args)) + '}' 
        info = info.replace('\r', '')
        for line in info.split('\n'):
            if self.log_file:
                self.log_file.write(f.format( *(args + (line,)) ) + end)
                #self.log_file.write(end)
                self.log_file.flush()
            else:
                print(f.format( *(args + (line,)) ), end=end)
    
    def tb2str(excinfo):
        strio = io.StringIO()
        strio.write(str(excinfo[0]) + '\n')
        strio.write(str(excinfo[1]) + '\n')
        traceback.print_tb(excinfo[2], file=strio)
        strio.flush()
        strio.seek(0)
        return strio.read()
    
    def writetb(self, *labels, excinfo=sys.exc_info()):
        self.writemany(*labels, info = LogFile.tb2str(excinfo))
        
    def close(self):
        if self.log_file is not None:
            self.log_file.close()
            del self.log_file
            self.log_file = None
        
#remove after correction

    def write2(self, label, info):
        self.writemany(label, info=info)
        warnings.warn('function LogFile::write2() will be removed, use writemany()')


    def write_tb(self, excinfo):
        self.writemany(info = LogFile.tb2str(excinfo))
        warnings.warn('function LogFile::write_tb() will be removed, use writetb()')
    
    
    def write_tb2(self, label, excinfo):
        self.writemany(label, info = LogFile.tb2str(excinfo))
        warnings.warn('function LogFile::write_tb2() will be removed, use writetb()')

if __name__ == '__main__':
    
    log = LogFile('outputs/test.log')
    try:
        a = 1/0
    except:
        log.writetb('a', 'b', excinfo=sys.exc_info())
        print('end')
