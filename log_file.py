# -*- coding: utf-8 -*-
"""
Created on Fri May 25 16:52:04 2018

@author: Asus
"""

import os
import traceback
import datetime as dt
import io
import sys
import json

from algos.xbrljson import ForDBJsonEncoder

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
            
class Logs():
    def __init__(self, log_dir: str, append_log=False, name='log'):
        self.__err = LogFile(log_dir + name + '.err', append_log)
        self.__log = LogFile(log_dir + name + '.log', append_log)
        self.__warn = LogFile(log_dir + name + '.warn', append_log)
        self.header = []
        
    def set_header(self, header):
        self.header = [h for h in header]
        
    def warning(self, message):
        self.__warn.writemany(*(self.header), info = str(message))
    
    def error(self, message):
        self.__err.writemany(*(self.header), info = str(message))
    def traceback(self):
        self.__err.writetb(*(self.header), excinfo = sys.exc_info())
    
    def log(self, message):
        self.__log.writemany(*(self.header), info = str(message))
        
    def close(self):
        self.__err.close()
        self.__log.close()
        self.__warn.close()
        
class RepeatFile():
    def __init__(self, filename):
        self.__file = open(filename, 'w')
        self.record = None
        self.zip_filename = None
        
    def set_state(self, record, zip_filename):
        self.record = json.dumps(record, 
                                 cls=ForDBJsonEncoder)
        self.zip_filename = zip_filename
        
    def repeat(self):
        self.__file.write('{0}\t{1}\n'.format(self.record,
                                              self.zip_filename))
        
    def close(self):
        self.__file.close()
        
if __name__ == '__main__':
    
    log = LogFile('outputs/test.log')
    try:
        a = 1/0
    except:
        log.writetb('a', 'b', excinfo=sys.exc_info())
        print('end')
