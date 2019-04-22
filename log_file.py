# -*- coding: utf-8 -*-
"""
Created on Fri May 25 16:52:04 2018

@author: Asus
"""

import os
import traceback
import datetime as dt
import io

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
        if self.log_file is None:
            print(info, end = end)
        else:
            self.log_file.write(str(info)+end)
            self.log_file.flush()

    def write2(self, label, info):
        info = info.replace('\r', '')
        for line in info.split('\n'):
            self.write("{0}\t{1}".format(label, line))

    def close(self):
        if self.log_file is not None:
            self.log_file.close()
            del self.log_file
            self.log_file = None

    def write_tb(self, excinfo):
        self.write(str(excinfo[0]))
        self.write(str(excinfo[1]))
        if self.log_file:
            traceback.print_tb(excinfo[2], file=self.log_file)
        else:
            traceback.print_tb(excinfo[2])

    def write_tb2(self, label, excinfo):
        strio = io.StringIO()
        strio.write(str(excinfo[0])+os.linesep)
        strio.write(str(excinfo[1])+os.linesep)
        traceback.print_tb(excinfo[2], file=strio)
        strio.flush()
        strio.seek(0)
        self.write2(label, strio.read())
