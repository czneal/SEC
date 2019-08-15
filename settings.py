# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 15:30:31 2018

@author: Media
"""

import json
import os

class Settings(object):
    @staticmethod
    def __open():
        try:
            wd = os.path.split(__file__)[0]
            f = open(wd + '/global.settings')
            settings = json.loads(f.read())
            f.close()
        except:
            print("create settings file")
            raise
        return settings
    
    @staticmethod
    def ssl_dir():        
        return Settings.__open()["ssl_dir"]
    
    @staticmethod
    def root_dir():
        return Settings.__open()["root_dir"]
    
    @staticmethod
    def host():
        return Settings.__open()["host"]
    
    @staticmethod
    def select_limit():
        return Settings.__open()["select_limit"]
    
    @staticmethod
    def years():
        return Settings.__open()["years"]
    
    @staticmethod
    def output_dir():
        return Settings.__open()["output_dir"]
    
    @staticmethod
    def models_dir():
        return Settings.__open()["models_dir"]
    
    @staticmethod
    def app_dir():
        return Settings.__open()["app_dir"]
            