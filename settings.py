# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 15:30:31 2018

@author: Media
"""

import json

class Settings(object):
    def __open():
        try:
            f = open("global.settings")
            settings = json.loads(f.read())
        except:
            print("create settings file")
            raise
        return settings
    
    def ssl_dir():        
        return Settings.__open()["ssl_dir"]
    
    def root_dir():
        return Settings.__open()["root_dir"]
    
    def host():
        return Settings.__open()["host"]
            