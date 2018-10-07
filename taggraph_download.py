# -*- coding: utf-8 -*-
"""
Created on Thu Oct  4 11:32:15 2018

@author: Asus
"""

import json
import mysql.connector
import pandas as pd

global_settings = """
{"ssl_dir":"d:/Documents/Certs/", 
    "root_dir":"z:/sec/", 
    "host":"server",
    "select_limit":" ",
    "years":[2013, 2017],
    "output_dir": "outputs/"
}
"""

class Settings(object):
    def __open():
        try:
            settings = json.loads(global_settings)
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
    
    def select_limit():
        return Settings.__open()["select_limit"]
    
    def years():
        return Settings.__open()["years"]
    
    def output_dir():
        return Settings.__open()["output_dir"]

def OpenConnection(host=Settings.host()):
    hosts = {"server":"server", "remote":"95.31.1.243","localhost":"localhost"}
    
    return mysql.connector.connect(user="app", password="Burkina!7faso", 
                              host=hosts[host], database="reports",
                              ssl_ca = Settings.ssl_dir()+"ca.pem",
                              ssl_cert = Settings.ssl_dir()+"client-cert.pem",
                              ssl_key = Settings.ssl_dir()+"client-key.pem",
                              connection_timeout = 1000)
    
def download_taggraph(buffer_size=100000, file_format='xlsx'):
    try:
        con = OpenConnection()
        cur = con.cursor(dictionary=True)
        
        print('execute select...', end='')
        cur.execute('select * from taggraph')
        print('ok')
        
        
        rows = cur.fetchmany(buffer_size)
        rows_fetched = len(rows)
        while len(rows) > 0:
            print('\rprocessed with {0}...'.format(rows_fetched), end='')
            df = pd.DataFrame(rows).set_index('id')
            print('write...', end = '')
            filename = Settings.output_dir() + 'taggraph_{0}.{1}'.format(int(rows_fetched/buffer_size), file_format)
            if file_format == 'xlsx':
                df.to_excel(filename)
            if file_format == 'csv':
                df.to_csv(filename)
            
            rows = cur.fetchmany(buffer_size)
            rows_fetched += len(rows)
        print('ok')
    finally:
        con.close()
        
download_taggraph()
            