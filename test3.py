# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 18:56:25 2019

@author: Asus
"""
from xbrlxml.xbrlchapter import ReferenceParser
import algos.xbrljson as xjson
import json
from utils import add_app_dir
    
if __name__ == '__main__':    
    parser = ReferenceParser('calculation')
    chapters = parser.parse(add_app_dir('test/xom-20181231_cal.xml'))
    chapter = chapters['http://www.exxonmobil.com/role/StatementConsolidatedBalanceSheet']
    s = json.dumps(chapter, cls=xjson.ForTestJsonEncoder)