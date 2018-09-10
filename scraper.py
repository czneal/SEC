# -*- coding: utf-8 -*-
"""
Created on Fri Jul 20 17:59:12 2018

@author: Media
"""
import download as dd
import xbrl_scan

print("start download...", end="")
dd.update_current_month()
print("ok")
print("start getting mgnums...", end="")
xbrl_scan.update_current_month()
print("ok")