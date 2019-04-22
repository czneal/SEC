# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 12:15:38 2019

@author: Asus
"""

from lxml import etree
tree = etree.parse("test/bkrs-20120128_cal.xml")
root = tree.getroot()
print(root.nsmap)

