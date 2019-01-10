# -*- coding: utf-8 -*-
"""
Created on Tue Dec  4 09:03:56 2018

@author: Asus
"""

import keras
import os
from time import sleep

def dosmth():
    for i in range(100):
        print(os.name)
        sleep(0.1)

dosmth()