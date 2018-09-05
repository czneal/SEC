# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 18:41:08 2018

@author: Asus
"""

import xml.etree.ElementTree as ET
import numpy as np

#root = ET.parse("ipython.html")
#item = root.find("body")

m = 0.5
X = np.array([1,0,1,0,0,1,0,0,0,0,0,1,0,1,0,1,0,0,1,0])
n = X.shape[0]
print(np.sum(X), n)

X_mean = np.mean(X)

sx = np.sum(np.square(X-X_mean))/(n-1)
sx = np.sqrt(sx)

t = (X_mean - m)/(sx/np.sqrt(n))

print(t)
