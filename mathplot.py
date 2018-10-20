# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 20:04:22 2018

@author: Asus
"""

import matplotlib.pyplot as plt
import numpy as np

x = np.random.randn(10000)
plt.hist(x, 100)
plt.title(r'Normal distribution with $\mu=0, \sigma=1$')
plt.savefig('matplotlib_histogram.png')
plt.show()
import numpy as np
import pandas as pd
a = pd.DataFrame(np.random.randn(30), columns=['foo'])
b = pd.DataFrame(np.random.randn(30), columns=['bar'])  
eg = a[a['foo'] >= b['bar']]