# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 18:56:25 2019

@author: Asus
"""
import os
import pandas as pd

from multiprocessing import Pool, Manager, Lock
from typing import List

def read(lock: Lock, retvals: list):
    pid = os.getpid()
    df = pd.DataFrame([[pid, 1], [pid, 2]], columns=['pid', 'v'])    
    retvals.append(df)
    
    if lock is not None:
        lock.acquire()
        try:
            print(os.getpid(), 'hello')        
        finally:
            lock.release()
            
    return os.getpid()

if __name__ == '__main__':    
    cpus = os.cpu_count() - 1
    print('run {0} proceses'.format(cpus))
    
    
    with Pool(cpus) as p, Manager() as m:
        params = []
        lock = m.Lock()
        retvals = m.list()
        for i in range(cpus):
            params.append((lock, retvals))
            
        print(p.starmap(read, params))
        df = pd.concat(retvals, ignore_index=True)