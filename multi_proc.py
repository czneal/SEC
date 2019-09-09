# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 10:07:08 2019

@author: Asus
"""

from multiprocessing import Process, Lock

def func(l, i):
    l.acquire()
    try:        
        print(i)
    finally:
        l.release()
        
    return 1

if __name__ == '__main__':
    lock = Lock()
    with open('outputs/multi.txt', 'w') as file:
        file.write('begins\n')
        
    for num in range(10):
        p = Process(target=func, args=(lock, num))
        p.start()
        print(p.join())
    