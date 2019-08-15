# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 22:20:26 2019

@author: Asus
"""

import unittest
from parameterized import parameterized_class

class A():
    pass

class B():
    pass

@parameterized_class(('cls_',), [(A, ), (B,)])
class TestClass(unittest.TestCase):
    def setUp(self):
        print(self.cls_)
        
    def test1(self):
        print('test1') #print(self.cls_)
    
    def test2(self):
        print('test2') #print(self.cls_)
        
if __name__ == '__main__':
    unittest.main()