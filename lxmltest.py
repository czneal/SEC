# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 11:18:35 2019

@author: Asus
"""

from lxml import etree

def make_big_xml_file(filename: str, children: int) -> None:
    root = etree.Element('root')
    
    for i in range(1,children):
        child = etree.SubElement(root, 'child', id=str(i), somedata='Some usefull string')
        child.text = "Some big text".join(' ' for i in range(100))
        
    with open(filename, 'wb') as f:
        f.write(etree.tostring(root, pretty_print=True))

if __name__ == '__main__':
    make_big_xml_file('test1.xml', 10000)
    make_big_xml_file('test2.xml', 10000)
    
    for i in range(60000):
        if i%100 == 0:
            print(i)
        
        if i%2 == 0:
            filename = 'test1.xml'
        else:
            filename = 'test2.xml'
        
        with open(filename,'rb') as f:
            root = etree.parse(f).getroot()
            root.clear()
            del root