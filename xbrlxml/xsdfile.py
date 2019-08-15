# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 14:05:20 2019

@author: Asus
"""

import re
import lxml # type: ignore
from collections import namedtuple

XSDChapter = namedtuple('XSDChapter', 
                                ['roleuri','label','sect','id'])

class XSDFile(object):    
    def read(self, xsd_file):        
        chapters = {}
        '''
        doc - document entry - matchDocument
        sta - financial statements - matchStatement
        not - notes to financial statements - all others
        pol - accounting policies - matchPolicy
        tab - notes tables - matchTable
        det - notes details - matchDetail
        '''
        matchStatement = re.compile('.* +\- +Statement +\- .*')
        matchDisclosure = re.compile('.* +\- +Disclosure +\- +.*')
        matchDocument = re.compile('.* +\- +Document +\- +.*')
        matchParenthetical = re.compile('.*\-.+-.*Paren.+')
        matchPolicy = re.compile('.*\(.*Polic.*\).*')
        matchTable = re.compile('.*\(Table.*\).*')
        matchDetail = re.compile('.*\(Detail.*\).*')

        root = lxml.etree.parse(xsd_file)            

        for role in root.iter('{*}roleType'):
            rol_def = role.find("{*}definition")
            
            sect = ""
            if matchDocument.match(rol_def.text):
                sect = "doc"
            elif matchStatement.match(rol_def.text):
                if matchParenthetical.match(rol_def.text):
                    sect = "sta"
                else:
                    sect = "sta"
            elif matchDisclosure.match(rol_def.text):
                if matchPolicy.match(rol_def.text):
                    sect = "pol"
                elif matchTable.match(rol_def.text):
                    sect = "tab"
                elif matchDetail.match(rol_def.text):
                    sect = "det"
                else:
                    sect = "not"
            roleuri = role.attrib["roleURI"]
            
            label = re.sub('\d+\s*-\s*\w*\s*-\s*', '', 
                           rol_def.text.strip(), flags=re.I)
            chapters[roleuri] = XSDChapter(roleuri, label, sect,
                                            role.attrib["id"])        
        return chapters
        
if __name__ == '__main__':
    xsd = XSDFile()
    chapters = xsd.read('../test/aal-20181231.xsd')
    