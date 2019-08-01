# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 16:42:10 2019

@author: Asus
"""

import pandas as pd
from utils import ProgressBar
from xbrl_file import Chapter
from xbrl_file import XBRLZipPacket
from xbrl_parse import FilingRSS
from log_file import LogFile
import os
import sys
import lxml
import classificators as cl
import re
from xml_tools import XmlTreeTools

class XSDFile(object):
    def __init__(self, log):
        self.log = log

    def read(self, xsd_file):
        try:
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

            tools = XmlTreeTools()
            tools.read_xml_tree(xsd_file)
            root = tools.root
            link = tools.link

            for role in root.iter(link+"roleType"):
                rol_def = role.find(link+"definition")
                chapter = ""
                if matchDocument.match(rol_def.text):
                    chapter = "doc"
                elif matchStatement.match(rol_def.text):
                    if matchParenthetical.match(rol_def.text):
                        chapter = "sta"
                    else:
                        chapter = "sta"
                elif matchDisclosure.match(rol_def.text):
                    if matchPolicy.match(rol_def.text):
                        chapter = "pol"
                    elif matchTable.match(rol_def.text):
                        chapter = "tab"
                    elif matchDetail.match(rol_def.text):
                        chapter = "det"
                    else:
                        chapter = "not"
                role_uri = role.attrib["roleURI"]                
                label = re.sub('\d+\s*-\s*Statement\s*-\s*', '', 
                               rol_def.text.strip(), flags=re.I)
                chapters[role_uri] = Chapter(role_uri, role.attrib["id"], chapter, label)
            
            return chapters
        except:
            return None

if __name__ == '__main__':
    err = LogFile()
    log = LogFile('outputs/chapters.log')
    xsd = XSDFile(log=log)
    ms = cl.MainSheets()
    pb = ProgressBar()
    
    data = []
    count = 100
    rss = FilingRSS()
    root_dir = 'z:/sec/2019/03/'
    rss.open_file(root_dir + 'rss-2019-03.xml')
    records = rss.filing_records()
    pb.start(len(records))
    for record in records:        
        try:
            zippacket = XBRLZipPacket(root_dir + 
                                      str(record['cik']).zfill(10) + 
                                      '-' + record['adsh'] + '.zip')
            zippacket.open_packet()
            
            chapters = xsd.read(zippacket.xsd_file)
            labels = [chapter.label for (_, chapter) in chapters.items() if chapter.chapter == 'sta']
            for label in labels:
                data.append([record['cik'], record['adsh'], label])
            labels = ms.select_ms(labels)
            if len(labels) != 3:
                for sheet, label in labels.items():
                    log.writemany(record['cik'], record['adsh'], sheet, info=label)
            
        except:
           err.writetb(record['cik'], record['adsh'], excinfo = sys.exc_info())
           
        pb.measure()
        print('\r' + pb.message(), end='')
    print()
               
        
    

