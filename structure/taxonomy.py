# -*- coding: utf-8 -*-
"""
Created on Fri May  3 11:01:42 2019

@author: Asus
"""
import re
import os
import zipfile
import pandas as pd
import lxml

from settings import Settings
import xbrl_file_v2 as xf
import xbrl_chapter as chapter
import urltools

class Taxonomy(object):
    gaap_link = "http://xbrl.fasb.org/us-gaap/"
    def __init__(self, gaap_id):
        self.gaap_dir = Settings.root_dir() + 'us-gaap/'
        self.gaap_file = 'us-gaap-' + gaap_id + '.zip'
        self.gaap_id = gaap_id
        self.taxonomy = None
    
    def read(self):
        if not self.__download():
            return False
        try:
            xsd = SchemeXSD()
            rep = xf.XBRLFile()
            data = []
            with zipfile.ZipFile(self.gaap_dir + self.gaap_file) as zfile:
                for xsd_filename in [f for f in zfile.namelist() if f.find('/stm/')>=0 and f.endswith('xsd')]:
                    xsd.read(zfile.open(xsd_filename))
                    for row in xsd.asdict():            
                        cal_file = zfile.open(os.path.dirname(xsd_filename) +'/'+ row['cal_filename'])
                        rep.chapters = {}
                        for ch in getchapters(cal_file):
                            cal_file = zfile.open(os.path.dirname(xsd_filename) +'/'+ row['cal_filename'])
                            rep.chapters[ch['role']] = chapter.Chapter(ch['role'], ch['id'], 'sta', row['doc'])
                            rep.read_cal(cal_file, rep.chapters)
                            row['structure'] = rep.structure_dumps()
                            data.append(row)
            self.taxonomy = pd.DataFrame(data)
        except:
            return False
        
        return True        
    
    def __download(self):        
        if not os.path.exists(self.gaap_dir):
            os.mkdir(self.gaap_dir)
                
        if not os.path.exists(self.gaap_dir + self.gaap_file):
            if not urltools.fetch_urlfile(self.gaap_link + self.gaap_id[0:4] + '/' 
                                          + self.gaap_file, 
                                          self.gaap_dir + self.gaap_file):
                return False
            
        return True
            
class SchemeXSD(object):
    def __init__(self):
        self.cal_filenames = []
        self.doc = []
        self.roles = []
        
    def read(self, filename):
        self.__init__()
        
        etree = lxml.etree.parse(filename)
        root = etree.getroot()
        
        xlink = root.nsmap['xlink']
        for link in list(root.iter('{*}linkbaseRef')):
            href = link.attrib.get('{%s}href' % xlink)
            role = link.attrib.get('{%s}role' % xlink)            
            if role is None:
                continue
            
            if role.split('/')[-1].lower() == 'calculationLinkbaseRef'.lower():
                self.cal_filenames.append(href)                
                
        doc = root.find('.//xs:documentation', root.nsmap)
        if doc is None:
            return
        ms = re.compile('.* +\- +Statement +\- .*(calculation).*')
        doc = [d.strip() for d in doc.text.strip().split('\n') if ms.match(d)]
        if len(doc) > 0:
            self.doc = doc
            
    def aslist(self):
        return [(f.split('-')[3], f.split('-')[4], f, doc) for (f, doc) in zip(self.cal_filenames, self.doc)]
    
    def asdict(self):
        return [{'sheet':f.split('-')[-6], 
                 'type':f.split('-')[-5], 
                 'cal_filename':f, 
                 'doc':doc} for (f, doc) in zip(self.cal_filenames, self.doc)]
    
def getchapters(file):
    chapters = []
    try:
        etree = lxml.etree.parse(file)
        root = etree.getroot()
        for c_link in root.iter('{*}calculationLink'):
            role = c_link.attrib.get('{%s}role' % root.nsmap['xlink'])
            if role is not None:
                chapters.append({'role':role, 'id':role.split('/')[-1]})
    except:
        return None
    
    return chapters