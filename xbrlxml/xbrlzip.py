# -*- coding: utf-8 -*-

import zipfile
from typing import Dict, Tuple

from xbrlxml.xbrlexceptions import XbrlException

class XBRLZipPacket(object):
    def __init__(self):
        self.zip_filename = None
        
        self.files = {'xbrl': None,
                      'xsd': None, 
                      'pre': None,
                      'lab': None,
                      'cal': None,
                      'def': None}        
        
    def open_packet(self, zip_filename) -> None:
        self.__init__()
        self.zip_filename = zip_filename
                
        try:
            z_file = zipfile.ZipFile(self.zip_filename)
        except:
            raise XbrlException('bad zip file')
            
        
        files = z_file.namelist()
        files = [f for f in files 
                     if f.endswith('.xml') or f.endswith('.xsd')]
        
        for name in files:
            low = name.lower()
            if low.endswith('.xsd'):
                self.files['xsd'] = name
            elif low.endswith('cal.xml'):
                self.files['cal'] = name
            elif low.endswith('pre.xml'):
                self.files['pre'] = name
            elif low.endswith('lab.xml'):
                self.files['lab'] = name
            elif low.endswith('def.xml'):
                self.files['def'] = name                    
            elif (not low.endswith('def.xml') and
                  not low.endswith('ref.xml')):
                self.files['xbrl'] = name
                
        z_file.close()
        
    def save_packet(self, 
                    files: Dict[str, Tuple[str, bytes]], 
                    zip_filename: str) -> None:
        """
        store files into zip_filename archive
        """
        try:
            with zipfile.ZipFile(zip_filename, mode='w') as zip_file:
                for type_, (filename, data) in files.items():
                    self.files[type_] = filename
                    zip_file.writestr(filename, data)
        except OSError as e:
            raise XbrlException('problem with saving files to {}\n'.format(
                    zip_filename) + str(e))
            
    def extract_to(self, temp_dir: str) -> None:
        try:
            with zipfile.ZipFile(self.zip_filename) as zfile:
                for name in self.files.values():
                    if name is None:
                        continue
                    zfile.extract(name, temp_dir)
        except OSError as e:
            XbrlException('problem with extracting files to {}\n'.format(
                    temp_dir) + str(e))
    
    def getfile(self, filetype):
        assert filetype in self.files
        
        if self.files[filetype] is None:
            return None
        z_file = zipfile.ZipFile(self.zip_filename)
        
        return z_file.open(self.files[filetype])
    
    @property
    def xbrl(self):
        return self.getfile('xbrl')
    @property
    def xsd(self):
        return self.getfile('xsd')
    @property
    def calc(self):
        return self.getfile('cal')
    @property
    def defi(self):
        return self.getfile('def')
    @property
    def pres(self):
        return self.getfile('pre')
    @property
    def labl(self):
        return self.getfile('lab')