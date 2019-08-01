# -*- coding: utf-8 -*-

import zipfile

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
        
    def open_packet(self, zip_filename):
        self.__init__()
        self.zip_filename = zip_filename
                
        try:
            z_file = zipfile.ZipFile(self.zip_filename)
        except:
            raise XbrlException('bad zip file')
            
        
        files = z_file.namelist()
        files = [f for f in files if f.endswith('.xml') or f.endswith('.xsd')]

        
        for name in files:
            if name.endswith('.xsd'):
                self.files['xsd'] = name
            elif name.endswith('cal.xml'):
                self.files['cal'] = name
            elif name.endswith('pre.xml'):
                self.files['pre'] = name
            elif name.endswith('lab.xml'):
                self.files['lab'] = name
            elif name.endswith('def.xml'):
                self.files['def'] = name                    
            elif (not name.endswith('def.xml') and
                  not name.endswith('ref.xml')):
                self.files['xbrl'] = name
                
        z_file.close()
        
    
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