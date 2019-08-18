# -*- coding: utf-8 -*-
import os
from typing import Dict, Tuple
from bs4 import BeautifulSoup
import lxml.etree
import xml.etree.ElementTree as ET

import xbrlxml.xbrlexceptions
from xbrlxml.xbrlzip import XBRLZipPacket
from urltools import fetch_urlfile
from utils import clear_dir
from settings import Settings

def download_files_from_sec(
        cik: int, adsh: str) -> Dict[str, Tuple[str, bytes]]:
    adsh_sep = adsh
    adsh = adsh_sep.replace('-', '')
    url = ('https://www.sec.gov/Archives/edgar/data/' + 
           '{0}/{1}/{2}-index.htm'.format(cik, adsh, adsh_sep))
    soup = BeautifulSoup(fetch_urlfile(url), 'lxml')

    files = {}
    for node in soup.find_all('a'):
        link = node.get('href')
        lower = link.lower()
        if lower.endswith('.xsd'):
            files['xsd'] = link
            continue
        if lower.endswith('cal.xml'):
            files['cal'] = link
            continue
        if lower.endswith('def.xml'):
            files['def'] = link
            continue
        if lower.endswith('lab.xml'):
            files['lab'] = link
            continue
        if lower.endswith('pre.xml'):
            files['pre'] = link
            continue
        if lower.endswith('ref.xml'):
            continue
        if lower.endswith('.xml'):
            files['xbrl']= link

    data = {}
    for type_, link in files.items():
        filename = link.split('/')[-1]
        byteio = bytes(fetch_urlfile('https://www.sec.gov' + link).read())
        data[type_] = [filename, byteio]
    
    return data

def download_enclosure(cik: int, adsh: str, zip_filename: str) -> None:
    adsh_sep = adsh
    adsh = adsh_sep.replace('-', '')
    url = ('https://www.sec.gov/Archives/edgar/data/' + 
           '{0}/{1}/{2}-xbrl.zip'.format(cik, adsh, adsh_sep))
    
    fetch_urlfile(url, zip_filename)
    
def check_zip_file(filename: str) -> None:
    """
    if zip file is Ok return None
    else raise XbrlException with message
    """
    "unittested"
    
    if not os.path.exists(filename):
        raise xbrlxml.xbrlexceptions.XbrlException('zip file doesnt exist')
        
    packet = XBRLZipPacket()
    packet.open_packet(filename)
    messages = []
    if packet.files['xbrl'] is None:
        messages.append('missing XBRL file')
    if packet.files['pre'] is None:
        messages.append('missing presentation file')
    if packet.files['xsd'] is None:
        messages.append('missing xsd file')
    
    if messages:
        raise xbrlxml.xbrlexceptions.XbrlException(
                '\n'.join(messages))
        
def check_zip_file_deep(filename: str) -> None:
    if not os.path.exists(filename):
        raise xbrlxml.xbrlexceptions.XbrlException('zip file doesnt exist')
    
    tmp_dir = os.path.join(Settings.root_dir(), 'tmp/')
    
    packet = XBRLZipPacket()
    packet.open_packet(filename)
    packet.extract_to(tmp_dir)
    messages = []
    
    for type_, filename in packet.files.items():
        try:
            if filename is None:
                continue
            
            lxml.etree.parse(
                    os.path.join(tmp_dir, filename))
#            ET.parse(packet.getfile(type_))
        except lxml.etree.ParseError as e:
            messages.append(str(e))
        except ET.ParseError as e:
            messages.append(str(e))
    try:
        clear_dir(tmp_dir)
    except OSError as e:
        messages.append(str(e))
        
    if messages:
        raise xbrlxml.xbrlexceptions.XbrlException(
                '\n'.join(messages))

def year_month_dir(year: int, month: int) -> None:
    path = Settings.root_dir()
    return (path + 
            '{0}/{1}/'.format(str(year), str(month).zfill(2))
            )
    
def create_download_dirs(year: int, month: int) -> None:
    root_dir = Settings.root_dir()
    if not os.path.exists(root_dir + str(year)):
        os.mkdir(root_dir + str(year))
    if not os.path.exists(root_dir + str(year) + "/" + str(month).zfill(2)):
        os.mkdir(root_dir + str(year) + "/" + str(month).zfill(2))
    
def download_rss(year: int, month: int) -> None:
    "unittested"
    root_link = "https://www.sec.gov/Archives/edgar/monthly/"
    rss_filename = "xbrlrss-{0}-{1}.xml".format(
            str(year), 
            str(month).zfill(2))
    link = root_link + rss_filename    
    rss_filename = rss_filename[4:]
    
    create_download_dirs(year, month)
        
    ret = fetch_urlfile(url_text=link, 
                        filename = year_month_dir(year, month) 
                                   + rss_filename)
    if ret == '':
        raise xbrlxml.xbrlexceptions.XbrlException(
                'download fails for SEC rss file for year: {0} and month: {1} '
                    .format(year, month))

def download_and_save(cik: int, adsh: str, zip_filename: str) -> None:
    try:
        download_enclosure(cik, adsh, zip_filename)
        check_zip_file(zip_filename)
        check_zip_file_deep(zip_filename)
    except xbrlxml.xbrlexceptions.XbrlException:
        files = download_files_from_sec(cik, adsh)
        packet = XBRLZipPacket()
        packet.save_packet(files, zip_filename)
    
if __name__ == '__main__':
    download_enclosure(1750, '0001047469-19-004266', 'z:/sec/2019/07/0000001750-0001047469-19-004266.zip')
    check_zip_file_deep('D:/Documents/Python Scripts/EdgarDownload/tests/resources/xbrldown_bad_deep.zip')
    