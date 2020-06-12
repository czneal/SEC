# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as ET
from exceptions import XbrlException
from typing import Dict, Optional, Tuple

import lxml.etree
from bs4 import BeautifulSoup

from urltools import fetch_urlfile, fetch_with_delay
from utils import add_root_dir, posix_join, year_month_dir
from xbrlxml.xbrlzip import XBRLZipPacket


def download_files_from_sec(
        cik: int, adsh: str) -> Dict[str, Tuple[str, Optional[bytes]]]:
    adsh_sep = adsh
    adsh = adsh_sep.replace('-', '')
    url = ('https://www.sec.gov/Archives/edgar/data/' +
           '{0}/{1}/{2}-index.htm'.format(cik, adsh, adsh_sep))
    soup = BeautifulSoup(fetch_with_delay(url), 'lxml')

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
            files['xbrl'] = link

    data = {}
    for type_, link in files.items():
        filename = link.split('/')[-1]
        byteio = fetch_with_delay('https://www.sec.gov' + link)
        data[type_] = (filename, byteio)

    return data


def download_enclosure(cik: int, adsh: str, zip_filename: str) -> None:
    if os.path.exists(zip_filename):
        return

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
        raise FileExistsError(filename)

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
        raise XbrlException(', '.join(messages))


def check_zip_file_deep(filename: str) -> None:
    if not os.path.exists(filename):
        raise FileExistsError(filename)

    packet = XBRLZipPacket()
    packet.open_packet(filename)

    messages = []
    for type_, filename in packet.files.items():
        try:
            if filename is None:
                continue
            ET.parse(packet.getfile(type_))
        except lxml.etree.ParseError as e:
            messages.append(str(e))
        except ET.ParseError as e:
            messages.append(str(e))

    if messages:
        raise XbrlException(', '.join(messages))


def create_download_dirs(year: int, month: int) -> None:
    root_dir = add_root_dir('')
    year_dir = add_root_dir(str(year))
    month_dir = year_month_dir(year, month)
    html_month_dir = os.path.join(month_dir, 'html')

    if not os.path.exists(root_dir):
        os.mkdir(root_dir)
    if not os.path.exists(year_dir):
        os.mkdir(year_dir)
    if not os.path.exists(month_dir):
        os.mkdir(month_dir)
    if not os.path.exists(html_month_dir):
        os.mkdir(html_month_dir)


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
                        filename=posix_join(year_month_dir(year, month),
                                            rss_filename))
    if ret == '':
        raise XbrlException(
            f'download fails for SEC rss file for year: {year} and month: {month}')


def download_and_save(cik: int, adsh: str, zip_filename: str) -> None:
    """
    download and save XBRL files for cik and adsh into filename zip file
    than check it
    if check fails raise XbrlException or FileExistError
    """
    try:
        download_enclosure(cik, adsh, zip_filename)
        check_zip_file(zip_filename)
        check_zip_file_deep(zip_filename)
        return

    except (XbrlException, FileExistsError):
        pass

    try:
        files = download_files_from_sec(cik, adsh)
        packet = XBRLZipPacket()
        packet.save_packet(files, zip_filename)
    except Exception:
        pass

    check_zip_file(zip_filename)
    check_zip_file_deep(zip_filename)


if __name__ == '__main__':
    download_enclosure(
        1750, '0001047469-19-004266',
        'z:/sec/2019/07/0000001750-0001047469-19-004266.zip')
    check_zip_file_deep(
        'D:/Documents/Python Scripts/EdgarDownload/tests/resources/xbrldown_bad_deep.zip')
