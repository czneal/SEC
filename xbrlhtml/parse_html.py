# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:54:55 2018

@author: Asus
"""

from bs4 import BeautifulSoup
import re
import datetime as dt
import pandas as pd

import urltools
import mysqlio.basicio as do
import logs
from utils import ProgressBar
from classifiers.mainsheets import MainSheets
from settings import Settings


def find_dates_pos(soup):
    dates = {}
    re_date = re.compile(r'\s*\w+\.\s*\d+\,\s*\d+\s*')
    re_date1 = re.compile(r'\s*\w+\s+\d+\,\s*\d+\s*')
    spaces = {}

    for row_index, tr in enumerate(soup.find_all("tr")[0:4]):
        col_index = 0
        for th in tr.find_all("th"):
            if row_index in spaces and col_index in spaces[row_index]:
                col_index += spaces[row_index][col_index]

            if 'colspan' in th.attrs:
                col_step = int(th.attrs['colspan'])
            else:
                col_step = 1
            if 'rowspan' in th.attrs:
                for i in range(row_index + 1, row_index +
                               int(th.attrs['rowspan'])):
                    spaces[i] = {col_index: col_step}

            for div in th.find_all('div'):
                if re_date.match(div.text.strip()):
                    d = dt.datetime.strptime(div.text.strip(), '%b. %d, %Y')
                    dates[col_index] = dt.date(d.year, d.month, d.day)
                if re_date1.match(div.text.strip()):
                    d = dt.datetime.strptime(div.text.strip(), '%b %d, %Y')
                    dates[col_index] = dt.date(d.year, d.month, d.day)
            col_index += col_step

    return dates


def convert2money(text):
    if re.match(r'.*\[.*\].*', text):
        raise

    s = re.findall(r'\d+', text)
    v = float(''.join(s))
    s = re.search(r'\.\d+', text)

    if s:
        v = v / 10**(len(s.group(0)) - 1)
    if re.match(r'.*\(.+\).*', text):
        v = -v

    return v


def find_multiplier(soup):
    if soup.find(string=re.compile('In Millions', re.IGNORECASE)):
        return 1000000.0
    if soup.find(string=re.compile('In Thousands', re.IGNORECASE)):
        return 1000.0
    return 1.0


def find_facts(soup, tags):

    mul = find_multiplier(soup)

    data = []

    for row in soup.find_all('tr'):
        for tag in tags:
            if not row.find_all(
                onclick=re.compile(
                    "_{0}'".format(tag),
                    re.IGNORECASE)):
                continue
            col_index = 0
            for td in row.find_all('td'):
                if td.attrs['class'][0] in ['nump', 'num']:
                    data.append({'tag': tag,
                                 'pos': col_index,
                                 'value': convert2money(td.text) * mul})
                if 'colspan' in td.attrs:
                    col_index += int(td.attrs['colspan'])
                else:
                    col_index += 1
    return data


def find_reports(cik, adsh):
    html_data = urltools.fetch_urlfile(
        'https://www.sec.gov/cgi-bin/viewer?action=view&cik={0}&accession_number={1}&xbrl_type=v#'.format(int(cik), adsh)
    )
    bf = BeautifulSoup(html_data, 'lxml')
    text = ""
    for script in bf.find_all('script'):
        text += script.text.strip()

    ms = MainSheets()
    # bs, cf, is
    sheets = {'bs': 0, 'cf': 0, 'is': 0}

    for st in bf.find_all('li', class_='accordion'):
        if not st.find('a', id="menu_cat2"):
            continue

        labels = {}
        for li in st.find_all('a', class_='xbrlviewer'):
            labels[li.text] = int(re.search(r'\d+', li.attrs['href']).group(0))

        sheet_labels = ms.select_ms(labels.keys())
#            try:
#
#                if (ms.match_bs(li.text)
#                        and not re.match('.*(.*parenth.*).*', li.text,  re.IGNORECASE)):
#                    sheets['bs'] =
#                if ms.match_cf(li.text):
#                    sheets['cf'] = int(re.search('\d+', li.attrs['href']).group(0))
#                if (ms.match_is(li.text)
#                        and not re.match('.*compreh.*', li.text, re.IGNORECASE)
#                        and not re.match('.*(.*parenth.*).*', li.text,  re.IGNORECASE)):
#                    sheets['is'] = int(re.search('\d+', li.attrs['href']).group(0))
#
#            except:
#                pass
        sheets = {s: labels[l] for l, s in sheet_labels.items()}

    reps = {'bs': '', 'cf': '', 'is': ''}
    for line in text.split(';'):
        if re.compile(r'\s*reports\[\d+.*\d+\]\s*=\s*').match(line):
            for sheet, no in sheets.items():
                try:
                    reps[sheet] = re.search(
                        r'\".+R{0}\.htm.*\"'.format(no),
                        line).group(0).replace(
                        '"', '')
                except BaseException:
                    pass
    return reps


if __name__ == '__main__':
    pass
