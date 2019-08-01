# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:54:55 2018

@author: Asus
"""

from bs4 import BeautifulSoup
import urltools
import re
import datetime as dt
from classifiers.mainsheets import MainSheets
import pandas as pd
import database_operations as do
from log_file import LogFile
from utils import ProgressBar
from settings import Settings

def find_dates_pos(soup):
    dates = {}
    re_date = re.compile('\s*\w+\.\s*\d+\,\s*\d+\s*')
    re_date1 = re.compile('\s*\w+\s+\d+\,\s*\d+\s*')
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
                for i in range(row_index+1, row_index + int(th.attrs['rowspan'])):
                    spaces[i] = {col_index:col_step}
                
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
    if re.match('.*\[.*\].*', text):
        raise
        
    s = re.findall('\d+', text)
    v = float(''.join(s))
    s = re.search('\.\d+', text)
    
    if s: v = v/10**(len(s.group(0))-1)
    if re.match('.*\(.+\).*', text): v = -v
    
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
            if not row.find_all(onclick = re.compile("_{0}'".format(tag), re.IGNORECASE)):
                continue
            col_index = 0            
            for td in row.find_all('td'):
                if td.attrs['class'][0] in ['nump', 'num']:
                    data.append({'tag' : tag,
                                 'pos' : col_index,
                                 'value' : convert2money(td.text) * mul})
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
    #bs, cf, is
    sheets = {'bs':0, 'cf':0, 'is':0}
    
    for st in bf.find_all('li', class_='accordion'):
        if not st.find('a', id="menu_cat2"):
            continue
        
        labels = {}  
        for li in st.find_all('a', class_='xbrlviewer'):
            labels[li.text] = int(re.search('\d+', li.attrs['href']).group(0))
            
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
        sheets = {s:labels[l] for l, s in sheet_labels.items()}
    
    reps = {'bs':'', 'cf':'', 'is':''}
    for line in text.split(';'):
        if re.compile('\s*reports\[\d+.*\d+\]\s*=\s*').match(line):
            for sheet, no in sheets.items():
                try:
                    reps[sheet] = re.search('\".+R{0}\.htm.*\"'.format(no), line).group(0).replace('"', '')
                except:
                    pass
    return reps


if __name__ == '__main__':
    import sys
    
#    cik_adsh = [[49600, '0000049600-19-000007'],
#                [1294649,'0001437749-19-002725'],
#                [1595627,'0001564590-19-010053'],
#                [1702780,'0001628280-19-002370']]
    cik_adsh = do.getquery('select cik, adsh from raw_reps where fy=2017',
                           dictionary = False)
    do.execquery('truncate table raw_html')
    err = LogFile('../' + Settings.output_dir() + 'html_parse.err')
    pb = ProgressBar()
    pb.start(len(cik_adsh))
    for cik, adsh in cik_adsh:
        try:
            reps = find_reports(cik, adsh)
            frames = []
            data = []
            for sheet, report in reps.items():
                if report == '': continue
            
                soup = BeautifulSoup(urltools.fetch_urlfile('https://www.sec.gov' + report),
                                     'lxml')
                
                dates = find_dates_pos(soup)
                facts = []
                if sheet == 'bs':
                    facts = find_facts(soup, ['Assets', 'Liabilities', 'LiabilitiesAndStockholdersEquity',
                                       'AssetsCurrent', 'AssetsNoncurrent',
                                       'LiabilitiesCurrent', 'LiabilitiesNoncurrent'])
                if sheet == 'is':
                    facts = find_facts(soup, ['Revenues', 'InterestExpense', 
                                       'ProfitLoss', 'NetIncomeLoss'])
                        
                for row in facts:
                    row['edate'] = dates[row['pos']]
                    row['adsh'] = adsh
                    row['cik'] = cik
                    row['sheet'] = sheet
                data.extend(facts)
                    
            df = pd.DataFrame(data)
        except:
            err.writetb(adsh, cik, excinfo=sys.exc_info())
            continue
        
        try:
            con = do.OpenConnection()
            table = do.Table('raw_html', con)
            cur = con.cursor(dictionary=True)
            table.write_df(df, cur)
            con.commit()
        except:
            err.writetb(adsh, cik, excinfo=sys.exc_info())
        finally:
            if 'con' in locals() and con: con.close()
            
        pb.measure()
        print('\r' + pb.message(), end='')
        
    print()
