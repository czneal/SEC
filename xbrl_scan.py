# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 11:39:09 2017

@author: Asus
"""
import xml.etree.ElementTree as ET
import os
import datetime as dt
import urllib
import urllib.request
import xbrl_file
import database_operations as dbo
import json
import sys
import log_file
import traceback

def SECdownload(year, month):
    sec_data = None
    root_link = "https://www.sec.gov/Archives/edgar/monthly/"
    rss_filename = "xbrlrss-" + str(year) + "-" + str(month).zfill(2) + ".xml"
    link = root_link + rss_filename
    
    tryout = 3
    while tryout > 0:
        print("Try to get rss file: %s, attempt %d " %(rss_filename, 4-tryout)) 
        try:
            req = urllib.request.Request(link)
            rss = urllib.request.urlopen(req)
            sec_data = rss.read()
            rss.close()
            print("Success!")
            break
        except:
            print("Fail!")
            tryout = tryout - 1
    return sec_data

def write_mgnums(report, table, adsh, cur):
    header = {"adsh":0, "tag":1, "version":2, "fy":3, "value":4, "uom":5, "type":6, "ddate":7}
    
    for t in report.fact_tags:
        values = [adsh]
        if t in report.facts:
            f = report.facts[t]
            values.append(f.tag)
            values.append(f.source)
            values.append(report.fy)
            values.append(f.value)
            values.append(f.uom)
            if len(report.contexts[f.context]) == 1:
                values.append("I")
            else:
                values.append("D")
            values.append(report.ddate)
        else:
            node = None
            for _, c in report.chapters.items():
                if c.chapter != "sta" or t not in c.nodes:
                    continue
                else:
                    node = c.nodes[t]
                    break
            if node is None or node.value is None:
                continue
            
            f = None
            for n in xbrl_file.Node.enum_children(node):
                if n.name in report.facts:
                    f = report.facts[n.name]
                    break                
            if f is None:
                continue
            values.append(node.tag)
            values.append(node.source)
            values.append(report.fy)
            values.append(node.value)
            values.append(f.uom)
            if len(report.contexts[f.context]) == 1:
                values.append("I")
            else:
                values.append("D")
            values.append(report.ddate)
        
        table.write(header, values, cur)
        
    table.flush(cur)
        
    
def scan_period(year, month, log, sec_dir="d:/sec/", cik_filter=0):    
    log.write_to_log("year: " + str(year) + " month: " + str(month))
    print("year:", year, "month:", month)
    
    ns = {"edgar":"http://www.sec.gov/Archives/edgar"} 
    xbrl_types = {"EX-100.INS":0, "EX-101.INS":0}
        
    parent_dir = sec_dir+str(year)+"/"+str(month).zfill(2)
    rss_filename = "rss-" + str(year)+"-"+str(month).zfill(2) + ".xml"
    
    if not os.access(parent_dir+"/"+rss_filename, os.F_OK):
        f = open(parent_dir+"/"+rss_filename,"wb")
        f.write(SECdownload(year, month))
        f.close()
        
    tree = ET.parse(parent_dir+"/"+rss_filename)    
    root = tree.getroot()
    
    items = root.findall(".//item")
    n_total_files = len(items)+1
    n_files_processed = 0
    step = 5    
    con = dbo.OpenConnection()
    try:
        cur = con.cursor()
        companies = dbo.Table("companies", con, buffer_size=1)    
        reports = dbo.Table("reports", con, buffer_size=1)
        mgnums = dbo.Table("mgnums", con)
        form_types = {"10-k","10-k/a"}
        
        for item in items:
            n_files_processed += 1
            
            if n_files_processed % step == 0:
                print("\r" + "Processed with {0} of {1}".format(n_files_processed, n_total_files), end="")            
                
            if item.find("description").text.lower() not in  form_types: 
                continue
            
            edgar = item.find("edgar:xbrlFiling", ns)
            company_name = edgar.find("edgar:companyName", ns).text.strip()
            cik = int(edgar.find("edgar:cikNumber",ns).text.strip())
            
            
            if cik_filter != 0:
                if cik_filter != int(cik):
                    continue
            
            adsh = edgar.find("edgar:accessionNumber", ns).text
#            dtstr = edgar.find("edgar:period",ns).text
#            period = dt.date(int(dtstr[:4]), int(dtstr[4:6]), int(dtstr[6:]))
#            period_end = dtstr[4:8]
#            if edgar.find("edgar:fiscalYearEnd", ns) is not None:
#                period_end = edgar.find("edgar:fiscalYearEnd", ns).text
            form = edgar.find("edgar:formType", ns).text
            
            
            file_date = edgar.find("edgar:acceptanceDatetime", ns).text[0:8]
            file_date = dt.date(int(file_date[:4]), int(file_date[4:6]), int(file_date[6:]))
            
            sic = 0
            if edgar.find("edgar:assignedSic", ns) is not None:
                sic = int(edgar.find("edgar:assignedSic", ns).text.strip())
            files = edgar.find("edgar:xbrlFiles", ns)
            
            xbrl_filename = ""
                    
            for f in files.findall("edgar:xbrlFile", ns):
                xbrl_type = f.attrib["{"+ns["edgar"]+"}type"]
                if xbrl_type in xbrl_types:
                    xbrl_filename = f.attrib["{"+ns["edgar"]+"}file"]
            
            z_filename = parent_dir + "/" + str(cik).zfill(10) + "-" + adsh + ".zip"
            report = xbrl_file.XBRLFile(log)
            try:
                if not report.read(z_filename, xbrl_filename):
                    continue
            except:
                print("\nUnexpected error while reading xml files.")
                print(sys.exc_info())
                continue
            
            
            companies.write({"company_name":0,"cik":1, "sic":2, "isin":3}, [company_name, cik, sic,  report.isin], cur)
            reports.write({"adsh":0, "cik":1, "period":2, "period_end":3, "quarter":4, 
                           "file_date":5, "file_link":6, "form":7, "fin_year":8, "contexts":9, "structure":10, "trusted":11}, 
                          [adsh, cik, report.ddate, report.fye, 0, file_date, z_filename, form, report.fy,
                           json.dumps(report.contexts), report.structure_dumps(), report.trusted], cur)
    
            write_mgnums(report, mgnums, adsh, cur)
            con.commit()
            
        print("\r" + "Processed with {0} of {1}".format(n_files_processed+1, n_total_files))
        
    except:
        print("Unexpected error:", sys.exc_info())
        con.close()
    
    return True

def update_current_month(y=None, m=None):
    try:
        if y is None or m is None:
            y = dt.date.today().year
            m = dt.date.today().month
            if dt.date.today().day == 1:
                m -= 1
            if m == 0:
                m = 12
                y -= 1
        
        err_log = log_file.LogFile("d:/sec/xbrl_err_log.txt")
        log = xbrl_file.LogFile("d:/sec/xbrl_log-{0}-{1}-{2}.{3}".format(y,m,dt.date.today(),"txt"))
        scan_period(y, m, log)        
    except:        
        err_log.write(sys.exc_info()[0])
        err_log.write(sys.exc_info()[1])
        traceback.print_tb(sys.exc_info()[1], file=err_log.log_file)
    finally:    
        log.close()
        err_log.close()
        
def update_parameteres_in_database():    
    for year in range(2010, 2019):
        for month in range(1,13):
            update_current_month(year, month) 
        
#update_current_month(2018,7)
#update_parameteres_in_database()