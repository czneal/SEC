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
import xbrl_file_ext
import log_file
import database_operations as dbo
import json
import sys
from settings import Settings

class AdshFilter(object):
    def __init__(self, filename):
        self.adshs = set()
        with open(filename) as f:
            self.adshs = set([l.replace('\n','') for l in f.readlines()])

    def check(self, adsh):
        return adsh in self.adshs

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

#def write_mgnums(report, table, adsh, cur):
#    header = {"adsh":0, "tag":1, "version":2, "fy":3, "value":4, "uom":5, "type":6, "ddate":7}
#
#    for t in report.fact_tags:
#        values = [adsh]
#        if t in report.facts:
#            f = report.facts[t]
#            values.append(f.tag)
#            values.append(f.source)
#            values.append(report.fy)
#            values.append(f.value)
#            values.append(f.uom)
#            if len(report.contexts[f.context]) == 1:
#                values.append("I")
#            else:
#                values.append("D")
#            values.append(report.ddate)
#        else:
#            node = None
#            for _, c in report.chapters.items():
#                if c.chapter != "sta" or t not in c.nodes:
#                    continue
#                else:
#                    node = c.nodes[t]
#                    break
#            if node is None or node.value is None:
#                continue
#
#            f = None
#            for n in xbrl_file.Node.enum_children(node):
#                if n.name in report.facts:
#                    f = report.facts[n.name]
#                    break
#            if f is None:
#                continue
#            values.append(node.tag)
#            values.append(node.source)
#            values.append(report.fy)
#            values.append(node.value)
#            values.append(f.uom)
#            if len(report.contexts[f.context]) == 1:
#                values.append("I")
#            else:
#                values.append("D")
#            values.append(report.ddate)
#
#        table.write({h:values[i] for h,i in header.items()}, cur)
#
#    table.flush(cur)

def write_mgnums(report, table, adsh, cur):
    cntx = []
    if 'bs' in report.cntx:
        cntx.append(report.cntx['bs'])
    if 'is' in report.cntx:
        cntx.append(report.cntx['is'])
    if 'cf' in report.cntx:
        cntx.append(report.cntx['cf'])

    df = report.facts_df
    df['adsh'] = adsh
    spl = df['tag'].str.split(':', expand=True)
    df['tag'] = spl[1]
    df['version'] = spl[0]
    df['type'] = df['instant'].apply(lambda x: 'I' if x else 'D')
    df['fy'] = report.fy
    df.rename(index = str, columns={'edate':'ddate'}, inplace=True)

    df = df[df['context'].isin(cntx)]
    if not table.write_df(df, cur):
        print('{0}:fail to write mgnums'.format(adsh))



def scan_period(year, month, log, warn, err, sec_dir=Settings.root_dir(), cik_filter=0, adsh_filter=None):
    log.write("year: " + str(year) + " month: " + str(month))
    print("year:", year, "month:", month)

    ns = {"edgar":"http://www.sec.gov/Archives/edgar"}

    parent_dir = sec_dir+str(year)+"/"+str(month).zfill(2)
    rss_filename = "rss-" + str(year)+"-"+str(month).zfill(2) + ".xml"

    if not os.access(parent_dir+"/"+rss_filename, os.F_OK):
        with open(parent_dir+"/"+rss_filename,"wb") as f:
            f.write(SECdownload(year, month))


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
        docum = dbo.Table('docum', con)
        mgtags = dbo.Table('mgtags', con)

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
            if adsh_filter is not None:
                if not adsh_filter(adsh):
                    continue

            form = edgar.find("edgar:formType", ns).text


            file_date = edgar.find("edgar:acceptanceDatetime", ns).text[0:8]
            file_date = dt.date(int(file_date[:4]), int(file_date[4:6]), int(file_date[6:]))

            sic = 0
            if edgar.find("edgar:assignedSic", ns) is not None:
                sic = int(edgar.find("edgar:assignedSic", ns).text.strip())

            z_filename = parent_dir + "/" + str(cik).zfill(10) + "-" + adsh + ".zip"
            report = xbrl_file_ext.XBRLFile(log, warn, err)
            try:
                if not report.read(z_filename, file_date):
                    continue
            except:
                print("\nUnexpected error while reading xml files.")
                print(sys.exc_info())
                continue


            companies.write({"company_name":company_name,"cik":cik, "sic":sic, "isin":report.isin}, cur)
            reports.write({"adsh":adsh, "cik":cik, "period":report.ddate,
                           "period_end":report.fye, "quarter":0,
                           "file_date":file_date,
                           "file_link":z_filename, "form":form,
                           "fin_year":report.fy,
                           "contexts":json.dumps(report.cntx),
                           "structure":report.structure_dumps(),
                           "trusted":report.trusted},
                             cur)
            if report.facts_df.shape[0] == 0:
                warn.write2(report.cik_adsh, 'no facts parsed in file {0}'.format(z_filename))
            else:
                write_mgnums(report, mgnums, adsh, cur)

            docum.write_df(report.lab, cur)
            mgtags.write_df(report.xsd, cur)
            con.commit()

        print("\r" + "Processed with {0} of {1}".format(n_files_processed+1, n_total_files))

    except:
        err.write_tb2(report.cik_adsh, sys.exc_info())
        con.close()

    return True

def update_current_month(y=None, m=None):
    err = None
    warn = None
    log = None
    try:
        if y is None or m is None:
            y = dt.date.today().year
            m = dt.date.today().month
            if dt.date.today().day == 1:
                m -= 1
            if m == 0:
                m = 12
                y -= 1

        err = log_file.LogFile(Settings.root_dir() + "xbrl_err.txt")
        warn = log_file.LogFile(Settings.root_dir() + "xbrl_warn.txt")
        log = log_file.LogFile(Settings.root_dir() + "xbrl_log.txt")
        scan_period(y, m, log, warn, err)


    except:
        if err is None:
            raise
        else:
            err.write_tb(sys.exc_info())
    finally:
        if not log is None:
            log.close()
        if not err is None:
            err.close()
        if not warn is None:
            warn.close()


def update_parameteres_in_database():
    for year in range(2013, 2019):
        for month in range(1,13):
            update_current_month(year, month)

#update_current_month(2018,4)
update_parameteres_in_database()