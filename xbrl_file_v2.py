# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 11:44:49 2017

@author: Asus
"""

import xbrl_file as xbrl
import datetime as dt
import re
import json
import io
import sys
import pandas as pd
import numpy as np
import classificators as cl
from log_file import LogFile
from xbrl_chapter import Chapter
import descr_types as doctype
import utils

def add_to_dict(d, sheet, what):
    if sheet in d:
        d[sheet].append(what)
    else:
        d[sheet] = [what]
        
class XBRLFile:
    def __init__(self, log_file = LogFile(), log_err=LogFile(), log_warn=LogFile()):
        self.log = log_file
        self.err = log_err
        self.warn = log_warn

        self.__setup_members()

    def __setup_members(self):
        self.rss_data = None
        self.true_dates = None
        self.file_link = ''
        self.facts = {}
        self.contexts = None
        self.chapters = None
        self.trusted = True
        self.fact_tags = None
        self.units = None
        self.cntx = {}
        self.ddate = None
        self.cik_adsh = None
        self.ms = cl.MainSheets()
        self.facts_df = None
        self.cntx_df = None
        self.lab = None
        self.xsd = None
        self.period_len = 365

    def read(self, zip_filename, rss):        
        self.__setup_members()
        self.rss = rss
        self.file_link = zip_filename
        
        try:
            #unpack zip file and get cal_filename, xbrl_filename, pre_filename, xsd_filename
            self.cik_adsh = str(rss['cik']).zfill(10) + '-' + rss['adsh']
            
            self.log.write2(self.cik_adsh, "open zip archive: " + zip_filename)
            packet = xbrl.XBRLZipPacket(zip_filename, None)
            status, message = packet.open_packet()
            if not status:
                self.err.write2(self.cik_adsh, message)
                return False

            self.chapters = self.read_xsd(packet.xsd_file)
            self.read_cal(packet.cal_file, self.chapters)
            self.read_pre(packet.pre_file, self.chapters)
#            self.read_labels(packet.lab_file)
            
            if not self.organize():
                self.warn.write2(self.cik_adsh, 'too many chapters')

            tools = xbrl.XmlTreeTools()
            tools.read_xml_tree(packet.xbrl_file)

            root, ns = tools.root, tools.ns
            prefixes = {}
            self.fact_tags = self.used_tags(only_sta=False)
            p = set([t.split(':')[0] for t in self.fact_tags])
            for prefix in p:
                if prefix in ns:
                    prefixes[ns[prefix]] = prefix
                    
            if 'us-gaap' in ns:
                self.rss['us-gaap'] = ns['us-gaap'].split('/')[-1][0:4]
            else:
                self.rss['us-gaap'] = None

            self.read_dei_section(root, ns)
            if self.fy is None or self.ddate is None or self.fye is None:
                self.err.write2(self.cik_adsh, 'dei section is not full')                

           
            self.read_contexts_section(root, ns, tools.xbrli)
            self.read_units_section(root, ns, tools.xbrli)
            self.read_facts_section(root, self.fact_tags, ns, prefixes)

#            calc_log = io.StringIO()
#            for _, c in self.chapters.items():
#                c.check_cal_scheme(self.facts, calc_log)
#                #c.update_pre_values(self.facts)
#            calc_log.flush()
#            calc_log.seek(0)
#            self.calc_log = calc_log.read()
#
#            if self.calc_log != "":
#                self.trusted = False
#                self.err.write2(self.cik_adsh, "calculation error!")
#                self.err.write2(self.cik_adsh, self.calc_log)
#            else:
#                self.trusted = True

            self.log.write2(self.cik_adsh, 'start find contexts...')
            self.true_dates = self.period_fy_fye(d_tolerance = 8)
            self.make_contexts_facts(8)
            self.log.write2(self.cik_adsh, 'end find contexts...ok')
#            self.find_instant_context(tolerance_days = 8)
#            self.find_noninstant_context(tolerance_days = 8)
#            self.log.write2(self.cik_adsh, 'end find contexts...ok')
#
#            if 'bs_err' in self.cntx:
#                self.err.write2(self.cik_adsh, 'bs_err:' + str(self.cntx['bs_err']))
#            if 'is_err' in self.cntx:
#                self.err.write2(self.cik_adsh, 'is_err:' + str(self.cntx['is_err']))
#            if 'cf_err' in self.cntx:
#                self.err.write2(self.cik_adsh, 'cf_err:' + str(self.cntx['cf_err']))
#            if 'bs_sum' in self.cntx:
#                self.err.write2(self.cik_adsh, 'bs_sum:' + str(self.cntx['bs_sum']))
#            if 'is_sum' in self.cntx:
#                self.err.write2(self.cik_adsh, 'is_sum:' + str(self.cntx['is_sum']))
#            if 'cf_sum' in self.cntx:
#                self.err.write2(self.cik_adsh, 'cf_sum:' + str(self.cntx['cf_sum']))
        except:
            self.err.write2(self.cik_adsh, "unexpected error while reading:"+zip_filename)
            self.err.write_tb2(self.cik_adsh, sys.exc_info())
            return False

        return True

    def used_tags(self, only_sta):
        fact_tags = set()
        for _, c in self.chapters.items():
            fact_tags.update(c.get_cal_tags(only_sta))
            fact_tags.update(c.get_pre_tags(only_sta))
        return fact_tags
    
    def get_cal_tags(self, only_sta):
        fact_tags = set()
        for _, c in self.chapters.items():
            fact_tags.update(c.get_cal_tags(only_sta))
        return fact_tags
    
    def get_pre_tags(self, only_sta):
        fact_tags = set()
        for _, c in self.chapters.items():
            fact_tags.update(c.get_pre_tags(only_sta))
        return fact_tags

    def get_dimentions(self, only_sta):
        dim = set()
        dim.add(None)
        for _, c in self.chapters.items():
            if self.ms.match(c.label):
                dim.update(c.get_dimentions(only_sta))
        return dim

    def get_members(self, only_sta):
        member = set()
        member.add(None)
        for _, c in self.chapters.items():
            if self.ms.match(c.label):
                member.update(c.get_members(only_sta))
        return member

    def structure_dumps(self):
        dump = {}
        for _, c in self.chapters.items():
            js = c.json()
            if js is not None:
                dump[c.label] = js

        return json.dumps(dump)


    def read_dei_section(self, root, ns):
        self.log.write2(self.cik_adsh, "start reading dei section...")
        self.fye = ""
        node = root.find("./dei:CurrentFiscalYearEndDate", ns)
        if node is not None:
            self.fye = node.text.strip().replace("-","")
        else:
            self.err.write2(self.cik_adsh, "dei:CurrentFiscalYearEndDate not found")

        self.ddate = None
        node = root.find("./dei:DocumentPeriodEndDate", ns)
        if node is not None:
            self.ddate = utils.str2date(node.text.strip())
        else:
            self.err.write2(self.cik_adsh, "dei:DocumentPeriodEndDate not found")

        self.isin = None
        node = root.find("./dei:TradingSymbol", ns)
        if node is not None and node.text is not None:
            self.isin = node.text.upper().strip()
            if len(self.isin)>12: self.isin = None

        self.fy = None
        node = root.find("./dei:DocumentFiscalYearFocus", ns)
        if node is not None:
            try:
                self.fy = int(node.text.strip())
            except ValueError:
                self.fy = None
            if self.fy < 2000 or self.fy > 2100:
                self.fy = None
        else:
            self.err.write2(self.cik_adsh, "dei:DocumentFiscalYearFocus not found")

        self.log.write2(self.cik_adsh, "end reading dei section...ok")

    def read_units_section(self, root, ns, xbrli):
        self.log.write2(self.cik_adsh, "start reading units section...")
        self.units = {}
        currency = set(['usd', 'cad', 'eur'])

        for elem in root.findall("./"+xbrli+"unit"):
            name = elem.attrib["id"].lower().strip()
            div = list(elem.iter(xbrli+"divide"))
            if len(div) > 0:
                num = div[0].find(xbrli+"unitNumerator")
                denum = div[0].find(xbrli+"unitDenominator")
                m1 = num.find(xbrli+"measure").text.lower().strip()
                m2 = denum.find(xbrli+"measure").text.lower().strip()
                for c in currency:
                    if m1.endswith(c) and m2.endswith("shares"):
                        self.units[name] = c
            else:
                measure = list(elem.iter(xbrli+"measure"))
                m = measure[0].text.lower().strip()
                for c in currency:
                    if m.endswith(c):
                        self.units[name] = c
                if m.endswith("shares"):
                    self.units[name] = "shares"
                if m.endswith("pure"):
                    self.units[name] = "pure"

        self.log.write2(self.cik_adsh, "end reading units section...ok")

    def read_contexts_section(self, root, ns, xbrli):
        self.log.write2(self.cik_adsh, "start reading contexts...")

        self.contexts = {}
        for elem in root.findall("./"+xbrli+"context"):
            cntx = Context(elem.iter())
            
            self.contexts[cntx.id] = cntx

        self.log.write2(self.cik_adsh, "end reading contexts...ok")


    def read_facts_section(self, root, fact_tags, ns, prefixes):
        self.log.write2(self.cik_adsh, "start reading facts section...")
        self.facts.clear()

        for elem in root.iter():
            for pref in prefixes:
                if elem.tag.startswith("{"+pref):
                    source = elem.tag.strip().split("}")[0][1:]
                    f = Fact(elem, prefixes[source])

                    if f.name not in fact_tags:
                        continue
                    if f.uom not in self.units:
                        continue
                    f.uom = self.units[f.uom]

                    if (f.name, f.context) in self.facts:
                        self.facts[(f.name, f.context)].update(elem)
                    else:
                        if f.context in self.contexts:
                            self.facts[(f.name, f.context)] = f
        self.log.write2(self.cik_adsh, "end reading facts section...ok")

    def check_period_fy(self, root, ns, xbrli):
        check = True
        dd = dt.date(int(self.ddate[0:4]), int(self.ddate[5:7]), int(self.ddate[8:10]))
        if dd.year < self.fy:
            check = False
        if abs((dt.date(self.fy, 12, 31) - dd).days)>270:
            check = False
        if check:
            return

        d = ""
        for elem in root.findall("./"+xbrli+"context"):
            if elem.attrib["id"].strip() == self.ddate_context:
                period = elem.find(xbrli+"period")
                if len(list(period)) == 1:
                    d = period[0].text.strip()
                else:
                    d = period[1].text.strip()
                break

        if d == self.ddate:
            if dd.month>3:
                self.fy = dd.year
            else:
                self.fy = dd.year-1
            check = True
        else:
            if d != "":
                dd = dt.date(int(d[0:4]), int(d[5:7]), int(d[8:10]))
                self.ddate = d
                if dd.month>3:
                    self.fy = dd.year
                else:
                    self.fy = dd.year-1
                check = True

        if not check:
            self.warn.write2(self.cik_adsh, "period and fy inconsistence can not be solved")
        return check

    def fey_dist(self, ddate):
        if ddate is None:
            return None
        y = ddate.year
        m = int(self.fye[:2])
        d = int(self.fye[2:])
        if m == 2 and d == 29:
            d = 28
        return abs((ddate-dt.date(y,m,d)).days)
    
    def period_fy_fye(self, d_tolerance):
        rss = self.rss
        true_dates = {}
        if (self.ddate is not None and 
                rss['period'] is not None and
                abs(self.ddate - rss['period']).days <= d_tolerance):
            true_dates['edate'] = self.ddate
        if self.fy == rss['fy'] and self.fy is not None:
            true_dates['fy'] = self.fy
        if self.fye == rss['fye'] and self.fye is not None:
            true_dates['fye'] = self.fye
        if len(true_dates) == 3:
            return true_dates
        
        if 'fy' not in true_dates:
            fy = int(self.rss['us-gaap'])
            if rss['fy'] is not None and fy == rss['fy']: 
                true_dates['fy'] = fy
            if self.fy == fy: 
                true_dates['fy'] = fy
        
        if 'edate' not in true_dates:            
            period_rss_fy = None
            period_r_fy = None
            period_fy = None
            if rss['fy'] is not None and rss['fye'] is not None:
                period_rss_fy = utils.periodend(rss['fy'], 
                                                   int(rss['fye'][0:2]), 
                                                   int(rss['fye'][2:4]))
            if self.fy is not None and self.fye is not None:
                period_r_fy = utils.periodend(self.fy, 
                                                 int(self.fye[0:2]), 
                                                 int(self.fye[2:4]))
            if 'fy' in true_dates and 'fye' in true_dates:
                period_fy = utils.periodend(true_dates['fy'],
                                            int(true_dates['fye'][0:2]),
                                            int(true_dates['fye'][2:4]))
            a = [period_r_fy, period_rss_fy, period_fy, rss['period'], self.ddate]
            a = [e for e in a if e is not None]
            a = sorted(a)            
            v = {}
            for e in a:
                if e in v: v[e] += 1
                else: v[e] = 0
            v = sorted(v.items(), key=lambda x: x[1], reverse=True)
            if len(v)>1 and v[0][1]>=3:
                true_dates['period'] = v[0][0]
            
        return true_dates

    def make_contexts_facts(self, day_tolerance):        
        edate = None
        if 'edate' in self.true_dates:
            edate = self.true_dates['edate']
        contexts = pd.DataFrame(data = [e.aslist() for (n, e) in self.contexts.items()],
                        columns = ['context', 'instant', 'sdate', 'edate',
                                   'dim', 'member'])
        if edate is not None:
            contexts = contexts[abs(contexts['edate']-edate) <= 
                                dt.timedelta(days=day_tolerance)]
            
        contexts = contexts[contexts['dim'].isin(self.get_dimentions(only_sta=True))
                            & contexts['member'].isin(self.get_members(only_sta=True))]
            
        facts = pd.DataFrame(data = [fact.aslist() for ((f, c), fact) in self.facts.items()],
                             columns=['tag', 'value', 'uom', 'context'])
        f = facts[facts['tag'].isin(self.get_pre_tags(only_sta=True))]
        contexts = contexts[contexts['context'].isin(f['context'].unique())]
        
        facts = (facts.merge(contexts, 'inner', left_on='context', right_on='context')
                      .sort_values(['tag','dim','member']))
        
        self.cntx_df = contexts
        self.facts_df = facts

    def find_instant_context(self, tolerance_days=8):
        markers = set()
        dims = set([None])
        members = set([None])
        for _, chapter in self.chapters.items():
            if not self.ms.match_bs(chapter.label):
                continue
            markers.update(chapter.get_pre_tags())
            markers.update(chapter.get_cal_tags())
            dims.update(chapter.get_dimentions())
            members.update(chapter.get_members())

        facts = self.facts_df[self.facts_df['dim'].isin(dims) &
                              self.facts_df['member'].isin(members) &
                              (self.facts_df['instant']) &
                              self.facts_df['tag'].isin(markers)]

        dates = (facts.groupby('edate')['edate']
                      .count()
                      .sort_values(ascending=False))
        dates = dates[dates>dates.mean()/2]
        edate = dates.index.max()
        if pd.isnull(edate):
            #this means that there is no apropriate sections in reports
            self.cntx['bs'] = [None]
            self.ddate = xbrl.str2date(self.ddate)
            return

        if xbrl.str2date(self.ddate) != edate:
            self.warn.write2(self.cik_adsh, 'ddate != edate')

        self.ddate = edate

        tolerance = dt.timedelta(days = tolerance_days)

        f = facts[(np.abs(facts['edate'] - edate) <= tolerance)]

        cntx_grp = f.groupby('context')['context'].count()
        cntx_grp = cntx_grp[cntx_grp>=cntx_grp.mean()/2]
        cntx_grp = self.cntx_df[self.cntx_df['context'].isin(cntx_grp.index)]

        if cntx_grp.shape[0] == 1:
            add_to_dict(self.cntx, 'bs', cntx_grp.iloc[0]['context'])
            return

        filtered = cntx_grp[cntx_grp['dim'].isnull()]
        if filtered.shape[0] == 1:
            add_to_dict(self.cntx, 'bs', filtered.iloc[0]['context'])
            return
        if filtered.shape[0] > 1:
            self.cntx['bs_err'] = filtered['context'].tolist()
            return

        #filtered.shape[0] == 0:
        filtered = cntx_grp[cntx_grp['member'].str.contains('successor', case=False)]
        if filtered.shape[0] == 1:
            add_to_dict(self.cntx, 'bs', filtered.iloc[0]['context'])
            return
        if filtered.shape[0] > 1:
            self.cntx['bs_err'] = filtered['context'].tolist()
            return
        #filtered.shape[0] == 0:
        filtered = cntx_grp[cntx_grp['member'].str.contains('parentcompany', case=False)]
        if filtered.shape[0] == 1:
            add_to_dict(self.cntx, 'bs', filtered.iloc[0]['context'])
        if filtered.shape[0] > 1:
            self.cntx['bs_err'] = filtered['context'].tolist()
        if filtered.shape[0] == 0:
            self.cntx['bs_sum'] = cntx_grp['context'].tolist()

        return

    def find_noninstant_context(self, tolerance_days=8):
        for _, chapter in self.chapters.items():
            sheet = ''
            if self.ms.match_is(chapter.label):
                sheet = 'is'
            if self.ms.match_cf(chapter.label):
                sheet = 'cf'
            if sheet == '':
                continue

            markers = set()
            dims = set([None])
            members = set([None])

            markers.update(chapter.get_pre_tags())
            markers.update(chapter.get_cal_tags())
            dims.update(chapter.get_dimentions())
            members.update(chapter.get_members())

            facts = self.facts_df[self.facts_df['dim'].isin(dims) &
                                  self.facts_df['member'].isin(members) &
                                  (self.facts_df['instant'] == False) &
                                  self.facts_df['tag'].isin(markers)]

            edate = self.ddate
            sdate = edate - dt.timedelta(days=365.56)

            tolerance = dt.timedelta(days = tolerance_days)

            f = facts[(np.abs(facts['edate'] - edate) <= tolerance) &
                      (np.abs(facts['sdate'] - sdate) <= tolerance)]

            cntx_grp = self.noninstant_prep(f)
            if cntx_grp.shape[0] == 1:
                add_to_dict(self.cntx, sheet, cntx_grp.iloc[0]['context'])
            elif cntx_grp.shape[0] > 1:
                filtered = cntx_grp[cntx_grp['dim'].isnull()]
                if filtered.shape[0] == 1:
                    add_to_dict(self.cntx, sheet, filtered.iloc[0]['context'])
                elif filtered.shape[0] > 1:
                    self.cntx[sheet+'_err'] = filtered['context'].tolist()
                else:
                    filtered = cntx_grp[cntx_grp['member'].str.contains('parentcompany', case=False)]
                    if filtered.shape[0] == 1:
                        add_to_dict(self.cntx, sheet, filtered.iloc[0]['context'])
                    elif filtered.shape[0] > 1:
                        self.cntx[sheet+'_err'] = filtered['context'].tolist()
                    else:
                        filtered = cntx_grp[cntx_grp['member'].str.contains('successor', case=False)]
                        if filtered.shape[0] == 1:
                            add_to_dict(self.cntx, sheet, filtered.iloc[0]['context'])
                        elif filtered.shape[0] > 1:
                            self.cntx[sheet+'_err'] = filtered['context'].tolist()
                        else:
                            self.cntx[sheet+'_sum'] = cntx_grp['context'].tolist()
            else:
                #merge contexts by date or find short periods
                f = facts[(np.abs(facts['edate'] - edate) <= tolerance) |
                          (np.abs(facts['sdate'] - sdate) <= tolerance)]
                cntx_grp = self.noninstant_prep(f)
                if cntx_grp.shape[0] == 1:
                    add_to_dict(self.cntx, sheet, cntx_grp.iloc[0]['context'])
                elif cntx_grp.shape[0] == 0:
                    add_to_dict(self.cntx, sheet, None)
                else:
                    self.warn.write2(self.cik_adsh, 'should be implemented')


    def noninstant_prep(self, f):
        cntx_grp = f.groupby('context')['context'].count()
        cntx_grp = cntx_grp[cntx_grp>=cntx_grp.mean()/2]
        cntx_grp = self.cntx_df[self.cntx_df['context'].isin(cntx_grp.index)]
        return cntx_grp

    def read_xsd(self, xsd_file):
        try:
            chapters = {}
            self.log.write2(self.cik_adsh, "start reading xsd scheme...")
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

            tools = xbrl.XmlTreeTools()
            tools.read_xml_tree(xsd_file)
            root = tools.root
            link = tools.link

            self.xsd = doctype.read_xsd_elements_xmltree(tools)

            for role in root.iter(link+"roleType"):
                rol_def = role.find(link+"definition")
                chapter = ""
                if matchDocument.match(rol_def.text):
                    chapter = "doc"
                elif matchStatement.match(rol_def.text):
                    if matchParenthetical.match(rol_def.text):
                        chapter = "par"
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
                label = rol_def.text.split(" - ")[-1].strip()
                chapters[role_uri] = Chapter(role_uri, role.attrib["id"], chapter, label)

            self.log.write2(self.cik_adsh, "end reading xsd scheme, status ok")
            return chapters
        except:
            self.err.write_tb2(self.cik_adsh, sys.exc_info())
            return None

    def read_cal(self, cal_filename, chapters):
        self.log.write2(self.cik_adsh, "start reading calculation scheme...")

        if cal_filename is None:
            self.warn.write2(self.cik_adsh, 'there is no calculation scheme')
            return

        tools = xbrl.XmlTreeTools()
        tools.read_xml_tree(cal_filename)

        xlink, empty = tools.xlink, tools.empty
        root = tools.root

        for calcLink in root.iter(empty+"calculationLink"):
            role_uri = calcLink.attrib[xlink+"role"].strip()
            if role_uri in chapters:
                chapters[role_uri].read_cal(calcLink, empty, xlink)
            else:
                self.warn.write2(self.cik_adsh, 'role_uri: {0} not in chapters (read_cal)'.format(role_uri))

        self.log.write2(self.cik_adsh, "end reading calculation scheme...ok")

    def read_pre(self, pre_filename, chapters):
        self.log.write2(self.cik_adsh, "start reading presentation scheme...")

        tools = xbrl.XmlTreeTools()
        tools.read_xml_tree(pre_filename)
        xlink, empty = tools.xlink, tools.empty
        root = tools.root

        for preLink in root.iter(empty+"presentationLink"):
            role_uri = preLink.attrib[xlink+"role"].strip()
            if role_uri in chapters:
                chapters[role_uri].read_pre(preLink, empty, xlink)
            else:
                self.warn.write2(self.cik_adsh, 'role_uri: {0} not in chapters (read_pre)'.format(role_uri))

        self.log.write2(self.cik_adsh, "end reading presentation scheme...ok")

    def read_labels(self, lab_filename):
        self.log.write2(self.cik_adsh, 'start reading labels and documentation...')

        try:
            self.lab = doctype.read_documentation(lab_filename)
            self.lab = self.lab[self.lab['xsd_id'].isin(self.xsd['xsd_id'])]
        except:
            self.warn.write(self.cik_adsh, 'lab file could not be read')

        self.log.write2(self.cik_adsh, 'end reading labels and documentation...ok')

    def chapters_counts(self):
        counts = {'bs':0, 'cf':0, 'is':0}
        for _, chapter in self.chapters.items():
            if chapter.chapter != 'sta':
                continue
            if len(chapter.nodes) == 0:
                continue

            if self.ms.match_bs(chapter.label):
                counts['bs'] += 1
            if self.ms.match_is(chapter.label):
                counts['is'] += 1
            if self.ms.match_cf(chapter.label):
                counts['cf'] += 1

        if (counts['bs'] > 1 or counts['is'] > 2 or counts['cf'] > 1):
            return False

        return True

    def organize(self):
        if not self.chapters_count():
            return False

        for c, chapter in self.chapters.items():
            if chapter.chapter != 'sta' or not self.ms.match(chapter.label):
                continue
#            global organize
            for c1, chapter1 in self.chapters.items():
                if c1==c:
                    continue
                if chapter1.chapter == 'sta':
                    continue

                for n, node in chapter.nodes.items():
                    if len(node.children) != 0:
                        continue
                    for m, node1 in chapter1.nodes.items():
                        if len(node1.children) == 0:
                            continue
                        if node1.name == node.name:
                            node.children = node1.children.copy()

#           local organize
#            for c1, chapter1 in chapters.items():
#                if chapter1.chapter != "sta":
#                    continue
#                if c1==c:
#                    continue
#
#                for n, node in chapter.nodes.items():
#                    if len(node.children) != 0:
#                        continue
#                    for m, node1 in chapter1.nodes.items():
#                        if len(node1.children) == 0:
#                            continue
#                        if node1.name == node.name:
#                            node.children = node1.children.copy()
        return True
    
    def chapters_count(self):
        counts = {'bs':0, 'cf':0, 'is':0}
        for _, chapter in self.chapters.items():
            if chapter.chapter != 'sta':
                continue
            if len(chapter.nodes) == 0:
                continue

            if self.ms.match_bs(chapter.label):
                counts['bs'] += 1
            if self.ms.match_is(chapter.label):
                counts['is'] += 1
            if self.ms.match_cf(chapter.label):
                counts['cf'] += 1

        if (counts['bs'] > 1 or counts['is'] > 2 or counts['cf'] > 1):
            return False

        return True

class Fact(object):
    def __init__(self, elem, source):
        self.tag, self.context, self.value, self.uom, self.decimals, _  = Fact.read(elem)
        self.source = source.lower()
        self.name = source+":"+self.tag

    def aslist(self):
        return [self.name, self.value, self.uom, self.context[0:512]]

    def read(elem):
        if 'contextRef' not in elem.attrib:
            return '', '', None, '', 0, ''

        context = elem.attrib["contextRef"].strip()

        uom = ""
        if "unitRef" in elem.attrib:
            uom = elem.attrib["unitRef"].strip().lower()


        decimals = ""
        if "decimals" in elem.attrib:
            decimals = elem.attrib["decimals"].strip()
        if decimals.lower() == "inf" or decimals == "":
            decimals = 0
        else:
            decimals = abs(int(decimals))

        if elem.text is None:
            value = 0.0
        else:
            try:
                value = float(elem.text.strip())
            except ValueError:
                value = 0.0

        #check overflow of mysql decimal(24,4)
        if abs(value) > 10**19:
            value = 0

        name = elem.tag.strip().split("}")[-1]
        prefix = elem.tag.strip().split("}")[0][1:]

        return name, context, value, uom, decimals, prefix

    def update(self, elem):
        name, context, value, uom, decimals, prefix = Fact.read(elem)
        if context != self.context:
            return

        if decimals < self.decimals:
            self.value = value


class ContextsGroundTruth(object):
    def __init__(self, filename):
        self.contexts = (pd.read_csv(filename,sep=';')
                            .set_index('adsh')
                            )
        self.contexts[pd.isnull(self.contexts)] = None

    def get_contexts(self, adsh):
        if adsh in self.contexts.index:
            return {'bs':self.contexts.loc[adsh]['bs'],
                    'is':self.contexts.loc[adsh]['is'],
                    'cf':self.contexts.loc[adsh]['cf']}

        return None

    def iterate(self):
        for index, row in self.contexts.iterrows():
            yield index, row['filename'], self.get_contexts(index)


class Context(object):
    attr = ['id', 'instant', 'sdate', 'edate', 'dim', 'member']
    def __init__(self, elems):
        self.instant = None
        self.sdate = None
        self.edate = None
        self.dim = None
        self.member = None
        self.id = None

        self.read(elems)

    def __str__(self):        
        return ("(id:{0}, instant:{1}, startDate:{2}, endDate: {3}, axis:{4}, dimension:{5})"
                .format(self.id, self.instant, self.sdate, self.edate,
                        self.dim, self.member))

    def aslist(self):        
        if self.member is None:
            return [self.id, self.instant,
                      self.sdate, self.edate,
                      None, None]
        else:            
            return [self.id[0:512], self.instant,
                          self.sdate, self.edate,
                          self.dim[0], self.member[0]]
        

    def read(self, elems):
        for e in elems:
            name = e.tag.lower().split('}')[-1]
            if name == 'context':
                self.id = e.attrib['id']
            if 'instant' == name:
                self.instant = True
                self.edate = xbrl.str2date(e.text.strip()[0:10])
            if 'startdate' == name:
                self.instant = False
                self.sdate = xbrl.str2date(e.text.strip()[0:10])
            if 'enddate' == name:
                self.instant = False
                self.edate = xbrl.str2date(e.text.strip()[0:10])
            if 'explicitmember' == name:
                if self.dim is None:
                    self.dim = []
                    self.member = []
                self.member.append(e.text.strip().replace(':','_'))
                self.dim.append(e.attrib['dimension']
                            .replace(':', '_'))