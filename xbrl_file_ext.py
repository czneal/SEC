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

class XBRLFile:
    def __init__(self, log_file = None, log_warn=None, log_err=None):
        self.log = log_file
        self.err = log_err
        self.warn = log_warn

        self.__setup_members()

    def __setup_members(self):
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
        self.file_date = None
        self.facts_df = None
        self.cntx_df = None
        self.lab = None
        self.xsd = None

    def read(self, zip_filename, file_date=None):
        self.__setup_members()
        try:
            #unpack zip file and get cal_filename, xbrl_filename, pre_filename, xsd_filename
            self.cik_adsh = (zip_filename
                         .split('/')[-1]
                         .split('.')[0])
            #only for tests
            if file_date is None:
                name = zip_filename.split('/')
                self.file_date = dt.date(int(name[2]), int(name[3]), 1)
                self.file_date = self.file_date - dt.timedelta(days=15)
            else:
                self.file_date = file_date
            #end only for tests

            self.log.write2(self.cik_adsh, "open zip archive: " + zip_filename)
            packet = xbrl.XBRLZipPacket(zip_filename, None)
            status, message = packet.open_packet()
            if not status:
                self.err.write2(self.cik_adsh, message)
                return False

            self.chapters = self.read_xsd(packet.xsd_file)
            self.read_cal(packet.cal_file, self.chapters)
            self.read_pre(packet.pre_file, self.chapters)
            self.read_labels(packet.lab_file)
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

            self.read_dei_section(root, ns)
            if self.fy == 0 or self.ddate == "" or self.fye == "":
                self.err.write2(self.cik_adsh, 'dei section is not full')
                return False

            if self.check_period_fy(root, ns, tools.xbrli) == False:
                self.err.write2(self.cik_adsh, 'ddate and fy inconsistance')
                return False

            self.read_contexts_section(root, ns, tools.xbrli)
            self.read_units_section(root, ns, tools.xbrli)
            self.read_facts_section(root, self.fact_tags, ns, prefixes)

            calc_log = io.StringIO()
            for _, c in self.chapters.items():
                c.check_cal_scheme(self.facts, calc_log)
                #c.update_pre_values(self.facts)
            calc_log.flush()
            calc_log.seek(0)
            self.calc_log = calc_log.read()

            if self.calc_log != "":
                self.trusted = False
                self.err.write2(self.cik_adsh, "calculation error!")
                self.err.write2(self.cik_adsh, self.calc_log)
            else:
                self.trusted = True

            self.log.write2(self.cik_adsh, 'start find contexts...')
            self.make_contexts_facts(18)
            self.find_instant_context(8)
            self.find_noninstant_context('is', 8)
            self.find_noninstant_context('cf', 8)
            self.log.write2(self.cik_adsh, 'end find contexts...ok')

            if 'bs_err' in self.cntx:
                self.err.write2(self.cik_adsh, 'bs_err:' + str(self.cntx['bs_err']))
            if 'is_err' in self.cntx:
                self.err.write2(self.cik_adsh, 'is_err:' + str(self.cntx['is_err']))
            if 'cf_err' in self.cntx:
                self.err.write2(self.cik_adsh, 'cf_err:' + str(self.cntx['cf_err']))
            if 'bs_sum' in self.cntx:
                self.err.write2(self.cik_adsh, 'bs_sum:' + str(self.cntx['bs_sum']))
            if 'is_sum' in self.cntx:
                self.err.write2(self.cik_adsh, 'is_sum:' + str(self.cntx['is_sum']))
            if 'cf_sum' in self.cntx:
                self.err.write2(self.cik_adsh, 'cf_sum:' + str(self.cntx['cf_sum']))
        except:
            self.err.write2(self.cik_adsh, "unexpected error while reading:"+zip_filename)
            self.err.write_tb2(self.cik_adsh, sys.exc_info())
            return False

        return True

    def used_tags(self, only_sta=True):
        fact_tags = set()
        for _, c in self.chapters.items():
            fact_tags.update(c.get_cal_tags(only_sta))
            fact_tags.update(c.get_pre_tags(only_sta))
        return fact_tags

    def get_dimentions(self, only_sta=True):
        dim = set()
        dim.add(None)
        for _, c in self.chapters.items():
            if self.ms.match(c.label):
                dim.update(c.get_dimentions(only_sta))
        return dim

    def get_dim_members(self, only_sta=True):
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

        self.ddate = ""
        node = root.find("./dei:DocumentPeriodEndDate", ns)
        if node is not None:
            self.ddate = node.text.strip()
            self.ddate_context = node.attrib["contextRef"].strip()
        else:
            self.err.write2(self.cik_adsh, "dei:DocumentPeriodEndDate not found")

        self.isin = None
        node = root.find("./dei:TradingSymbol", ns)
        if node is not None and node.text is not None:
            self.isin = node.text.upper().strip()
            if len(self.isin)>12: self.isin = None

        self.fy = 0
        node = root.find("./dei:DocumentFiscalYearFocus", ns)
        if node is not None:
            try:
                self.fy = int(node.text.strip())
            except ValueError:
                self.fy = 0
            if self.fy < 2000 or self.fy > 2100:
                self.fy = dt.date.today().year
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
            if cntx.dim is not None and len(cntx.dim)>1:
                continue
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

    def make_contexts_facts(self, day_tolerance=8):
        contexts = pd.DataFrame(data = [e.aslist() for (n, e) in self.contexts.items()],
                        columns = ['context', 'instant', 'sdate', 'edate',
                                   'dim', 'member'])
        contexts['edist'] = contexts['edate'].apply(self.fey_dist)
        contexts['sdist'] = contexts['sdate'].apply(self.fey_dist)
        contexts = contexts[((contexts['edist']<=day_tolerance) |
                            (contexts['sdist']<=day_tolerance)) &
                            (contexts['edate']<=self.file_date)]


        facts = pd.DataFrame(data = [fact.aslist() for ((f, c), fact) in self.facts.items()],
                             columns=['tag', 'value', 'uom', 'context'])

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
            self.cntx['bs'] = None
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
            self.cntx['bs'] = cntx_grp.iloc[0]['context']
            return

        filtered = cntx_grp[cntx_grp['dim'].isnull()]
        if filtered.shape[0] == 1:
            self.cntx['bs'] = filtered.iloc[0]['context']
            return
        if filtered.shape[0] > 1:
            self.cntx['bs_err'] = filtered['context'].tolist()
            return

        #filtered.shape[0] == 0:
        filtered = cntx_grp[cntx_grp['member'].str.contains('successor', case=False)]
        if filtered.shape[0] == 1:
            self.cntx['bs'] = filtered.iloc[0]['context']
            return
        if filtered.shape[0] > 1:
            self.cntx['bs_err'] = filtered['context'].tolist()
            return
        #filtered.shape[0] == 0:
        filtered = cntx_grp[cntx_grp['member'].str.contains('parentcompany', case=False)]
        if filtered.shape[0] == 1:
            self.cntx['bs'] = filtered.iloc[0]['context']
        if filtered.shape[0] > 1:
            self.cntx['bs_err'] = filtered['context'].tolist()
        if filtered.shape[0] == 0:
            self.cntx['bs_sum'] = cntx_grp['context'].tolist()

        return

    def find_noninstant_context(self, tolerance_days=8):
        def add_to_dict(d, sheet, what):
            if sheet in d:
                d[sheet].append(what)
            else:
                d[sheet] = what

        for _, chapter in self.chapters.items():
            sheet = ''
            if self.ms.match_is(chapter.label):
                sheet = 'is'
            if self.ms.match_is(chapter.label):
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
                self.cntx[sheet] = cntx_grp.iloc[0]['context']
            elif cntx_grp.shape[0] > 1:
                filtered = cntx_grp[cntx_grp['dim'].isnull()]
                if filtered.shape[0] == 1:
                    self.cntx[sheet] = filtered.iloc[0]['context']
                elif filtered.shape[0] > 1:
                    self.cntx[sheet + '_err'] = filtered['context'].tolist()
                else:
                    filtered = cntx_grp[cntx_grp['member'].str.contains('parentcompany', case=False)]
                    if filtered.shape[0] == 1:
                        self.cntx[sheet] = filtered.iloc[0]['context']
                    elif filtered.shape[0] > 1:
                        self.cntx[sheet + '_err'] = filtered['context'].tolist()
                    else:
                        filtered = cntx_grp[cntx_grp['member'].str.contains('successor', case=False)]
                        if filtered.shape[0] == 1:
                            self.cntx[sheet] = filtered.iloc[0]['context']
                        elif filtered.shape[0] > 1:
                            self.cntx[sheet + '_err'] = filtered['context'].tolist()
                        else:
                            self.cntx[sheet + '_sum'] = cntx_grp['context'].tolist()
            else:
                #merge contexts by date or find short periods
                f = facts[(np.abs(facts['edate'] - edate) <= tolerance) |
                          (np.abs(facts['sdate'] - sdate) <= tolerance)]
                cntx_grp = self.noninstant_prep(f)
                if cntx_grp.shape[0] == 1:
                    self.cntx[sheet] = cntx_grp.iloc[0]['context']
                elif cntx_grp.shape[0] == 0:
                    self.cntx[sheet] = None
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

class Fact(object):
    def __init__(self, elem, source):
        self.tag, self.context, self.value, self.uom, self.decimals, _  = Fact.read(elem)
        self.source = source.lower()
        self.name = source+":"+self.tag

    def aslist(self):
        return [self.name, self.value, self.uom, self.context]

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
    attr = ['id', 'instant', 'sdate', 'edate', 'axis', 'dim']
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
            x = [self.id, self.instant,
                      self.sdate, self.edate,
                      None, None]
        else:
            x = [self.id, self.instant,
                          self.sdate, self.edate,
                          self.dim[0], self.member[0]]
        return x


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


log = LogFile("outputs/log.txt", append=False)
err = LogFile("outputs/err.txt", append=False)
warn = LogFile("outputs/warn.txt", append=False)
r = XBRLFile(log, warn, err)

files = ['d:/sec/2018/02/0001043604-0001043604-18-000011.zip', #contextRef not in fact attrib
        'd:/sec/2013/03/0001449488-0001449488-13-000018.zip', #role_uri not in chapters
        'd:/sec/2014/08/0000754811-0000754811-14-000086.zip', #labels
        'd:/sec/2015/01/0000317889-0001437749-15-000640.zip', #cicles in structure
        'd:/sec/2013/03/0000886136-0001144204-13-015502.zip', #cicles in structure
        'd:/sec/2016/02/0000317889-0001437749-16-026192.zip', #cicles in structure
        'd:/sec/2016/03/0000317889-0001437749-16-026192.zip', #cicles in structure
        'd:/sec/2016/06/0000890491-0001193125-16-630221.zip', #cicles in structure
        'd:/sec/2017/04/0001541354-0001493152-17-004516.zip', #cicles in structure
        'd:/sec/2013/03/0001393066-0001193125-13-087936.zip', #is problem
        'd:/sec/2013/06/0000014693-0000014693-13-000038.zip' #lab problem
        ]

for file in files[-2:-1]:
    r.read('z'+file[1:], None)
    print(len(r.chapters), r.facts_df.shape, r.lab.shape, r.cntx)

log.close()
err.close()
warn.close()



