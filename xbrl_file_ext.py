# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 11:44:49 2017

@author: Asus
"""

import xbrl_file as xbrl
import datetime as dt
import re
import os
import json
import io
import sys
import pandas as pd
import numpy as np
import database_operations as do
from settings import Settings

class XBRLFile:    
    def __init__(self, log_file = None):
        self.log = log_file
        self.facts = {}
        self.contexts = None
        self.chapters = None
        self.trusted = True
        self.fact_tags = None
        self.units = None
    
    def read(self, zip_filename, xbrl_filename):
        try:
            #unpack zip file and get cal_filename, xbrl_filename, pre_filename, xsd_filename
            packet = xbrl.XBRLZipPacket(zip_filename, xbrl_filename)
            if not packet.open_packet(self.log):
                return False
            
            xsd_file = XSDFile(self.log)
            cal_file = CALFile(self.log)
            pre_file = PREFile(self.log)
            
            self.chapters = xsd_file.read(packet.xsd_file)        
            cal_file.read(packet.cal_file, self.chapters)
            pre_file.read(packet.pre_file, self.chapters)
            
            tools = xbrl.XmlTreeTools()
            tools.read_xml_tree(packet.xbrl_file)
            
            root, ns = tools.root, tools.ns
            prefixes = {}
            self.fact_tags = self.used_tags()
            for t in self.fact_tags:
                prefix = t.split(":")[0]
                if prefix in ns:
                    prefixes[ns[prefix]] = prefix
                    
            self.read_dei_section(root, ns)
            if self.fy == 0 or self.ddate == "" or self.fye == "":
                self.log.write_to_log("Dei section is not full")
                return False
            if self.check_period_fy(root, ns, tools.xbrli) == False:
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
                self.log.write_to_log("calculation error!")
                self.log.write_to_log(self.calc_log)
            else:
                self.trusted = True
            
            self.fact_tags = self.used_tags()
        except:
            self.log.write_to_log("unexpected error while reading:"+zip_filename)
            self.log.write_to_log(str(sys.exc_info()))
            return False
        return True
    
    def used_tags(self):
        fact_tags = set()        
        for _, c in self.chapters.items():
            fact_tags.update(c.get_cal_tags())
            fact_tags.update(c.get_pre_tags())
        return fact_tags
    
    def structure_dumps(self):
        dump = {}
        for _, c in self.chapters.items():
            js = c.json()
            if js is not None:
                dump[c.label] = js
                
        return json.dumps(dump)
            

    def read_dei_section(self, root, ns):
        self.log.write_to_log("start reading dei section...")
        self.fye = ""
        node = root.find("./dei:CurrentFiscalYearEndDate", ns)
        if node is not None:
            self.fye = node.text.strip().replace("-","")
        else:
            self.log.write_to_log("  dei:CurrentFiscalYearEndDate not found")
            
        self.ddate = ""
        node = root.find("./dei:DocumentPeriodEndDate", ns)
        if node is not None:
            self.ddate = node.text.strip()
            self.ddate_context = node.attrib["contextRef"].strip()
        else:
            self.log.write_to_log("  dei:DocumentPeriodEndDate not found")
        
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
            self.log.write_to_log("  dei:DocumentFiscalYearFocus not found")
            
        self.log.write_to_log("end reading dei section")
        
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
            self.log.write_to_log("Period and fy inconsistence can not be solved")
        return check
        

    def read_units_section(self, root, ns, xbrli):
        self.log.write_to_log("start reading contexts...")
        self.units = {}
        
        for elem in root.findall("./"+xbrli+"unit"):
            name = elem.attrib["id"].lower().strip()
            div = list(elem.iter(xbrli+"divide"))
            if len(div) > 0:
                num = div[0].find(xbrli+"unitNumerator")
                denum = div[0].find(xbrli+"unitDenominator")
                m1 = num.find(xbrli+"measure").text.lower().strip()
                m2 = denum.find(xbrli+"measure").text.lower().strip()
                if m1.endswith("usd") and m2.endswith("shares"):
                    self.units[name] = "usd"
            else:
                measure = list(elem.iter(xbrli+"measure"))
                m = measure[0].text.lower().strip()
                if m.endswith("usd"):
                    self.units[name] = "usd"
                if m.endswith("shares"):
                    self.units[name] = "shares"
                if m.endswith("pure"):
                    self.units[name] = "pure"
    
    def compare_dates(d1, d2):
        d1 = dt.datetime(int(d1.split('-')[0]), 
                         int(d1.split('-')[1]), int(d1.split('-')[2]))
        d2 = dt.datetime(int(d2.split('-')[0]), 
                         int(d2.split('-')[1]), int(d2.split('-')[2]))
        if abs((d1-d2).days) <= 10:
            return True
        return False
        
    def read_contexts_section(self, root, ns, xbrli):
        self.log.write_to_log("start reading contexts...")
        
               
        self.contexts = {} 
        for elem in root.findall("./"+xbrli+"context"):
            cntx = Context(elem.iter())
            if cntx.dim is not None and len(cntx.dim)>1:
                continue
            self.contexts[cntx.id] = cntx
        
#        other = {}
#        #find dates for context filtering        
#        fin = dt.date(int(self.ddate[0:4]), int(self.ddate[5:7]), int(self.ddate[8:10]))        
#        
#        #read full year contexts without segments        
#        duration = 0
#        st = None
#        c_name = ""
#        for elem in root.findall("./"+xbrli+"context"):            
#            if len(list(elem.iter(xbrli+"segment"))) > 0:
#                continue
#                        
#            period = elem.find(xbrli+"period")            
#            if len(list(period)) == 1:
#                instant = period.find(xbrli+"instant")
#                if instant is None:
#                    continue
#                d = period[0].text.strip()                
#                if XBRLFile.compare_dates(d, self.ddate):
#                    self.contexts[elem.attrib["id"]] = [d]
#            else:
#                startDate = period.find(xbrli+"startDate")
#                if startDate is None:
#                    continue
#                
#                d1 = period[0].text.strip()
#                d2 = period[1].text.strip()
#                other[elem.attrib["id"]] = [d1,d2]
#                if XBRLFile.compare_dates(d2, self.ddate):
#                    d = dt.date(int(d1[0:4]),int(d1[5:7]),int(d1[8:10]))
#                    length = (fin-d).days
#                    if length > duration and length < 380:
#                        st = d
#                        duration = length
#                        c_name = elem.attrib["id"]
#        if st is not None:
#            self.contexts[c_name] = [str(st), str(fin)]
#            other.pop(c_name)
#            for name, [d1,d2] in other.items():
#                d1 = dt.date(int(d1[0:4]),int(d1[5:7]),int(d1[8:10]))
#                d2 = dt.date(int(d2[0:4]),int(d2[5:7]),int(d2[8:10]))
#                if st < d1:
#                    st = d1
#                if fin > d2:
#                    fin = d2
#                if (fin-st).days > 320:
#                    self.contexts[name] = [str(d1),str(d2)]
#        else:
#            self.log.write_to_log("  entity contexts not found")
#            
#        if len(self.contexts) != 2:
#            self.log.write_to_log("  missing contexts")
            
        self.log.write_to_log("end reading contexts...ok")
        
        
    def read_facts_section(self, root, fact_tags, ns, prefixes):
        self.log.write_to_log("start reading facts section...")
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
        self.log.write_to_log("end reading facts section...ok")
        
    def find_contexts(self):
        data = []
        
        for _, cntx in self.contexts.items():
            data.extend(cntx.aslist())
            
        df = pd.DataFrame(data, columns=Context.attr)
#        df['sdate'] = pd.to_datetime(df['sdate'])
#        df['edate'] = pd.to_datetime(df['edate'])
        
        data = [[name, context, fact.value] for ((name,context),fact) in r.facts.items()]
        facts = pd.DataFrame(data, columns=['tag', 'context', 'value'])
        
        edate = xbrl.str2date(r.ddate)
        sdate = edate - dt.timedelta(days=365.2425)
        tolerance = dt.timedelta(days=10)
        
        info = pd.merge(facts, df, 'inner', left_on='context', right_on='id')
        
        instant = info[info['instant'] & 
                       (np.abs(info['edate'] - edate) <= tolerance)]
        c = instant.groupby('context')['context'].count().sort_values().index[-1]
        #instant = instant[(instant['context'] == c)]
        self.instant_cntx = c
        
        noninstant = info[~info['instant'] & 
                       ((np.abs(info['edate'] - edate) <= tolerance) |
                        (np.abs(info['sdate'] - sdate) <= tolerance))]
                       
        c = noninstant.groupby('context')['context'].count().sort_values().iloc[-2:]
        
        v1 = c.iloc[0]
        v2 = c.iloc[1]
        c = c.index
        if 2*abs(v1-v2)/(v1+v2) > 0.25:
            self.noninstant_cntx = [c[1]]
            #noninstant = noninstant[(noninstant['context'] == c[1])]
        else:
            self.noninstant_cntx = [c[0], c[1]]
            noninstant = noninstant[(noninstant['context'] == c[0]) |
                    (noninstant['context'] == c[1])]
        
    
class LogFile(object):
    def __init__(self, filename):
        self.log_file = open(filename, "w")
        
    def write_to_log(self, s, end = os.linesep):
        self.log_file.write(s + end)
        self.log_file.flush()
        
    def close(self):
        self.log_file.flush()
        self.log_file.close()
        del self.log_file
        
class Node(object):
    """Represent node in calculation tree
    name - us_gaap or custom name
    source - whether it coms from us_gaap, or custom
    children - node children
    parent - node parent, after using __organize() function in CALFile has no meaning
    """
    def __init__(self, tag, source):
        self.name = source+":"+tag
        self.tag = tag
        self.source = source.lower()
        self.children = {}
        self.parent = None
        self.value = None
        
    def enum_children(node):
        for _, c in node.children.items():
            for n in Node.enum_children(c[0]):
                yield n
        yield node
        
    def print_children(self, spaces):
        """print children with structure"""
        for _, c in self.children.items():
            print(spaces, c[0].name, c[1], c[0].value)
            c[0].print_children(spaces+" ")
            
    def calculate(self, facts, calc_log):
        self.value = None
        for _, c in self.children.items():
            c[0].calculate(facts, calc_log)
            if c[0].value is not None:
                self.value = (0.0 if self.value is None else self.value) + (c[0].value)*c[1]
        
        if self.name not in facts:
            return
        
        if len(self.children) == 0 or self.value is None:
            self.value = facts[self.name].value
        
        if self.value != facts[self.name].value:
            difference = facts[self.name].value - self.value
            calc_log.write("{0}\t{1}\t{2}\t{3}\n".format(self.name, self.value, facts[self.name].value, difference))
            
            expected = {}
            for _, f in facts.items():
                if f.value == difference:
                    expected[f.name] = f.value
            if len(expected)>0:
                calc_log.write("expected: {0}\n".format(expected))
    
    def json(self):
        retval = {"name":self.name, "tag":self.tag, "source":self.source, "weight": 0.0, "children": None}
        if len(self.children) == 0:
            return retval
        retval["children"] = {}
        for name, c in self.children.items():            
            retval["children"][name] = c[0].json()
            retval["children"][name]["weight"] = c[1]
            
        return retval    
        
class Chapter(object):
    """Represents chapter in terms of financial report
    role_uri - RoleURI from xsd file
    chap_id - id from xsd file
    chapter - chapter type, from whitch part of report it comes. "sta" - Statement, "doc" - document and so on
    label - as it represent in final report
    nodes - {node_id:[Node, weight]}, represent all tags using in calc scheme, node_id - is a "label" for node in "loc" tag in cal, pre or lab file 
    nodes_pre - {node_id: Node} same as nodes
    """
    def __init__(self, role_uri, chap_id, chapter, label):
        self.role_uri = role_uri
        self.chap_id = chap_id
        self.chapter = chapter
        self.label = label
        self.nodes = {}
        self.nodes_pre = {}    
    def read_cal(self, calcLink, empty, xlink):
        """reads chapter content from cal file"""
#        if self.chapter != 'sta':
#            return
        
        nodes_ids = {}
        
        for loc in calcLink.iter(empty+"loc"):
            loc_id = loc.attrib[xlink+"label"].strip()
            href = loc.attrib[xlink+"href"].strip()
            href = href.split("#")[-1]
            source = href.split("_")[0].lower()
            
            tag = href.split("_")[-1]
            n = Node(tag, source)
            nodes_ids[loc_id] = n.name
            self.nodes[n.name] = n
        
        #find all calculation arcs
        for calcArc in calcLink.iter(empty+"calculationArc"):
            p_id = calcArc.attrib[xlink+"from"].strip()
            c_id = calcArc.attrib[xlink+"to"].strip()
            c_name = nodes_ids[c_id]
            p_name = nodes_ids[p_id]
            weight = float(calcArc.attrib["weight"])
            
            self.nodes[p_name].children[c_name] = [self.nodes[c_name], weight]
            self.nodes[c_name].parent = self.nodes[p_name]
            
    def read_pre(self, preLink, empty, xlink):
        """reads chapter content from pre file, reads chapters which comes from Statement section of report"""
        if self.chapter != "sta":
            return
        
        nodes_ids = {}
        
        for loc in preLink.iter(empty+"loc"):
            loc_id = loc.attrib[xlink+"label"].strip()
            href = loc.attrib[xlink+"href"].strip()
            href = href.split("#")[-1]
            source = href.split("_")[0].lower()            
            tag = href.split("_")[-1]
            
            if tag.endswith("Abstract"):
                continue            
            if loc_id in nodes_ids:
                continue
            
            n = Node(tag, source)
            if n.name in self.nodes:
                continue
            
            nodes_ids[loc_id] = n.name
            self.nodes_pre[n.name] = n
    
    def get_pre_tags(self):
        """Returns set of tags shown in Statements sections of report, 
        tags stored as "us-gaap:TagName", "custom:CustomTagName" """
#        if self.chapter != 'sta':
#            return set()
        
        tags = set()
        
        for name, node in self.nodes_pre.items():
            if name not in self.nodes:
                tags.add(node.name)
            else:
                for n in Node.enum_children(node):
                    tags.add(n.name)
        return tags
    
    def get_cal_tags(self):
        """Returns set of tags shown in calculation scheme
        """
#        if self.chapter != 'sta':
#            return set()
        
        tags = set()
        for name, node in self.nodes.items():
            if node.parent is None:
                for c in Node.enum_children(node):
                    tags.add(c.name)
        return tags
                
    def print_self(self):
        if self.chapter != "sta":
            return
        if len(self.nodes) == 0 and len(self.nodes_pre) == 0:
            return
        
        print("chapter:", self.label)
        for _, node in self.nodes.items():
            if node.parent is None:
                print(" "+node.name)
                node.print_children("  ")
        for _, node in self.nodes_pre.items():
            print(" "+ node.name, node.value, "Presentation")
            
    def check_cal_scheme(self, facts, calc_log):
        if self.chapter != "sta":
            return
        
        for _, n in self.nodes.items():
            if n.parent is None:
                n.calculate(facts, calc_log)
                
    def update_pre_values(self, facts):
        for name, f in facts.items():
            if name not in self.nodes_pre:
                continue
            self.nodes_pre[name].value = f.value
        
        deleted = []
        for name in self.nodes_pre.keys():
            if self.nodes_pre[name].value == None:
                deleted.append(name)
        for name in deleted:
            self.nodes_pre.pop(name)
    
    def json(self):
        if self.chapter != "sta" or len(self.nodes) == 0:
            return None
        
        retval = {}
        for name, n in self.nodes.items():
            if n.parent is None:
                retval[name] = n.json()
#        for name, n in self.nodes_pre.items():
#            retval[name] = n.json()
            
        return retval
                    
class XSDFile(object):
    def __init__(self, log):
        self.log = log
        
    def read(self, xsd_file):
        try:
            chapters = {}
            self.log.write_to_log("start reading xsd scheme...")
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
                
            for role in root.iter(link+"roleType"):
                rol_def = role.find(link+"definition")
                chapter = ""
                if matchDocument.match(rol_def.text):
                    chapter = "doc"
                elif matchStatement.match(rol_def.text):
                    if matchParenthetical.match(rol_def.text):
                        chapter = "sta"
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
            
            self.log.write_to_log("end reading xsd scheme, status ok")
            return chapters
        except:
            return None
        
class CALFile(object):
    def __init__(self, log):
        self.log = log
    
    def read(self, cal_filename, chapters):
        self.log.write_to_log("start reading calculation scheme...")
        
        tools = xbrl.XmlTreeTools()
        tools.read_xml_tree(cal_filename)
        
        xlink, empty = tools.xlink, tools.empty
        root = tools.root
        
        for calcLink in root.iter(empty+"calculationLink"):
            role_uri = calcLink.attrib[xlink+"role"].strip()
            chapters[role_uri].read_cal(calcLink, empty, xlink)
            
        #self.__organize(chapters)            
        self.log.write_to_log("end reading calculation scheme...ok")
    
    def __organize(self, chapters):
        for c, chapter in chapters.items():
            if chapter.chapter != "sta":
                continue
#            global organize
#            for c1, chapter1 in chapters.items():
#                if c1==c:
#                    continue
#                for n, node in chapter.nodes.items():
#                    if len(node.children) != 0:
#                        continue
#                    for m, node1 in chapter1.nodes.items():
#                        if len(node1.children) == 0:
#                            continue
#                        if node1.name == node.name:
#                            node.children = node1.children.copy()

#           local organize
            for c1, chapter1 in chapters.items():
                if chapter1.chapter != "sta":
                    continue
                if c1==c:
                    continue
                
                for n, node in chapter.nodes.items():
                    if len(node.children) != 0:
                        continue
                    for m, node1 in chapter1.nodes.items():
                        if len(node1.children) == 0:
                            continue
                        if node1.name == node.name:
                            node.children = node1.children.copy()
            
            
class PREFile(object):
    def __init__(self, log):
        self.log = log
        
    def read(self, pre_filename, chapters):
        self.log.write_to_log("start reading presentation scheme...")
        
        tools = xbrl.XmlTreeTools()
        tools.read_xml_tree(pre_filename)
        xlink, empty = tools.xlink, tools.empty
        root = tools.root
                
        for preLink in root.iter(empty+"presentationLink"):
            role_uri = preLink.attrib[xlink+"role"].strip()
            chapters[role_uri].read_pre(preLink, empty, xlink)
            
        self.log.write_to_log("end reading presentation scheme...ok")
        
   
class Fact(object):
    def __init__(self, elem, source):
        self.tag, self.context, self.value, self.uom, self.decimals, _  = Fact.read(elem)
        self.source = source.lower()
        self.name = source+":"+self.tag
        
    def read(elem):        
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
        

#log = LogFile("outputs/l.txt")
#r = XBRLFile(log)
#r.read("z:/sec/2014/02/0000849213-0000849213-14-000010.zip", 
#       "pcl-20131231.xml")
#
#log.close()


class Context(object):
    attr = ['id', 'instant', 'sdate', 'edate', 'axis', 'dim']
    def __init__(self, elems):
        self.instant = None
        self.sdate = None
        self.edate = None
        self.axis = None
        self.dim = None
        self.id = None
        
        self.read(elems)
        
    def __str__(self):
        return ("(id:{0}, instant:{1}, startDate:{2}, endDate: {3}, axis:{4}, dimension:{5})"
                .format(self.id, self.instant, self.sdate, self.edate,
                        self.axis, self.dim))
    
    def aslist(self):
        x = []
        if self.axis is None:
            x.append([self.id, self.instant, 
                      self.sdate, self.edate,
                      None, None])
        else:
            for a, d in zip(self.axis, self.dim):
                x.append([self.id, self.instant, 
                          self.sdate, self.edate,
                          a, d])
        return x
        
    
    def read(self, elems):
        for e in elems:
            name = e.tag.lower().split('}')[-1]
            if name == 'context':
                self.id = e.attrib['id']
            if 'instant' == name:
                self.instant = True
                self.edate = xbrl.str2date(e.text)
            if 'startdate' == name:
                self.instant = False
                self.sdate = xbrl.str2date(e.text)
            if 'enddate' == name:
                self.instant = False
                self.edate = xbrl.str2date(e.text)
            if 'explicitmember' == name:
                if self.dim is None:
                    self.dim = []
                    self.axis = []
                self.axis.append(e.text.strip().replace(':','_'))
                self.dim.append(e.attrib['dimension']
                            .replace(':', '_'))


log = LogFile("outputs/l.txt")
r = XBRLFile(log)
#file = ('z:/sec/2018/02/0000030554-0000030554-18-000002.zip',
#        'dd-20171231.xml')
#file2 = ("z:/sec/2014/03/0000031235-0001193125-14-106388.zip", 
#       "kodk-20131231.xml")
#file3 = ('z:/sec/2014/02/0000012927-0000012927-14-000004.zip',
#         'ba-20131231.xml')
#
#r.read(file2[0], file2[1])

data = []

try:
    con = do.OpenConnection()
    cur = con.cursor(dictionary=True)
    cur.execute('select adsh, cik, file_link, contexts from reports ' +
                ' where fin_year between {0} and {1}'
                .format(Settings.years()[0], Settings.years()[1]) + 
                Settings.select_limit())
    
    for index, row in enumerate(cur):
        print('\rProcessed with:{0}...'.format(index+1), end='')
        r.read("z" + row['file_link'][1:], None)
        r.find_contexts()
        contexts = json.loads(row['contexts'])
        check = True
        if not r.instant_cntx in contexts:
            check = False
        for c in r.noninstant_cntx:
            if not c in contexts:
                check = False
        rep = [row['adsh'], row['cik'], row['file_link'], check, contexts.keys(),
               r.instant_cntx, r.noninstant_cntx]
        data.append(rep)
        
    print('ok')
    df = pd.DataFrame(data, columns=['adsh', 'cik', 'file_link', 'check',
                                     'old_cntx', 'instant', 'noninstant'])
    
        
finally:
    con.close()
    log.close()
#df['end'] = np.abs(df['edate'] - edate) <= diff
#df['start'] = np.abs(df['sdate'] - sdate) <= diff
#end = df[df['end']  & (df['instant'] != True)].sort_values('sdate')
#start = df[df['start'] & (df['instant'] != True)].sort_values('edate')
#end['date'] = pd.to_datetime(end['sdate'])
#start['date'] = pd.to_datetime(start['edate'])
#
#pp = pd.merge_asof(start, end, on='date',
#                   tolerance = pd.Timedelta(days=5),
#                   direction='nearest')
        

