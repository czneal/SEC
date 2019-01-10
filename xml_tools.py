# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 17:41:00 2018

@author: Asus
"""

import xml.etree.ElementTree as ET

class XmlTreeTools(object):
    def __init__(self):
        self.root = None
        self.ns = None
        self.xbrli = ""
        self.link = ""
        self.empty = ""
        self.xlink = ""
        self.xsd = ""
        self.xml = ''

    def read_xml_tree(self, xml_file):
        events = ("start-ns", "start")
        ns = {}
        c = ET.iterparse(xml_file, events=events)
        c = iter(c)
        root = None
        #parse namespace names
        for event, elem in c:
            if event == "start-ns":
                ns[elem[1]] = elem[0].lower()
            if event == "start" and root == None:
                root = elem

        blank = [(v,k) for (k,v) in ns.items() if v == '']
        nss = {}
        if len(blank) >= 2:
            for k,v in ns.items():
               if v == '':
                   if k == "http://www.xbrl.org/2003/instance":
                       nss[v] = k
                   else:
                       nss[k.split('/')[-1]] = k
               else:
                   nss[v] = k
        else:
            nss = dict([(v,k) for k,v in ns.items()])
        ns = nss
        self.root = root
        self.ns = ns

        if "xbrli" in ns:
            self.xbrli = "{"+ns["xbrli"]+"}"
        elif "" in ns:
            self.xbrli = "{"+ns[""]+"}"

        if "link" in ns:
            self.link = "{" +ns["link"]+ "}"
        elif "" in ns:
            self.link = "{" +ns[""]+ "}"

        if "xlink" in ns:
            self.xlink = "{"+ns["xlink"]+"}"
        elif "" in ns:
            self.xlink = "{" +ns[""]+ "}"
        if "" in ns:
            self.empty = "{"+ns[""]+"}"
        elif "link" in ns:
            self.empty = "{"+ns["link"]+"}"
        if 'xsd' in ns:
            self.xsd = '{'+ns['xsd']+'}'
        elif 'xs' in ns:
            self.xsd = '{'+ns['xs']+'}'
        if 'xml' in ns:
            self.xml = '{'+ns['xml']+'}'
        else:
            self.xml = '{http://www.w3.org/XML/1998/namespace}'