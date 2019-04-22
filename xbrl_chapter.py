# -*- coding: utf-8 -*-
"""
Created on Wed Jan  9 10:30:58 2019

@author: Asus
"""

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
        self.dim = set()
        self.member = set()

    def read_cal(self, calcLink, empty, xlink, only_sta = False):
        """reads chapter content from cal file"""
        if only_sta and self.chapter != 'sta':
            return

        nodes_ids = {}

        for loc in calcLink.iter(empty+"loc"):
            loc_id = loc.attrib[xlink+"label"].strip()
            href = loc.attrib[xlink+"href"].strip()
            href = href.split("#")[-1]
            source = href.split("_")[0].lower()

            tag = href.split("_")[-1]
            n = Node(tag, source, in_cal=True)
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

    def read_pre(self, preLink, empty, xlink, only_sta = False):
        """reads chapter content from pre file, reads chapters which comes from Statement section of report"""
        if only_sta and self.chapter != "sta":
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
            if tag.endswith("Axis"):
                self.dim.add(href)
                continue
            if tag.endswith('Member'):
                self.member.add(href)
                continue
            if tag.endswith('Domain'):
                self.member.add(href)
                continue

            if loc_id in nodes_ids:
                continue

            n = Node(tag, source, in_pre=True)
            if n.name in self.nodes:
                self.nodes[n.name].in_pre = True
                continue

            nodes_ids[loc_id] = n.name
            self.nodes_pre[n.name] = n

    def get_dimentions(self, only_sta):
        if only_sta and self.chapter != 'sta':
            return set()

        return self.dim

    def get_members(self, only_sta):
        if only_sta and self.chapter != 'sta':
            return set()

        return self.member

    def get_pre_tags(self, only_sta):
        """Returns set of tags shown in presentation file,
        tags stored as "us-gaap:TagName", "custom:CustomTagName"
        """
        if only_sta and self.chapter != 'sta':
            return set()

        tags = set()
        for _, node in list(self.nodes_pre.items()) + list(self.nodes.items()):
            if node.in_pre:
                tags.add(node.name)

        return tags

    def get_cal_tags(self, only_sta):
        """Returns set of tags shown in calculation file
        tags stored as "us-gaap:TagName", "custom:CustomTagName"
        """
        if only_sta and self.chapter != 'sta':
            return set()

        tags = set()
        for _, node in list(self.nodes_pre.items()) + list(self.nodes.items()):
            if node.in_cal:
                tags.add(node.name)
                
        return tags

    def print_self(self, only_sta=True):
        if only_sta and self.chapter != "sta":
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

    def check_cal_scheme(self, facts, calc_log, only_sta = True):
        if only_sta and self.chapter != "sta":
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

class Node(object):
    """Represent node in calculation tree
    name - us-gaap or custom name
    source - whether it coms from us_gaap, or custom
    children - node children
    parent - node parent, after using __organize() function in CALFile has no meaning
    """
    def __init__(self, tag, source, in_cal=False, in_pre=False):
        self.source = source.lower()
        self.name = self.source+":"+tag
        self.tag = tag        
        self.children = {}
        self.parent = None
        self.value = None
        self.in_cal = in_cal
        self.in_pre = in_pre

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