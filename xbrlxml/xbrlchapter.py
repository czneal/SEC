# -*- coding: utf-8 -*-
"""
Created on Sat May 25 16:55:50 2019

@author: Asus
"""
import lxml
import re


class Node():
    """
    implements single element in scheme
    represents <loc> block with arc property for 
    <calculationArc>, <presentationArc>, <definitionArc> block
    """
    def __init__(self, version=None, tag=None, label=None):
        self.version = version
        self.tag = tag
        self.label = label
        self.arc = None
        self.parent = None
        self.children = {}        
        if version is not None and tag is not None:
            self.name = self.getname()
        else:
            self.name = None
        
    def getname(self):
        "unittested"
        return '{0}:{1}'.format(self.version, self.tag)
    
    def asdict(self):
        "unittested"
        r = {'tag': self.name}
        if self.arc is not None:
            r.update(self.arc.items())
        return r
    
    def copy(self):
        from algos.scheme import makenodecopy
        return makenodecopy(self)
    
    def __simple_eq(self, node):        
        if node is None:
            return False
        
        if (self.version == node.version and
            self.tag == node.tag and            
            self.arc == node.arc and
            self.label == node.label):
            return True
        else:
            return False
        
    def __parent_eq(self, node):
        if (node.parent is None and self.parent is None):
            return True
        if (node.parent is not None and
            self.parent is not None):
            return self.parent.__simple_eq(node.parent)
        return False
        
    def __eq__(self, node):
        if (not self.__simple_eq(node)):
            return False
        if (not self.__parent_eq(node)):
            return False
        
        if (self.children == node.children):
            return True
        
        return False

class ChapterFactory():
    def chapter(ref_type):
        "unittested"
        assert (ref_type in {'calculation', 'presentation', 'definition'}) 
        
        if ref_type == 'calculation':
            return CalcChapter()
        if ref_type == 'definition':
            return DimChapter()
        if ref_type == 'presentation':
            return DimChapter()
    
class Chapter(object):
    """
    implements single scheme 
    represents <calculationLink>, <presentationLink>, <definitionLink> block
    """
    def __init__(self, roleuri = None):
        self.roleuri = roleuri
        self.nodes = {}
        
    def update_arc(self, arc):
        "unittested"
        n_from = self.nodes[arc['from']]
        n_to = self.nodes[arc['to']]
        n_to.parent = n_from
        n_from.children[n_to.label] = n_to
        n_to.arc = arc['attrib']
        
    def getnodes(self):
        "unittested"
        return [n.asdict() for _, n in self.nodes.items()]
    
    def gettags(self):
        "unittested"
        return set([n.name for n in self.nodes.values()])
    
    def __eq__(self, chapter):
        if (chapter is None):
            return False
        if (self.roleuri != chapter.roleuri):
            return False
        if (self.nodes != chapter.nodes):
            return False
        
        return True
    
class CalcChapter(Chapter):
    def dimmembers(self):
        return [[None, None]]
    
    def dims(self):
        return [None]
    
    def extend(self, calc, pres):
        pass
    
class DimChapter(Chapter):   
    def dimmembers(self):
        "unittested"
        retval = [[None, None]]
        for n in self.nodes.values():
            if not re.match('.*member', n.tag, re.IGNORECASE):
                continue
            p = n.parent
            while True or p is None:
                if re.match('.*axis', p.tag, re.I):
                    retval.append([p.name, n.name])
                    break
                p = p.parent
                
        return retval
    
    def dims(self):
        "unittested"
        retval = [None]
        for n in self.nodes.values():
            if re.match('.*axis', n.tag, re.I):
                retval.append(n.name)
                
        return retval
        
class ReferenceParser(object):
    def __init__(self, ref_type):
        """
        ref_type = {calculation, presentation, definition}
        """
        self.__ref_type = None
        self.setreftype(ref_type)
    
    def setreftype(self, ref_type):
        assert (ref_type in {'calculation', 'presentation', 'definition'})
        self.__ref_type = ref_type
        
    def parse(self, file):
        etree = lxml.etree.parse(file)
        root = etree.getroot()
        
        chapters = {}
        
        for link in root.findall('{*}' + self.__ref_type + 'Link'):
            chapter = self.parse_chapter(link)
            if len(chapter.nodes) != 0:
                chapters[chapter.roleuri] = chapter
            
        return chapters
        
    def parse_chapter(self, link):
        "unittested"
        """
        return Chapter object
        """
        chapter = ChapterFactory.chapter(self.__ref_type)
        chapter.roleuri = link.attrib['{%s}role' % link.nsmap['xlink']]
        
        for loc in link.findall('{*}' + 'loc'):
            node = self.parse_node(loc)
            chapter.nodes[node.label] = node
            
        for arc in link.findall('{*}' + self.__ref_type + 'Arc'):
            arc = self.parse_arc(arc)
            chapter.update_arc(arc)
            
        return chapter
    
    def parse_node(self, loc):
        "unittested"
        """
        return Node object
        """
        n = Node()
        n.label = loc.attrib['{%s}label' % loc.nsmap['xlink']]
        href = loc.attrib['{%s}href' % loc.nsmap['xlink']].split('#')[-1].split('_')
        n.version = href[0]
        n.tag = href[1]
        n.name = n.getname()
        
        return n
        
    def parse_arc(self, arc):
        "unittested"
        """
        return dict of attributes
        """
        arcdict = {'attrib':{}}
        for attr, value in arc.attrib.items():
            attr = re.sub('{.*}', '', attr, re.IGNORECASE)
            
            if attr.endswith('from'):
                arcdict['from'] = value
                continue
            if attr.endswith('to'):
                arcdict['to'] = value
                continue            
            
            if (attr == 'arcrole' or
                attr == 'preferredLabel'):
                arcdict['attrib'][attr] = value.split('/')[-1]            
            else:
                arcdict['attrib'][attr] = value
            
        return arcdict
        
if __name__ == '__main__':
    pass
    

    
    
