# -*- coding: utf-8 -*-

import re

from xbrlxml.xbrlchapter import Chapter, Node
from xbrlxml.xbrlexceptions import XBRLDictException

def _enum(structure, offset, func):
    "unittested"
    if isinstance(structure, Chapter):
        for node in structure.nodes.values():
            if node.parent is None:                
                yield (structure.roleuri, 
                       func(node), 
                       1.0, 
                       offset, 
                       True if not node.children else False,
                       node)
                for item in _enum(node, offset=offset+1, func=func):
                    yield item
    if isinstance(structure, Node):
        for child in structure.children.values():
            yield (func(structure), 
                   func(child), 
                   child.arc['weight'] if 'weight' in child.arc else 1.0, 
                   offset,
                   True if not child.children else False,
                   child)
            for item in _enum(child, offset=offset+1, func=func):
                yield item
    if isinstance(structure, dict):
        for chapter in structure.values():
            assert isinstance(chapter, Chapter)
            for item in _enum(chapter, offset = offset, func=func):
                yield item

def enum(structure, leaf=False,
         outpattern = 'pcwol',
         func = lambda x: x.name):
    "unittested"
    assert re.fullmatch('[pcwoln]+', outpattern)
    
    for item in _enum(structure, offset = 0, func = func):
        if leaf and not item[4]:
            continue
        
        retval = []        
        for c in outpattern:
            if c == 'p': retval.append(item[0])
            if c == 'c': retval.append(item[1])
            if c == 'w': retval.append(item[2])
            if c == 'o': retval.append(item[3])
            if c == 'l': retval.append(item[4])
            if c == 'n': retval.append(item[5])
        yield retval
        
def find_extentions(roleuri, calc, pres, xsds):
    """
    try to find extention for calculation scheme roleuri
    return tuple({NodeLabel:roleuri, ...}, list(warning))    
    """
    extentions = {}
    warnings = []
    chapter = calc.get(roleuri, None)
    if chapter is None:
        return extentions, warnings
    
    #for every leaf node in chapter
    for n in [n for n in chapter.nodes.values() if len(n.children) == 0]:
        #look at every chapter which contains presentation 
        #and calculation scheme
        for xsd_roleuri in xsds: 
            if xsd_roleuri not in pres: continue
            if xsd_roleuri not in calc: continue
            
            c = calc[xsd_roleuri]
            #only look for not leaf nodes
            if n.label not in c.nodes or len(c.nodes[n.label].children) == 0:
                continue
            
            extnode = c.nodes[n.label]
            try:
                #check possibility of extention
                check_extention(chapter, extnode)
            except XBRLDictException as exc:
                exc.exc_data['base chapter'] = roleuri
                exc.exc_data['ext chapter'] = xsd_roleuri
                warnings.append(str(exc))
                continue
            
            extentions[n.label] = xsd_roleuri
            
    return extentions, warnings

def check_extention(chapter, newnode):
    """
    Check whether possible extend calculation scheme.
    newnode may containts children which is allready in chapter.nodes
    If them are leaf in chapter.nodes, then it's possible to
    remove them from chapter and finish extention
    If not, then throw exception
    """
    newchildren = set([elem 
                         for [elem] in enum(structure=newnode, 
                                            outpattern='c', 
                                            func=lambda x:x.label)])
    warnchildren = newchildren.intersection(set(chapter.nodes.keys()))
    
    for newchild in warnchildren:
        if len(chapter.nodes[newchild].children) != 0:
            exc_data = {'message': 'extention fails, base chapter has nodes with children',
                        'node in ext chapter': 
                            newnode.label,
                        'node in base chapter with children': 
                            newchild}            
            raise XBRLDictException(exc_data)
    
            
def extend_clac_scheme(roleuri, calc, extentions):
    """
    extend calculation scheme
    use only after find_extentions
    """
    "unittested"
    
    if roleuri not in calc:
        return
    
    chapter = calc[roleuri]
    for label, exturi in extentions.items():            
        c = calc[exturi]
        node = chapter.nodes[label]
        extnode = c.nodes[label]
        
        #remove leaf nodes from chapter which is in extnode.children
        remove_leaf_nodes(chapter, extnode)
        
        newnode = extnode.copy()
        node.children = newnode.children
        for [newlabel, newchild] in enum(node, 
                                         outpattern='cn', 
                                         func=lambda x: x.label):
            chapter.nodes[newlabel] = newchild

def remove_leaf_nodes(chapter, newnode):
    """
    remove leaf nodes from chapter which is in extnode.children
    """
    #find children to be add
    newchildren = set([elem for [elem] in enum(newnode, outpattern = 'c', func = lambda x:x.label)])
    #find children which is already in chapter.nodes
    warnchildren = newchildren.intersection(set(chapter.nodes.keys()))
        
    #remove them from parent.children
    #and remove them from chapter.nodes collection
    for newchild in warnchildren:
        if chapter.nodes[newchild].parent is not None:
            chapter.nodes[newchild].parent.children.pop(newchild)
        chapter.nodes.pop(newchild)

def makenodecopy(node):
    """
    make deep copy of node
    """
    "unittested"
    
    assert isinstance(node, Node)
    
    n = Node(tag=node.tag, version=node.version, label=node.label)
    
    if node.arc is not None:
        n.arc = node.arc.copy()
        
    for label, child in node.children.items():
        n.children[label] = child.copy()
        n.children[label].parent = n
        
    return n