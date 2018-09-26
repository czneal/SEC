# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 12:58:44 2017

@author: Asus
"""
import classificators as cl

def enumerate_tags_basic_leaf(structure, tag = None, chapter = None, offset="", delim="  "):
    """
    returns tuple: (parent, child, child_weight, child_structure, offset, leaf)
    """
    for node in _enumerate_tags_basic(structure, tag, chapter):
        if "children" not in node[3] or node[3]["children"] is None:
            yield node + (True,)
        else:
            yield node + (False,)
            
def _enumerate_tags_basic(structure, tag = None, chapter = None, offset="", delim="  "):
    """
    returns tuple: (parent, child, child_weight, child_structure, offset)
    """
    root = None
    if "children" in structure:
        root = structure["children"]
    else:
        for retval in enumerate_chapter_tags(structure, chapter):
            if tag is None or tag.lower() == retval[1].lower():
                yield retval + (offset,)
            for tags in _enumerate_tags_basic(retval[3], tag, chapter, offset+delim, delim):
                yield tags
                    
    if root is not None:
        for name, child in root.items():
            if tag is None or tag.lower() == name.lower():
                yield (structure["name"], name, child["weight"], child, offset)
            for retval in _enumerate_tags_basic(child, tag, chapter, offset+delim, delim):
                yield retval
                
def enumerate_tags_basic(structure, tag = None, chapter = None):
    for node in _enumerate_tags_basic(structure, tag, chapter):
        yield node[0:4]
#    root = None
#    if "children" in structure:
#        root = structure["children"]
#    else:
#        for retval in enumerate_chapter_tags(structure, chapter):
#            if tag is None or tag.lower() == retval[1].lower():
#                yield retval
#            for tags in enumerate_tags_basic(retval[3], tag, chapter):
#                yield tags
#                    
#    if root is not None:
#        for name, child in root.items():
#            if tag is None or tag.lower() == name.lower():
#                yield (structure["name"], name, child["weight"], child)
#            for (parent_name, tag_name, tag_weight, tag_data) in enumerate_tags_basic(child, tag, chapter):
#                yield (parent_name, tag_name, tag_weight, tag_data)
                
def enumerate_tags_parent_child(structure, tag=None, chapter=None):
    for (p, c, _, _) in enumerate_tags_basic(structure, tag, chapter):
        yield (p, c)

def enumerate_tags_parent_child_leaf(structure, tag=None, chapter=None):
    for (p, c, _, c_data) in enumerate_tags_basic(structure, tag, chapter):
        if "children" not in c_data or c_data["children"] is None:
            yield (p, c, True)
        else:
            yield (p, c, False)
            
def enumerate_chapter_tags(structure, chap_id = None):
    for chapter, chapter_data in structure.items():
        if chap_id is None or cl.ChapterClassificator.match(chapter) == chap_id:
            for tag_name, tag_data in chapter_data.items():
                yield (chapter, tag_name, tag_data["weight"], tag_data)

#old ones  
def enumerate_tags(structure, tag = None):
    root = None
    if "children" in structure:
        root = structure["children"]
    else:
        root = structure
        
    if root is not None:
        for name, child in root.items():
            if tag is None or tag == name:
                yield name, child
            for p, c in enumerate_tags(child, tag):
                yield p, c

def enumerate_tags_weight(structure, tag = None):
    root = None
    if "children" in structure:
        root = structure["children"]
    else:
        root = structure
        
    if root is not None:
        for name, child in root.items():
            if tag is None or tag == name:
                yield name, child, child["weight"]
            for p, c, w in enumerate_tags_weight(child, tag):
                yield p, c, w

def enumerate_leaf_tags(structure):
    root = None
    if "children" in structure:
        root = structure["children"]
    else:
        root = structure
        
    if root is not None:
        for name, child in root.items():
            for p, c in enumerate_leaf_tags(child):
                yield p, c
    elif structure is not None:
        yield structure["name"], structure
                
def calculate_by_tree(facts, structure, chapter = None, root_node_name = None):
    result = {}
    for chapter_name, chap in structure.items():
        if chapter is not None and cl.ChapterClassificator.match(chapter_name) != chapter:
            continue
        
        sub_trees = None
        if root_node_name is not None:
            sub_trees = [n for _, n in enumerate_tags(chap, root_node_name)]
        else:
            sub_trees = [chap]
        tags = set([f for f in facts])
        used_tags = set()
        
        for t in facts:
            for tree in sub_trees:
                for node_name, node in enumerate_tags(tree, t):
                    used_tags.add(t)
                    children = set([child for child, _ in enumerate_tags(node)])
                    tags = tags.difference(children)
                
        result[chapter_name] = None
        for t in tags.intersection(used_tags):
            if result[chapter_name] is None:
                result[chapter_name] = facts[t]
            else:
                result[chapter_name] += facts[t]
    return result

class TreeSum(object):
    def by_leafs(facts, structure):
        root = structure["name"]
        
        data = {root:[None, 1, 0.0]}
        for (p,c,w,_) in enumerate_tags_basic(structure):
            if c in facts:
                data[c] = [p, w, facts[c]]
            else:
                data[c] = [p, w, 0.0]
                
        tag_for_sum = set([t for t in facts])
        
        while(True):
            tag_for_next_sum = set()
            for t in tag_for_sum:
                [p, w, v] = data[t]
                if p is None:
                    tag_for_next_sum.add(t)
                    continue
                
                data[p][2] += w*v
                data[t][2] = 0.0
                
                tag_for_next_sum.add(p)
                
            tag_for_sum = tag_for_next_sum
            
            if len(tag_for_sum) == 0:
                return None
            if len(tag_for_sum) == 1 and list(tag_for_sum)[0] == root:
                break
            
        return data[root][2]
    
    def by_tops(facts, structure):
        root = structure["name"]
        
        #           parent, w, value, truth
        data = {root:[None, 1, 0.0, False]}
        for (p,c,w,_) in enumerate_tags_basic(structure):
            if c in facts:
                data[c] = [p, w, facts[c], True]
            else:
                data[c] = [p, w, 0.0, False]
                
        tag_for_sum = set([t for t in facts])
        
        while(True):
            tag_for_next_sum = set()
            for t in tag_for_sum:
                [p, w, v, truth] = data[t]
                if p is None:
                    tag_for_next_sum.add(t)
                    continue
                
                if not data[p][3]:
                    data[p][2] += w*v
                    data[t][2] = 0.0
                
                tag_for_next_sum.add(p)
                
            tag_for_sum = tag_for_next_sum
            
            if len(tag_for_sum) == 0:
                return None
            if len(tag_for_sum) == 1 and list(tag_for_sum)[0] == root:
                break
            
        return data[root][2]
        
