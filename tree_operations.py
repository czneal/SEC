# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 12:58:44 2017

@author: Asus
"""
import classificators as cl

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
            

#with open("structure.json") as f:
#    struct = json.loads(f.read())
#
#facts = {"us-gaap:AssetsCurrent":1.0,
#         "us-gaap:Cash":2.0,
#         "us-gaap:AccountsReceivableNetCurrent":3.0,
#         "us-gaap:PropertyPlantAndEquipmentNet":4.0}
#
#print(calculate_by_tree(facts,struct))
