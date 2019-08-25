# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

from xbrlxml.xbrlchapter import Chapter, Node, CalcChapter
from algos.scheme import enum
import json
import datetime as dt


class CustomJsonEncoder(json.JSONEncoder, metaclass=ABCMeta):
    """ custom
    """
        
    def default(self, obj):
        if isinstance(obj, dt.date):
            return (str(obj.year).zfill(4) + '-' + 
                    str(obj.month).zfill(2) + '-' +
                    str(obj.day).zfill(2))
        if isinstance(obj, Chapter):
            return self.chapter_json(obj)
        elif isinstance(obj, Node):            
            return self.node_json(obj)
        else:
            return super().default(self, obj)
    
    @abstractmethod
    def chapter_json(self, obj):     
        pass
    
    @abstractmethod
    def node_json(self, obj):
        pass
    
class ForTestJsonEncoder(CustomJsonEncoder):
    "unittested"
    def chapter_json(self, obj):
        j = {"roleuri": obj.roleuri,
             "nodes": {k:v for k, v in obj.nodes.items() if v.parent is None}
            }
        return j
    
    def node_json(self, obj):
        j = {'tag':obj.tag,
            'version':obj.version,
            'arc':obj.arc,
            'children':None if len(obj.children) == 0 else obj.children}
        return j
    
class ForDBJsonEncoder(CustomJsonEncoder):
    "unittested"
    def chapter_json(self, obj):
        j = {"roleuri": obj.roleuri,
             "nodes": {v.name: v for k, v in obj.nodes.items() if v.parent is None}
            }
        return j
    
    def node_json(self, obj):
        j = {'name':obj.name,            
             'weight':obj.arc.get('weight', 1.0) if obj.arc is not None else 1.0,
             'children': None
             }
        if len(obj.children) > 0:
            j['children'] = {v.name: v for k,v in obj.children.items()}
            
        return j

def custom_decoder(obj):
    "unittested"
    if not isinstance(obj, dict):
        return obj
    
    if 'roleuri' in obj:
        c = CalcChapter(roleuri=obj['roleuri'])
        if 'nodes' not in obj:
            return c
        
        c.nodes = obj['nodes']        
        nodes = {name: obj for name, obj in enum(c, outpattern='cn')}
        c.nodes = nodes
        return c
    
    if 'name' in obj and 'weight' in obj and 'children' in obj:
        [version, tag] = obj['name'].split(":")
        n = Node(tag = tag, version=version, label=obj['name'])        
        n.arc = {'weight': float(obj['weight'])}
        
        n.children = obj['children'] if obj['children'] is not None else {}
        for c, cobj in n.children.items():
            cobj.parent = n
        return n
    
    return obj

def loads(strin):
    return json.loads(strin, object_hook = custom_decoder)

if __name__ == '__main__':
    string = """{"bs": {"name": "simple_name", 
                        "chapter": {"roleuri": "roleuri1"}}}"""
    
    c = json.loads(string, object_hook=custom_decoder)