# -*- coding: utf-8 -*-

from xbrlxml.xbrlchapter import Chapter, Node

def make_chapters():
    """
    c1 -- n0 -- n1 -- n3
          |     |              
          n2    n4
          
    c2 -- n5 -- n6
          |
          n7 -- n8
          |
          n9
          
    n5 == n2
    """
    c1 = Chapter(roleuri = 'roleuri1')        
    c2 = Chapter(roleuri = 'roleuri2')
    
    n = []
    for i in range(0,10):
        if i<5:
            node = Node('us-gaap', 'NodeName'+str(i), label='NodeLabel'+str(i))                
            c1.nodes[node.label] = node
        else:
            node = Node('mg', 'NodeName'+str(i-5), label='NodeLabel'+str(i))
            if i == 5:
                node.version = 'us-gaap'
                node.label = 'NodeLabel2'
                node.tag = 'NodeName2'
                node.name = node.getname()
                
            c2.nodes[node.label] = node
        n.append(node)        
    
    n[0].children[n[1].label] = n[1]        
    n[0].children[n[2].label] = n[2]
    n[1].parent = n[0]
    n[2].parent = n[0]
    n[1].arc = {'weight':1.0}
    n[2].arc = {'weight':-1.0}
    n[1].children[n[3].label] = n[3]
    n[1].children[n[4].label] = n[4]
    n[3].parent = n[1]
    n[4].parent = n[1]
    n[3].arc = {'weight':1.0}
    n[4].arc = {'weight':-1.0}
    n[5].children[n[6].label] = n[6]
    n[5].children[n[7].label] = n[7]
    n[6].parent = n[5]
    n[7].parent = n[5]
    n[6].arc = {'weight':-1.0}
    n[7].arc = {'weight':1.0}
    n[7].children[n[8].label] = n[8]
    n[7].children[n[9].label] = n[9]        
    n[8].parent = n[7]
    n[9].parent = n[7]
    n[8].arc = {'weight':1.0}
    n[9].arc = {'weight':-1.0}
    
    return {c1.roleuri:c1, c2.roleuri:c2}

class TestLoad():
    def make_simple_node():
        node_str = """{"name": "us-gaap:NodeName1", "weight": 1.0, "children": {"us-gaap:NodeName3": {"name": "us-gaap:NodeName3", "weight": 1.0, "children": null}, "mg:NodeName4": {"name": "mg:NodeName4", "weight": -1.0, "children": null}}}"""
        #make a ethalon node        
        n = Node(tag = 'NodeName1', version='us-gaap', label='us-gaap:NodeName1')
        n.arc = {'weight': 1.0}
        
        n1 = Node(tag = 'NodeName3', version='us-gaap', label='us-gaap:NodeName3')
        n1.arc = {'weight': 1.0}
        n1.parent = n
        
        n2 = Node(tag = 'NodeName4', version='mg', label='mg:NodeName4')
        n2.arc = {'weight': -1.0}
        n2.parent = n
        
        n.children[n1.label] = n1
        n.children[n2.label] = n2
        
        return (n, node_str)
    
    def make_simple_chapter():
        chap_str = """{"roleuri": "roleuri1", "nodes":{"us-gaap:NodeName1": {"name": "us-gaap:NodeName1", "weight": 1.0, "children": {"us-gaap:NodeName3": {"name": "us-gaap:NodeName3", "weight": 1.0, "children": null}, "mg:NodeName4": {"name": "mg:NodeName4", "weight": -1.0, "children": null}}}}}"""
        c = Chapter(roleuri='roleuri1')
        n = Node(tag = 'NodeName1', version='us-gaap', label='us-gaap:NodeName1')
        n.arc = {'weight': 1.0}
        
        n1 = Node(tag = 'NodeName3', version='us-gaap', label='us-gaap:NodeName3')
        n1.arc = {'weight': 1.0}
        n1.parent = n
        
        n2 = Node(tag = 'NodeName4', version='mg', label='mg:NodeName4')
        n2.arc = {'weight': -1.0}
        n2.parent = n
        
        n.children[n1.label] = n1
        n.children[n2.label] = n2
        
        c.nodes[n.label] = n
        c.nodes[n1.label] = n1
        c.nodes[n2.label] = n2
        
        return c, chap_str
    
    def make_simple_structure():
        pass
    