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
    c1 = Chapter(roleuri='roleuri1', label="balance sheet")
    c2 = Chapter(roleuri='roleuri2', label="income statement")

    n = []
    for i in range(0, 10):
        if i < 5:
            node = Node(
                version='us-gaap',
                tag=f'NodeName{i}')
            c1.nodes[node.name] = node
        else:
            node = Node(
                version='mg', tag=f'NodeName{i-5}')
            if i == 5:
                node.version = 'us-gaap'
                node.tag = 'NodeName2'
                node.name = node.getname()

            c2.nodes[node.name] = node
        n.append(node)

    n[0].children[n[1].name] = n[1]
    n[0].children[n[2].name] = n[2]
    n[1].parent = n[0]
    n[2].parent = n[0]
    n[1].arc = {'weight': 1.0}
    n[2].arc = {'weight': -1.0}
    n[1].children[n[3].name] = n[3]
    n[1].children[n[4].name] = n[4]
    n[3].parent = n[1]
    n[4].parent = n[1]
    n[3].arc = {'weight': 1.0}
    n[4].arc = {'weight': -1.0}
    n[5].children[n[6].name] = n[6]
    n[5].children[n[7].name] = n[7]
    n[6].parent = n[5]
    n[7].parent = n[5]
    n[6].arc = {'weight': -1.0}
    n[7].arc = {'weight': 1.0}
    n[7].children[n[8].name] = n[8]
    n[7].children[n[9].name] = n[9]
    n[8].parent = n[7]
    n[9].parent = n[7]
    n[8].arc = {'weight': 1.0}
    n[9].arc = {'weight': -1.0}

    return {'bs': c1, 'is': c2}
