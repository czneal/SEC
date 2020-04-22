# -*- coding: utf-8 -*-

import re
from typing import Dict, List, Tuple, Callable, Iterator, Union, Any, cast

from xbrlxml.xbrlchapter import Chapter, Node
from xbrlxml.xsdfile import XSDChapter
from xbrlxml.xbrlexceptions import XBRLDictException

Chapters = Dict[str, Chapter]
Structure = Union[Chapter, Node, Chapters]


def _enum(structure: Structure,
          offset: int,
          func: Callable[[Node], str]) -> Iterator[Tuple[str,
                                                         str,
                                                         float,
                                                         int,
                                                         bool,
                                                         Node,
                                                         str]]:
    "unittested"
    if isinstance(structure, Chapter):
        for node in structure.nodes.values():
            if node.parent is None:
                yield (structure.roleuri,
                       func(node),
                       1.0,
                       offset,
                       True if not node.children else False,
                       node,
                       node.version)
                for item in _enum(node, offset=offset + 1, func=func):
                    yield item
    if isinstance(structure, Node):
        for child in structure.children.values():
            yield (func(structure),
                   func(child),
                   child.arc['weight'] if 'weight' in child.arc else 1.0,
                   offset,
                   True if not child.children else False,
                   child,
                   child.version)
            for item in _enum(child, offset=offset + 1, func=func):
                yield item
    if isinstance(structure, dict):
        for sheet in structure.values():
            assert isinstance(sheet, Chapter)
            for item in _enum(sheet, offset=offset, func=func):
                yield item


def enum(structure: Structure,
         leaf: bool = False,
         outpattern: str = 'pcwol',
         func: Callable[[Node],
                        str] = lambda x: cast(str,
                                              x.name)) -> Iterator[List[Any]]:
    "unittested"
    assert re.fullmatch('[pcwolnv]+', outpattern)

    for item in _enum(structure, offset=0, func=func):
        if leaf and not item[4]:
            continue

        retval: List[Any] = []
        for c in outpattern:
            if c == 'p':
                retval.append(item[0])
            if c == 'c':
                retval.append(item[1])
            if c == 'w':
                retval.append(item[2])
            if c == 'o':
                retval.append(item[3])
            if c == 'l':
                retval.append(item[4])
            if c == 'n':
                retval.append(item[5])
            if c == 'v':
                retval.append(item[6])
        yield retval


def _enum_filtered(structure: Structure,
                   offset: int,
                   nfunc: Callable[[Node],
                                   str],
                   ffunc: Callable[[str,
                                    str],
                                   bool]) -> Iterator[Tuple[str,
                                                            str,
                                                            float,
                                                            int,
                                                            bool,
                                                            Node,
                                                            str]]:

    if isinstance(structure, Chapter):
        for node in structure.nodes.values():
            if node.parent is None:
                stop = ffunc(structure.roleuri, nfunc(node))
                if stop:
                    continue

                has_children = False
                for item in _enum_filtered(node,
                                           offset=offset + 1,
                                           nfunc=nfunc,
                                           ffunc=ffunc):
                    has_children = True
                    yield item

                yield (structure.roleuri,
                       nfunc(node),
                       1.0,
                       offset,
                       not has_children,
                       node,
                       node.version)

    if isinstance(structure, Node):
        for child in structure.children.values():
            stop = ffunc(nfunc(structure), nfunc(child))
            if stop:
                continue

            has_children = False
            for item in _enum_filtered(child,
                                       offset=offset + 1,
                                       nfunc=nfunc,
                                       ffunc=ffunc):
                has_children = True
                yield item

            yield (nfunc(structure),
                   nfunc(child),
                   child.arc['weight'] if 'weight' in child.arc else 1.0,
                   offset,
                   not has_children,
                   child,
                   child.version)

    if isinstance(structure, dict):
        for sheet in structure.values():
            assert isinstance(sheet, Chapter)
            for item in _enum_filtered(sheet,
                                       offset=offset,
                                       nfunc=nfunc,
                                       ffunc=ffunc):
                yield item


def enum_filtered(structure: Structure,
                  leaf: bool = False,
                  outpattern: str = 'pcwol',
                  nfunc: Callable[[Node],
                                  str] = lambda x: cast(str,
                                                        x.name),
                  ffunc: Callable[[str,
                                   str],
                                  bool] = lambda x,
                  y: False) -> Iterator[List[Any]]:
    assert re.fullmatch('[pcwolnv]+', outpattern)

    for item in _enum_filtered(structure,
                               offset=0,
                               nfunc=nfunc,
                               ffunc=ffunc):
        if leaf and not item[4]:
            continue

        retval: List[Any] = []
        for c in outpattern:
            if c == 'p':
                retval.append(item[0])
            if c == 'c':
                retval.append(item[1])
            if c == 'w':
                retval.append(item[2])
            if c == 'o':
                retval.append(item[3])
            if c == 'l':
                retval.append(item[4])
            if c == 'n':
                retval.append(item[5])
            if c == 'v':
                retval.append(item[6])
        yield retval


def find_extentions(roleuri: str,
                    calc: Chapters,
                    pres: List[str],
                    xsds: List[str]) -> \
        Tuple[Dict[str, str], List[str]]:
    """
    try to find extention for calculation scheme roleuri
    return tuple({NodeLabel:roleuri, ...}, list(warning))
    """
    extentions = {}  # type: Dict[str, str]
    warnings = []  # type: List[str]

    chapter = calc.get(roleuri, None)
    if chapter is None:
        return extentions, warnings

    # for every leaf node in chapter
    for n in [n for n in chapter.nodes.values() if len(n.children) == 0]:
        # look at every chapter which contains presentation
        # and calculation scheme
        for xsd_roleuri in xsds:
            if xsd_roleuri not in pres:
                continue
            if xsd_roleuri not in calc:
                continue

            c = calc[xsd_roleuri]
            # only look for not leaf nodes
            if n.name not in c.nodes or len(c.nodes[n.name].children) == 0:
                continue

            extnode = c.nodes[n.name]
            try:
                # check possibility of extention
                check_extention(chapter, extnode)
            except XBRLDictException as exc:
                exc.exc_data['base chapter'] = roleuri
                exc.exc_data['ext chapter'] = xsd_roleuri
                warnings.append(str(exc))
                continue

            extentions[n.name] = xsd_roleuri

    return extentions, warnings


def check_extention(chapter: Chapter, newnode: Node):
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
                                          func=lambda x:x.name)])
    warnchildren = newchildren.intersection(set(chapter.nodes.keys()))

    for newchild in warnchildren:
        if len(chapter.nodes[newchild].children) != 0:
            exc_data = {
                'message': 'extention fails, base chapter has nodes with children',
                'node in ext chapter': newnode.name,
                'node in base chapter with children': newchild}
            raise XBRLDictException(exc_data)


def extend_clac_scheme(roleuri: str,
                       calc: Chapters,
                       extentions: Dict[str, str]):
    """
    extend calculation scheme
    use only after find_extentions
    """
    "unittested"

    if roleuri not in calc:
        return

    chapter = calc[roleuri]
    for name, exturi in extentions.items():
        c = calc[exturi]
        node = chapter.nodes[name]
        extnode = c.nodes[name]

        # remove leaf nodes from chapter which is in extnode.children
        remove_leaf_nodes(chapter, extnode)

        newnode = extnode.copy()
        node.children = newnode.children
        for [newname, newchild] in enum(node,
                                        outpattern='cn',
                                        func=lambda x: x.name):
            chapter.nodes[newname] = newchild


def remove_leaf_nodes(chapter: Chapter, newnode: Node):
    """
    remove leaf nodes from chapter which is in extnode.children
    """
    # find children to be add
    newchildren = set([elem for [elem] in enum(
        newnode, outpattern='c', func=lambda x:x.name)])
    # find children which is already in chapter.nodes
    warnchildren = newchildren.intersection(set(chapter.nodes.keys()))

    # remove them from parent.children
    # and remove them from chapter.nodes collection
    for newchild in warnchildren:
        parent = chapter.nodes[newchild].parent
        if parent is not None:
            parent.children.pop(newchild)
        chapter.nodes.pop(newchild)


def makenodecopy(node: Node) -> Node:
    """
    make deep copy of node
    """
    "unittested"

    assert isinstance(node, Node)

    n = Node(tag=node.tag, version=node.version)

    if node.arc is not None:
        n.arc = node.arc.copy()

    for name, child in node.children.items():
        n.children[name] = child.copy()
        n.children[name].parent = n

    return n


def cut_one_node(chapter: Chapter, name: str) -> None:
    for node in chapter.nodes.values():
        if name in node.children:
            node.children.pop(name)
    for child in chapter.nodes[name].children.values():
        pass

    raise Exception('not implemented')


def cut_empty_leafs(chapter: Chapter, facts: Dict[str, float]) -> None:
    raise Exception('not implemented')
