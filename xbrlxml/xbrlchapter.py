# -*- coding: utf-8 -*-
"""
Created on Sat May 25 16:55:50 2019

@author: Asus
"""
import lxml  # type: ignore
import re

from typing import Optional, Dict, Any, Union, List, Set, Tuple


class Node():
    """
    implements single element in scheme
    represents <loc> block with arc property for
    <calculationArc>, <presentationArc>, <definitionArc> block
    """

    def __init__(self,
                 version: str = '',
                 tag: str = ''):
        self.version: str = version
        self.tag: str = tag
        self.arc: Dict[str, int] = {}
        self.parent = None  # type: Optional[Node]
        self.children: Dict[str, Node] = {}
        self.name: str = self.getname()

        # self.label = label

    def getname(self) -> str:
        "unittested"
        if self.version != '':
            return f'{self.version}:{self.tag}'
        else:
            return self.tag

    def getweight(self) -> int:
        return int(self.arc.get('weight', 1))

    def asdict(self) -> Dict[str, Union[str, int]]:
        "unittested"
        r: Dict[str, Union[str, int]] = {'tag': self.name}
        r.update(self.arc.items())
        return r

    def copy(self):
        from algos.scheme import makenodecopy
        return makenodecopy(self)

    def __simple_eq(self, node):
        # type: (Node) -> bool

        if (self.version == node.version and
            self.tag == node.tag and
                self.arc == node.arc):
            return True
        else:
            return False

    def __parent_eq(self, node):
        # type: (Node) -> bool
        if (node.parent is None and self.parent is None):
            return True
        if (node.parent is not None and
                self.parent is not None):
            return self.parent.__simple_eq(node.parent)
        return False

    def __eq__(self, node) -> bool:
        if not isinstance(node, Node):
            raise NotImplementedError()

        if (not self.__simple_eq(node)):
            return False
        if (not self.__parent_eq(node)):
            return False

        if (self.children == node.children):
            return True

        return False


class Chapter(object):
    """
    implements single scheme
    represents <calculationLink>, <presentationLink>, <definitionLink> block
    """

    def __init__(self,
                 roleuri: str = '',
                 label: str = ''):
        self.roleuri = roleuri
        self.label = label
        self.nodes: Dict[str, Node] = {}

    def update_arc(
            self, arc: Dict[str, Any],
            labels: Dict[str, str]):
        "unittested"
        n_from = self.nodes[labels[arc['from']]]
        n_to = self.nodes[labels[arc['to']]]
        n_to.parent = n_from
        n_from.children[n_to.name] = n_to
        n_to.arc = arc['attrib']

    def getnodes(self) -> List[Dict[str, Any]]:
        "unittested"
        return [n.asdict() for n in self.nodes.values()]

    def gettags(self) -> Set[str]:
        "unittested"
        return set([n.name for n in self.nodes.values()])

    def __eq__(self, chapter) -> bool:
        if (chapter is None):
            return False
        if (self.roleuri != chapter.roleuri):
            return False
        if (self.nodes != chapter.nodes):
            return False

        return True


DimMembersType = List[Tuple[Optional[str], Optional[str]]]
DimsType = List[Optional[str]]


class CalcChapter(Chapter):
    def dimmembers(self) -> DimMembersType:
        return [(None, None)]

    def dims(self) -> DimsType:
        return [None]

    def extend(self, calc, pres):
        pass


class DimChapter(Chapter):
    def dimmembers(self) -> DimMembersType:
        "unittested"
        retval: DimMembersType = [(None, None)]
        for n in self.nodes.values():
            if not re.match('.*member', n.tag, re.IGNORECASE):
                continue
            p = n.parent
            while p is not None:
                if re.match('.*axis', p.tag, re.I):
                    retval.append((p.name, n.name))
                    break
                p = p.parent

        return retval

    def dims(self) -> DimsType:
        "unittested"
        retval = set([dim for (dim, _) in self.dimmembers()])
        return list(retval)


class ChapterFactory():
    @staticmethod
    def chapter(ref_type: str) -> Chapter:
        "unittested"

        if ref_type == 'calculation':
            return CalcChapter()
        if ref_type == 'definition':
            return DimChapter()
        if ref_type == 'presentation':
            return DimChapter()

        raise ValueError(f'unsupported reference type')


class ReferenceParser(object):
    def __init__(self, ref_type: str):
        """
        ref_type = {'calculation', 'presentation', 'definition'}
        """
        self.__ref_type = ''
        self.setreftype(ref_type)
        self.decimal_re = re.compile(r'[\+,\-]{0,1}\d*(\.\d+)?$')

    def setreftype(self, ref_type):
        assert (ref_type in {'calculation', 'presentation', 'definition'})
        self.__ref_type = ref_type

    def parse(self, file) -> Dict[str, Chapter]:
        etree = lxml.etree.parse(file)
        root = etree.getroot()

        chapters: Dict[str, Chapter] = {}

        for link in root.findall('{*}' + self.__ref_type + 'Link'):
            chapter = self.parse_chapter(link)
            if len(chapter.nodes) != 0:
                chapters[chapter.roleuri] = chapter

        return chapters

    def parse_chapter(self, link) -> Chapter:
        """
        return Chapter object
        unittested
        """

        labels: Dict[str, str] = {}
        chapter = ChapterFactory.chapter(self.__ref_type)
        chapter.roleuri = link.attrib['{%s}role' % link.nsmap['xlink']]

        for loc in link.findall('{*}' + 'loc'):
            node, label = self.parse_node(loc)
            labels[label] = node.name
            chapter.nodes[node.name] = node

        for arc in link.findall('{*}' + self.__ref_type + 'Arc'):
            arc = self.parse_arc(arc)
            chapter.update_arc(arc, labels)

        return chapter

    def parse_node(self, loc) -> Tuple[Node, str]:
        "unittested"
        """
        return tuple (Node, label)
        """

        label = loc.attrib['{%s}label' % loc.nsmap['xlink']]
        href = loc.attrib['{%s}href' %
                          loc.nsmap['xlink']].split('#')[-1].split('_')
        n = Node(version=href[0], tag=href[1])

        return n, label

    def parse_arc(self, arc) -> Dict[str, Any]:
        "unittested"
        """
        return dict of attributes
        """
        arcdict: Dict[str, Any] = {'attrib': {}}

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
                if self.decimal_re.match(value):
                    arcdict['attrib'][attr] = float(value)
                else:
                    arcdict['attrib'][attr] = value

        return arcdict


if __name__ == '__main__':
    pass
