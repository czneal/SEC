# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from typing import Dict, Any, cast

from xbrlxml.xbrlchapter import Chapter, Node, CalcChapter
from algos.scheme import enum
import json
import datetime as dt


class CustomJsonEncoder(json.JSONEncoder, metaclass=ABCMeta):
    """ custom
    """

    def default(self, obj):
        from xbrlxml.shares import Share

        if isinstance(obj, dt.datetime):
            return (str(obj.year).zfill(4) + '-' +
                    str(obj.month).zfill(2) + '-' +
                    str(obj.day).zfill(2) + ' ' +
                    str(obj.hour).zfill(2) + ':' +
                    str(obj.minute).zfill(2) + ':' +
                    str(obj.second).zfill(2) + ':' +
                    str(obj.microsecond).zfill(4))
        if isinstance(obj, dt.date):
            return (str(obj.year).zfill(4) + '-' +
                    str(obj.month).zfill(2) + '-' +
                    str(obj.day).zfill(2))
        if isinstance(obj, Chapter):
            return self.chapter_json(obj)
        if isinstance(obj, Node):
            return self.node_json(obj)
        if isinstance(obj, Share):
            return self.share_json(obj)

        return json.JSONEncoder.default(super(), obj)

    @abstractmethod
    def chapter_json(self, obj):
        pass

    @abstractmethod
    def node_json(self, obj):
        pass

    @abstractmethod
    def share_json(self, obj):
        pass


class ForTestJsonEncoder(CustomJsonEncoder):
    "unittested"

    def chapter_json(self, obj: Chapter) -> Dict[str, Any]:
        j = {"roleuri": obj.roleuri,
             "nodes": {k: v for k, v in obj.nodes.items() if v.parent is None},
             "label": obj.label
             }
        return j

    def node_json(self, obj: Node) -> Dict[str, Any]:
        j = {'tag': obj.tag,
             'version': obj.version,
             'arc': obj.arc,
             'children': None if len(obj.children) == 0 else obj.children}
        return j

    def share_json(self, obj):
        return {}


class ForDBJsonEncoder(CustomJsonEncoder):
    "unittested"

    def chapter_json(self, obj: Chapter) -> Dict[str, Any]:
        j = {
            "roleuri": obj.roleuri,
            "nodes": {
                v.name: v for k,
                v in obj.nodes.items() if v.parent is None},
            "label": obj.label}
        return j

    def node_json(self, obj: Node) -> Dict[str, Any]:
        j = {
            'name': obj.name,
            'weight': obj.arc.get('weight', 1.0),
            'children': None}
        if len(obj.children) > 0:
            j['children'] = {v.name: v for k, v in obj.children.items()}

        return j

    def share_json(self, obj) -> Dict[str, Any]:
        return cast(Dict[str, Any], obj.__dict__)


def custom_decoder(obj):
    "unittested"
    if not isinstance(obj, dict):
        return obj

    if 'roleuri' in obj:
        c = CalcChapter(roleuri=obj['roleuri'],
                        label=obj.get('label', ''))
        if 'nodes' not in obj:
            return c

        c.nodes = obj['nodes']
        nodes = {name: obj for name, obj in enum(c, outpattern='cn')}
        c.nodes = nodes
        return c

    if 'name' in obj and 'weight' in obj and 'children' in obj:
        [version, tag] = obj['name'].split(":")
        n = Node(tag=tag, version=version)
        n.arc = {'weight': float(obj['weight'])}

        n.children = obj['children'] if obj['children'] is not None else {}
        for c, cobj in n.children.items():
            cobj.parent = n
        return n

    return obj


def loads(strin):
    return json.loads(strin, object_hook=custom_decoder)


def dumps(chapters: Dict[str, Chapter], indent=None) -> str:
    return json.dumps(chapters, cls=ForDBJsonEncoder, indent=indent)


if __name__ == '__main__':
    # string = """{"bs": {"name": "simple_name",
    #                     "chapter": {"roleuri": "roleuri1"}}}"""

    # c = json.loads(string, object_hook=custom_decoder)
    pass
