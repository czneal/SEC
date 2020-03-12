from typing import List, Tuple, Optional, cast, Dict, Iterator
from algos.scheme import Chapters, Node, enum_filtered, enum
from indi.modclass import Classifier


class Feeder(object):
    def __init__(self, chapter: str, names: List[str]):
        self.chapter = chapter
        self.names = names.copy()

    def find_start(self, structure: Chapters) -> Node:
        """
        return Node for the first occurence in names
        raise IndexError exception otherwise
        """

        if self.chapter not in structure:
            raise IndexError()

        chapter = structure[self.chapter]
        for name in self.names:
            if name in chapter.nodes:
                return chapter.nodes[name]
        else:
            raise IndexError()

    def filter(
            self, structure: Chapters) -> List[Tuple[str, str]]:
        """
        return list of pairs: [('us-gaap:Liabilities','us-gaap:LaibilitiesCurrent'), ...]
        """
        try:
            node = self.find_start(structure)
        except IndexError:
            return ([])

        pairs: List[Tuple[str, str]] = []
        names: List[str] = []
        for p, c in self._custom_filter(node):
            pairs.append((p, c))
        return pairs

    def _custom_filter(self, node: Node) -> Iterator[Tuple[str, str]]:
        return cast(Iterator[Tuple[str, str]],
                    enum(node,
                         leaf=True,
                         outpattern='pc',
                         func=lambda x: cast(str, x.name)))


class ClassFeeder(Feeder):
    def __init__(
            self,
            chapter: str,
            names: List[str],
            cl: Classifier,
            cl_id: int):
        super().__init__(chapter, names)
        self.cl = cl
        self.cl_id = cl_id

    def stop(self, p: str, c: str) -> bool:
        if self.cl.predict([(p, c)])[0] == 0:
            return True
        else:
            return False

    def _custom_filter(self, node: Node) -> Iterator[Tuple[str, str]]:
        return cast(Iterator[Tuple[str, str]],
                    enum_filtered(node,
                                  outpattern='pc',
                                  leaf=True,
                                  nfunc=lambda x: cast(str, x.name),
                                  ffunc=self.stop))


def create(chapter: str,
           names: List[str],
           cl: Optional[Classifier] = None,
           cl_id: int = -1) -> Feeder:

    if cl is None:
        return Feeder(chapter, names)
    else:
        return ClassFeeder(chapter, names, cl, cl_id)
