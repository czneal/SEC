from typing import List, Tuple, Optional, cast, Dict, Iterator, Union
from algos.scheme import Chapters, Node, Chapter, enum_filtered, enum
from indi.modclass import Classifier


class Feeder(object):
    def __init__(self, chapter: str, names: List[str], strict: bool):
        self.chapter = chapter
        self.names = names.copy()
        self.strict = strict

    def find_start(self, structure: Chapters) \
            -> Tuple[Chapter, Optional[Node]]:
        """
        return Node for the first occurence in names if strict==True
        raise IndexError exception otherwise

        return root Node if strict=False

        return biggest Node id strict=False

        return Node for the first occurence in names if strict==False
        raise IndexError
        """

        if self.chapter not in structure:
            raise IndexError()

        chapter = structure[self.chapter]
        if self.strict:
            for name in self.names:
                if name in chapter.nodes:
                    return chapter, chapter.nodes[name]
            else:
                raise IndexError()
        else:
            starts: List[Tuple[Node, int]] = [
                (node, 0) for node in chapter.nodes.values()
                if node.parent is None]

            # if only one root node
            if len(starts) == 1:
                return chapter, starts[0][0]
            if len(starts) == 0:
                raise IndexError()

            return chapter, None

            # # try to find biggest node
            # for i in range(len(starts)):
            #     starts[i] = (starts[i][0],
            #                  len([0
            #                       for [c] in enum(starts[i][0],
            #                                       outpattern='c')])
            #                  )

            # starts = sorted(starts, key=lambda x: x[1], reverse=True)
            # if starts[0][1] > starts[1][1]:
            #     return starts[0][0]

            # # find start by names
            # for name in self.names:
            #     if name in chapter.nodes:
            #         return chapter.nodes[name]
            # else:
            #     raise IndexError()

    def filter(
            self, structure: Chapters) -> List[Tuple[str, str]]:
        """
        return list of pairs: [('us-gaap:Liabilities','us-gaap:LaibilitiesCurrent'), ...]
        """
        try:
            chapter, node = self.find_start(structure)
        except IndexError:
            return ([])
        start: Union[Node, Chapter] = chapter
        if node:
            start = node

        pairs: List[Tuple[str, str]] = []
        names: List[str] = []
        for p, c in self._custom_filter(start):
            pairs.append((p, c))
        return pairs

    def _custom_filter(
            self, start: Union[Node, Chapter]) -> Iterator[Tuple[str, str]]:
        return cast(Iterator[Tuple[str, str]],
                    enum(start,
                         leaf=True,
                         outpattern='pc',
                         func=lambda x: cast(str, x.name)))


class ClassFeeder(Feeder):
    def __init__(
            self,
            chapter: str,
            names: List[str],
            strict: bool,
            cl: Classifier,
            cl_id: int):
        super().__init__(chapter, names, strict)
        self.cl = cl
        self.cl_id = cl_id

    def stop(self, p: str, c: str) -> bool:
        if self.cl.predict([(p, c)])[0] == 0:
            return True
        else:
            return False

    def _custom_filter(
            self, start: Union[Node, Chapter]) -> Iterator[Tuple[str, str]]:
        return cast(Iterator[Tuple[str, str]],
                    enum_filtered(start,
                                  outpattern='pc',
                                  leaf=True,
                                  nfunc=lambda x: cast(str, x.name),
                                  ffunc=self.stop))


def create(chapter: str,
           names: List[str],
           strict: bool,
           cl: Optional[Classifier] = None,
           cl_id: int = -1) -> Feeder:

    if cl is None:
        return Feeder(chapter, names, strict)
    else:
        return ClassFeeder(chapter, names, strict, cl, cl_id)
