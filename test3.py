import typing
import abc
import numpy as np

from algos.calc import calc_filtered
from algos.scheme import Structure, Chapter, Node, enum


class Filterer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def is_end(self, parent: str, child: str) -> bool:
        pass


class FiltererProxy(Filterer):
    def is_end(self, parent: str, child: str) -> bool:
        return True


class LSTMFilterer(Filterer):
    def __init__(self, fmodel: str):
        self.classifier = get_classifier(fmodel)

    def is_end(self, parent: str, child: str) -> bool:
        return self.classifier.predict(parent, child) != 0


class LSTMClassifier(metaclass=abc.ABCMeta):
    def __init__(self, fmodel: str):
        pass

    # @abc.abstractmethod
    # def _explain_result(self, result: float) -> int:
    #     pass

    # @abc.abstractmethod
    # def _vectorize(self, parent: str, child: str) -> np.ndarray:
    #     pass

    def predict(self, parent: str, child: str) -> int:
        return 1


class Indicator():
    def __init__(self, class_fmodel: str, filter_fmodel: str):
        self.classifier = get_classifier(class_fmodel)
        self.filterer = get_filterer(filter_fmodel)
        self.class_id = 1
        self.classified: typing.List[typing.Tuple[str, str]] = []

    def its_mine(self, parent: str, child: str) -> bool:
        if self.classifier.predict(parent, child) == self.class_id:
            self.classified.append((parent, child))
            return True
        else:
            return False

    def calc(self,
             structure: Structure,
             nums: typing.Dict[str, float]) -> typing.Optional[float]:

        self.classified = []
        value: typing.Optional[float] = None

        structure = typing.cast(
            typing.Dict[str, typing.Dict[str, Chapter]],
            structure)
        for node in structure['bs']['chapter'].nodes.values():
            node_value = calc_filtered(node,
                                       nums,
                                       self.filterer.is_end,
                                       self.its_mine)
            if node_value and value:
                value += node_value
            if node_value and not value:
                value = node_value

        return value


def get_classifier(fmodel: str) -> LSTMClassifier:
    return LSTMClassifier(fmodel=fmodel)


def get_filterer(fmodel: str) -> Filterer:
    if fmodel == 'proxy':
        return FiltererProxy()

    return LSTMFilterer(fmodel)


if __name__ == "__main__":
    pass
