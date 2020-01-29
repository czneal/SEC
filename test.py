import numpy as np
from typing import List, Tuple, cast, Any
from tensorflow.keras.models import load_model  # type: ignore
from abc import ABCMeta, abstractmethod
from settings import Settings
import re
import os
import json


class ModelClassifier(metaclass=ABCMeta):
    def __init__(self,
                 fdict: str,
                 model,
                 max_len: int):

        self.max_len = max_len
        self._load_dict(fdict)
        self.model = model

    def _load_dict(self, filename: str) -> None:
        with open(Settings.models_dir() + filename) as f:
            self.tag_to_code = {
                l.split("\t")[0]: l.replace("\n", "").split("\t")[1]
                for l in f}

    def _to_vector(self, tag: str, x: np.ndarray, row: int, pos: int) -> None:
        words = re.findall('[A-Z][^A-Z]*', tag)
        try:
            for i, word in enumerate(words):
                x[row, pos + i] = self.tag_to_code.get(word, 1)
        except IndexError:
            pass

    @abstractmethod
    def _vectorize(self, parent: str, child: str,
                   x: np.ndarray, row: int) -> None:
        pass

    @abstractmethod
    def _explain_results(self, x: np.ndarray) -> List[int]:
        pass

    def predict(self, pairs: List[Tuple[str, str]]) -> List[int]:
        x = np.ones((len(pairs), self.max_len))
        for i, (parent, child) in enumerate(pairs):
            self._vectorize(parent, child, x, i)

        y = self.model.predict(x)
        return self._explain_results(y)


class SingleAnswer(ModelClassifier):
    def _explain_results(self, x) -> List[int]:
        x = (x > 0.5).astype(int)
        x = x.reshape((x.shape[0],))
        return cast(List[int], x.tolist())


class MultiAnswer(ModelClassifier):
    def _explain_results(self, x) -> List[int]:
        x = np.argmax(x, axis=1).astype(int)
        x = x.reshape((x.shape[0],))
        return cast(List[int], x.tolist())


class ParentAndChild(ModelClassifier):
    def _vectorize(self, parent: str, child: str,
                   x: np.ndarray, row: int) -> None:

        self._to_vector(parent, x=x, row=row, pos=0)
        self._to_vector(child, x=x, row=row, pos=int(self.max_len / 2))


class OnlyChild(ModelClassifier):
    def _vectorize(self, parent: str, child: str,
                   x: np.ndarray, row: int) -> None:
        self._to_vector(child, x=x, row=row, pos=0)


class SingleParentAndChild(SingleAnswer, ParentAndChild):
    pass


class MultiParentAndChild(MultiAnswer, ParentAndChild):
    pass


class MultiOnlyChild(MultiAnswer, OnlyChild):
    pass


class SingleOnlyChild(SingleAnswer, OnlyChild):
    pass


def load_classifiers() -> List[ModelClassifier]:
    with open(os.path.join(
            Settings.models_dir(),
            'classifiers.json')) as f:
        classifiers = json.load(f)
    return [load_classifier(sett['model'],
                            sett['dict'],
                            sett['pc'],
                            sett['multi'],
                            sett['max_len']) for sett in classifiers]


def load_classifier(
        fmodel: str,
        fdict: str,
        pc: bool,
        multi: bool,
        max_len) -> ModelClassifier:

    model = load_model(
        os.path.join(
            Settings.models_dir(),
            fmodel))

    if pc:
        if multi:
            return MultiParentAndChild(fdict, model, max_len)
        else:
            return SingleParentAndChild(fdict, model, max_len)
    else:
        if multi:
            return MultiOnlyChild(fdict, model, max_len)
        else:
            return SingleOnlyChild(fdict, model, max_len)


if __name__ == '__main__':
    load_classifiers()
