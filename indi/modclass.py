import json
import os
import re
from abc import ABCMeta, abstractmethod
from typing import Any, List, Tuple, cast

import numpy as np
from tensorflow.keras.models import load_model  # type: ignore

from settings import Settings


class Classifier(metaclass=ABCMeta):
    def __init__(self, fdict: str, model_id: int, max_len: int):
        self.fdict = fdict
        self.max_len = max_len
        self.model_id = model_id

    @abstractmethod
    def predict(self, pairs: List[Tuple[str, str]]) -> List[int]:
        pass


class ModelClassifier(Classifier):
    def __init__(self,
                 fdict: str,
                 model_id: int,
                 max_len: int,
                 model):

        super().__init__(fdict, model_id, max_len)
        self._load_dict(self.fdict)
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


def load_classifiers() -> List[Classifier]:
    with open(os.path.join(
            Settings.models_dir(),
            'classifiers.json')) as f:
        classifiers = json.load(f)

    cl_objects: List[Classifier] = []
    for model_id, sett in enumerate(classifiers):
        cl_objects.append(load_classifier(sett['model'],
                                          model_id,
                                          sett['dict'],
                                          sett['pc'],
                                          sett['multi'],
                                          sett['max_len']))
    return cl_objects


def load_classifier(
        fmodel: str,
        model_id: int,
        fdict: str,
        pc: bool,
        multi: bool,
        max_len: int) -> ModelClassifier:

    model = load_model(
        os.path.join(
            Settings.models_dir(),
            fmodel))

    if pc:
        if multi:
            return MultiParentAndChild(fdict, model_id, max_len, model)
        else:
            return SingleParentAndChild(fdict, model_id, max_len, model)
    else:
        if multi:
            return MultiOnlyChild(fdict, model_id, max_len, model)
        else:
            return SingleOnlyChild(fdict, model_id, max_len, model)


if __name__ == '__main__':
    load_classifiers()
