import json
import os
import re
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Tuple, TypeVar, cast

import numpy as np
from tensorflow.keras.models import load_model  # type: ignore

from settings import Settings
import indi.indcache


class Classifier(metaclass=ABCMeta):
    def __init__(
            self,
            fdict: str,
            model_id: int,
            max_len: int,
            model_name: str):
        self.fdict = fdict
        self.max_len = max_len
        self.model_id = model_id
        self.model_name = model_name

    @abstractmethod
    def predict(self, pairs: List[Tuple[str, str]]) -> List[int]:
        pass

    @abstractmethod
    def description(self) -> str:
        pass


class KerasModel(object):
    @abstractmethod
    def predict(self, x: np.ndarray) -> np.ndarray:
        pass


class ModelClassifier(Classifier):
    def __init__(self,
                 fdict: str,
                 model_id: int,
                 max_len: int,
                 model_name: str,
                 model: KerasModel):

        super().__init__(fdict, model_id, max_len, model_name)
        self._load_dict(self.fdict)
        self.model = model
        self.dict_name = fdict

    def _load_dict(self, filename: str) -> None:
        with open(Settings.models_dir() + filename) as f:
            self.tag_to_code = {
                l.split("\t")[0]: l.replace("\n", "").split("\t")[1]
                for l in f}

    def _to_vector(self, tag: str, x: np.ndarray, row: int, pos: int) -> None:
        tag = tag.split(':')[-1]
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
        labels = indi.indcache.check(pairs, self.model_id)
        u_pairs = [pair for _, pair in filter(
            lambda x: x[0] == -1, zip(labels, pairs))]

        u_labels = self.predict_calc(u_pairs)
        indi.indcache.append(u_pairs, u_labels, self.model_id)
        it = iter(u_labels)

        for i in range(0, len(labels)):
            if labels[i] == -1:
                labels[i] = it.__next__()

        return labels

    def predict_calc(self, pairs: List[Tuple[str, str]]) -> List[int]:
        if not pairs:
            return []

        x = np.ones((len(pairs), self.max_len))
        for i, (parent, child) in enumerate(pairs):
            self._vectorize(parent, child, x, i)

        y = self.model.predict(x)
        return self._explain_results(y)

    def description(self) -> str:
        return (f'model: {self.model_name}\n' +
                f'model_id: {self.model_id}\n' +
                f'max_len: {self.max_len}\n'
                f'dictionary: {self.dict_name}')


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


models: Dict[str, KerasModel] = {}


def get_model(model_name: str) -> KerasModel:
    if model_name in models:
        return models[model_name]

    model = load_model(
        os.path.join(
            Settings.models_dir(),
            model_name))
    models[model_name] = model
    return cast(KerasModel, model)


def create(
        model_name: str,
        model_id: int,
        fdict: str,
        pc: bool,
        multi: bool,
        max_len: int) -> ModelClassifier:

    model = get_model(model_name)
    if pc:
        if multi:
            return MultiParentAndChild(
                fdict, model_id, max_len, model_name, model)
        else:
            return SingleParentAndChild(
                fdict, model_id, max_len, model_name, model)
    else:
        if multi:
            return MultiOnlyChild(fdict, model_id, max_len, model_name, model)
        else:
            return SingleOnlyChild(fdict, model_id, max_len, model_name, model)


if __name__ == '__main__':
    pass
