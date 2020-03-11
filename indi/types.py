from __future__ import annotations
from typing import Dict, Optional, Union, Tuple, Iterable, cast

Facts = Dict[str, float]
Nums = Dict[int, Facts]
Result = Optional[float]
Pair = Tuple[str, str]
Triple = Tuple[str, str, int]


class EmptyPlace(object):
    def __neg__(self) -> EmptyPlace:
        return eph

    def __add__(self, other):
        if other is not None:
            return other
        else:
            return eph

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        if other is not None:
            return -other
        else:
            return eph

    def __rsub__(self, other):
        return self.__sub__(-other)

    def __mul__(self, other):
        return eph

    def __rmul__(self, other):
        return eph

    def __truediv__(self, other):
        return eph

    def __rtruediv__(self, other):
        return eph

    def __abs__(self):
        return eph

    def __pow__(self, other):
        return eph

    def __rpow__(self, other):
        return eph

    def __bool__(self):
        return False

    def __repr__(self):
        return "empty value"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if other is None:
            return True
        if isinstance(other, EmptyPlace):
            return True
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


eph = EmptyPlace()


def assign(a, b):
    if a != eph and a is not None:
        return a

    if b == eph or b is None:
        return a

    return b


def nansum(values: Iterable[Union[Optional[float], EmptyPlace]]
           ) -> Tuple[Union[float, EmptyPlace], int]:
    kokku = eph
    count = 0
    for v in filter(lambda x: x is not None and x != eph, values):
        count += 1
        kokku = kokku + v

    return kokku, count


def nanprod(values: Iterable[Union[Optional[float], EmptyPlace]]
            ) -> Tuple[Union[float, EmptyPlace], int]:
    kokku = 1.0
    count = 0
    for v in filter(lambda x: x is not None and x != eph, values):
        kokku = kokku * v  # type: ignore
        count += 1
    if count:
        return kokku, count
    else:
        return eph, count


def nanmin(values: Iterable[Union[Optional[float],
                                  EmptyPlace]]) -> Union[EmptyPlace, float]:
    mn: Union[EmptyPlace, float] = eph
    for v in filter(lambda x: x is not None and x != eph, values):
        v = cast(float, v)
        if mn == eph:
            mn = v
        else:
            if mn > v:  # type: ignore
                mn = v

    return mn


def nanmax(values: Iterable[Union[Optional[float],
                                  EmptyPlace]]) -> Union[EmptyPlace, float]:
    mx: Union[EmptyPlace, float] = eph
    for v in filter(lambda x: x is not None and x != eph, values):
        v = cast(float, v)
        if mx == eph:
            mx = v
        else:
            if mx < v:  # type: ignore
                mx = v

    return mx


NoneFact = Union[EmptyPlace, float]
NoneFacts = Dict[str, NoneFact]

if __name__ == '__main__':
    print(nanprod([None, eph, 1, 0]))
