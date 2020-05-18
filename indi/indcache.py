from typing import Dict, List, Tuple
from indi.types import Pair, Triple

__cache: Dict[Triple, int] = {}


def check(pairs: List[Pair], model_id: int) -> List[int]:
    labels: List[int] = []
    for pair in pairs:
        labels.append(__cache.get(pair + (model_id,), -1))

    return labels


def append(pairs: List[Pair],
           labels: List[int],
           model_id: int) -> None:

    for pair, label in zip(pairs, labels):
        __cache[pair + (model_id,)] = label


def to_list() -> List[Tuple[str, str, int, int]]:
    return [k + (v,) for k, v in __cache.items()]


def reset() -> None:
    __cache.clear()


if __name__ == "__main__":
    pass
