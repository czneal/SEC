# -*- coding: utf-8 -*-

from typing import TypeVar, Dict, List, Union

StrInt = TypeVar('StrInt', str, int)
TRecord = Dict[str, StrInt]
TRecordPair = List[Union[TRecord, str]]