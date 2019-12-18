# -*- coding: utf-8 -*-
import datetime
from typing import Tuple, Dict, Union

TRecord = Dict[str, Union[str, int, datetime.date]]
TRecordPair = Tuple[TRecord, str]
StrInt = Union[str, int]
