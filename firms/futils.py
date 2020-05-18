# -*- coding: utf-8 -*-
import re
import datetime

import pandas as pd
import numpy as np

from typing import List, Optional, Union, cast, Dict, Any, Sequence

from utils import str2date


def split_company_name(company_name: str) -> List[str]:
    company_name = (company_name
                    .lower()
                    .replace('.com', ' com')
                    .replace('corporation', 'corp')
                    .replace('company', 'co')
                    .replace('incorporated', 'inc')
                    .replace('limited', 'ltd')
                    .replace('the', ''))
    company_name = re.sub(r'\.+|\'+', '', company_name)
    company_name = re.sub(r'/.{2,5}$', '', company_name)
    company_name = re.sub(r'\&\#\d+;', '', company_name)
    symbols = {',': ' ', '.': '', '/': ' ', '&': ' ', '-': ' ',
               '\\': ' ', '\'': '', '(': ' ', ')': ' ', '#': ' ',
               ':': ' ', '!': ' ', ' ': ' '}

    parts = [company_name]
    for symbol in symbols:
        new_parts = []
        for part in parts:
            new_parts.extend([p.strip().lower() for p in part.split(symbol)
                              if p.strip()])
        parts = new_parts

    return parts


def distance(name1: str, name2: str) -> float:
    words1 = set(split_company_name(name1))
    words2 = set(split_company_name(name2))

    return len(words1.intersection(words2)) * 2 / (len(words1) + len(words2))


def cap(market_cap: str) -> float:
    if pd.isna(market_cap):
        return cast(float, np.nan)

    value = re.findall(r'\d+\.*\d*', market_cap)
    if not value:
        return cast(float, np.nan)
    value = float(value[0])
    mult = 1.0
    if 'm' in market_cap.lower():
        mult = 1000000
    if 'b' in market_cap.lower():
        mult = 1000000000

    return value * mult


def convert_decimal(value: Optional[Union[str, float]]) -> Optional[float]:
    if pd.isna(value):
        return None

    if isinstance(value, float):
        return value
    value = cast(str, value)
    try:
        return float(value.replace('$', '').replace(',', ''))
    except ValueError:
        return None


def convert_date(value: Optional[str]) -> Optional[datetime.date]:
    if pd.isna(value):
        return None
    try:
        return str2date(value, pattern='mdy')
    except AssertionError:
        return None


def select_data(data: Dict[str, Any], key_seq: Sequence[str]) -> Optional[str]:
    try:
        value = data
        for key in key_seq:
            value = value[key]
    except (KeyError, TypeError):
        return None

    return cast(str, value)


def stock_type(stock_str: str) -> str:
    stock_str = re.sub(r'\r+|\n+|\t', '', stock_str)
    types = {
        'warrant': re.compile(r'.*\bwarrants?\b.*', re.I),
        'derivative': re.compile(r'.*%.*|.*consist.*|.*\bnotes?\b.*', re.I),
        'right': re.compile(r'.*\brights?\b.*', re.I),
        'unit': re.compile(r'.*\bunits?\b.*', re.I),
        'stock': re.compile(r'.*\bstocks?\b.*', re.I),
        'share': re.compile(r'.*\bshares?\b.*', re.I),
        'adr': re.compile(
            r'.*\badr\b.*|.*\bads\b.*|.*\bdeposit[o,a]ry\b.*', re.I),
        'com': re.compile(r'.*\bcom{1,3}on\b.*', re.I),
        'ord': re.compile(r'.*\bordinary\b.*', re.I),
        'pref': re.compile(r'.*\bpreferred\b.*', re.I),
        'fund': re.compile(r'.*\bfund\b.*', re.I),
        'bene': re.compile(r'.*\bbeneficial\b.*', re.I),
        'other': re.compile(r'.*\bother\b.*', re.I)}
    words: List[str] = [k for k, v in types.items() if v.match(stock_str)]

    class_re = re.compile(r'(class|series)\s+\b([A-Z])\b', re.I)
    class_letter = class_re.findall(stock_str)
    if class_letter:
        words.append(class_letter[0][0].lower() + class_letter[0][1].upper())
    return '.'.join(words)


def date_from_timestamp(time_str: Optional[str]) -> Optional[datetime.date]:
    if time_str is None:
        return None

    try:
        date_str = re.findall(r'\w{3,5}\s*\d{1,2},\s*\d{4}', time_str)[0]
        dt = datetime.datetime.strptime(date_str, '%b %d, %Y')
        return dt.date()
    except (TypeError, ValueError):
        return None
