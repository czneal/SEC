# -*- coding: utf-8 -*-
import re

import pandas as pd
import numpy as np

from typing import List

def split_company_name(company_name: str) -> List[str]:
    company_name = (company_name
                    .lower()
                    .replace('.com', ' com')
                    .replace('corporation', 'corp')
                    .replace('company', 'co')
                    .replace('incorporated', 'inc')
                    .replace('limited', 'ltd')
                    .replace('the', ''))
    company_name = re.sub('\.+|\'+', '', company_name)
    company_name = re.sub('/.{2,5}$', '', company_name)
    company_name = re.sub('\&\#\d+;', '', company_name)
    symbols = {',':' ','.':'','/':' ','&':' ','-':' ',
               '\\':' ','\'':'','(':' ',')':' ','#':' ',
               ':':' ','!':' ', ' ': ' '}
               
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
    
    return len(words1.intersection(words2))*2/(len(words1) + len(words2))
    
def cap(market_cap: str) -> float:
    if pd.isna(market_cap):
        return np.nan
    
    value = re.findall(r'\d+\.*\d*', market_cap)
    if not value:
        return np.nan
    value = float(value[0])
    mult = 1.0
    if 'm' in market_cap.lower():
        mult = 1000000
    if 'b' in market_cap.lower():
        mult = 1000000000
        
    return value*mult