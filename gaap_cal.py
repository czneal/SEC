# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 16:51:16 2019

@author: Asus
"""

from structure.taxonomy import Taxonomy
from structure import tree_operations as to
import json
import pandas as pd
from settings import Settings

data = []
for y in range(2011, 2020):
    tx=Taxonomy(str(y) + '-01-31')
    tx.read()
    
    for index, row in tx.taxonomy.iterrows():
        structure = json.loads(row['structure'])        
        for p, c, w, _ in to.enumerate_tags_basic(structure):
            data.append([y, row['sheet'], row['type'], p, c, w])
            
df = pd.DataFrame(data, columns=['year', 'sheet', 'type', 'parent', 'child', 'weight'])
df.to_csv(Settings.output_dir() + 'taxonomy.csv', sep='\t')
