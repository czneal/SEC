# -*- coding: utf-8 -*-
"""
Created on Tue May 21 18:09:14 2019

@author: Asus
"""

import database_operations as do
import classificators as cl
from utils import ProgressBar
import json
from log_file import LogFile

if __name__ == '__main__':
    data = do.getquery("select cik, adsh, structure from raw_reps where fy = 2018")
    
    log = LogFile('outputs/chapters.log')
    ms = cl.MainSheets()
    
    pb = ProgressBar()        
    pb.start(len(data))
    
    for row in data:
        structure = json.loads(row['structure'])
        labels = [label for label in structure]
        
        labels = ms.select_ms(labels)
        if len(labels) > 3:
            for label, sheet in labels.items():
                log.writemany(row['cik'], row['adsh'], sheet, info=label)
        
        pb.measure()
        print('\r' + pb.message(), end="")
    print()

