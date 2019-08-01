# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 15:34:47 2019

@author: Asus
"""
import re
import pandas as pd

class MainSheets(object):
    def __init__(self):
        self.rebs = re.compile('.*balance.*sheet.*|.*financial.*position.*|.*finanical.*position.*|.*financial.*condition.*|.*statement.*condition.*|.*assets.*liabilities.*|.*statement.*assets.*', re.I)
        self.reis = re.compile('.*income.*statement.*|.*statement.*income.*|.*statement.*operation.*|.*statement.*earning.*|.*statement.*loss.*|.*result.*operation.*|.*comprehensive.*income.*|.*comprehensive.*loss.*', re.I)
        self.recf = re.compile('.*cash.*flow.*', re.I)
        self.detail = re.compile('.*\(detail.*\).*', re.I)
        self.rescores = [(re.compile('.*parenth.*', re.I), 1000),                         
                         (re.compile('.*compre.*', re.I), 10),
                         (re.compile('.*supplem.*', re.I), 10),
                         (re.compile('.*retain.*', re.I), 10),
                         (re.compile('.*\(.+\).*', re.I), 1),
                         (re.compile('.+-.+', re.I),10),
                         (re.compile('.*assets.*', re.I), 1),
                         (re.compile('.*changes.*', re.I), 1),
                         (re.compile('.*operations.*', re.I), -10)]
        
    def match_bs(self, label):
        if self.detail.match(label):
            return False
        if self.rebs.match(label):
            return True
        return False

    def match_is(self, label):
        if self.detail.match(label):
            return False
        if self.reis.match(label):
            return True
        return False

    def match_cf(self, label):
        if self.detail.match(label):
            return False
        if self.recf.match(label):
            return True
        return False

    def match(self, label):
        if (self.match_bs(label) or
            self.match_cf(label) or
            self.match_is(label)):
            return True
        return False
    
    def select_ms(self, labels, priority = None):
        if priority is None:
            priority = [1 for l in labels]
        assert len(labels) == len(priority)
        
        scores = []
        for label, p in zip(labels, priority):
            sheet = ''
            if self.match_bs(label):
                sheet = 'bs'
            if self.match_is(label):
                sheet = 'is'
            if self.match_cf(label):
                sheet = 'cf'
            
            if sheet == '': continue
        
            label_loss = re.sub('\(loss\)', 'Loss', label, flags=re.I)
            
            score = 0
            for reg, w in self.rescores:
                if reg.match(label_loss): score += w
                
            scores.append([label, sheet, score, p])
        
        df = pd.DataFrame(scores, columns=['label', 'sheet', 'score', 'p'])
        mins = df.groupby(by='sheet')['score'].min()
        maxs = df.groupby(by='sheet')['p'].max()
        main = {}
        for sheet in mins.index:
            f = df[(df['sheet'] == sheet) & 
                   (df['score'] == mins.loc[sheet]) &
                   (df['p'] == maxs.loc[sheet])]
            
            for index, row in f.iterrows():
                main[row['label']] = sheet
                
        return main
    
if __name__ == '__main__':
    ms = MainSheets()
    labels = ["CONSOLIDATED BALANCE SHEET (Unaudited)",
                    "CONSOLIDATED BALANCE SHEET (Parenthetical)",
                    "CONSOLIDATED STATEMENTS OF INCOME (Unaudited)",
                   "CONSOLIDATED STATEMENTS OF COMPRENENSIVE INCOME (LOSS) (Unaudited)",
                    "CONSOLIDATED STATEMENTS OF CHANGES IN SHAREHOLDERS' EQUITY",
                    "CONSOLIDATED STATEMENTS OF CASH FLOWS (Unaudited)"
                    ]
    msl = ms.select_ms(labels)
    print(msl)