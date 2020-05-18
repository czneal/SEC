# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 15:34:47 2019

@author: Asus
"""
import re
import pandas as pd  # type: ignore


class MainSheets():
    sheets_re = (('bs', re.compile('.*balance.*sheet.*|' +
                                   '.*financial.*position.*|' +
                                   '.*finanical.*position.*|' +
                                   '.*financial.*condition.*|' +
                                   '.*statement.*condition.*|' +
                                   '.*assets.*liabilities.*|' +
                                   '.*statement.*assets.*', re.I)),
                 ('is', re.compile('.*income.*statement.*|' +
                                   '.*statement.*income.*|' +
                                   '.*statement.*operation.*|' +
                                   '.*statement.*earning.*|' +
                                   '.*statement.*loss.*|' +
                                   '.*result.*operation.*|' +
                                   '.*comprehensive.*income.*|' +
                                   '.*comprehensive.*loss.*', re.I)),
                 ('cf', re.compile('.*cash.*flow.*', re.I)),
                 ('se', re.compile('.*stockhold.*|' +
                                   '.*statement.*equit.*|' +
                                   '.*shareholder.*|' +
                                   '.*partner.*capital.*', re.I)))

    @staticmethod
    def sheets():
        return [e for e in MainSheets.sheets_re]

    def __init__(self):
        self.detail = re.compile(r'.*\(detail.*\).*', re.I)
        self.rescores = [(re.compile('.*parenth.*', re.I), 1000),
                         (re.compile('.*compre.*', re.I), 100),
                         (re.compile('.*supplem.*', re.I), 100),
                         (re.compile('.*retain.*', re.I), 100),
                         (re.compile(r'(?<=\().*?(?=\))', re.I), 10),
                         (re.compile('.+-.+', re.I), 10),
                         (re.compile('.*assets.*', re.I), 1),
                         (re.compile('.*stockhold.*', re.I), 1),
                         (re.compile('.*changes.*', re.I), 1),
                         (re.compile('.*operations.*', re.I), -10)]

    def match(self, label):
        return self.match_sheet(label) != ''

    def match_sheet(self, label: str) -> str:
        if self.detail.match(label) is not None:
            return ''
        for (sheet, match) in self.sheets_re:
            if match.match(label) is not None:
                return sheet
        return ''

    def select_ms(self, labels, priority=None):
        if priority is None:
            priority = [1 for l in labels]

        assert len(labels) == len(priority)

        scores = []
        for label, p in zip(labels, priority):
            sheet = self.match_sheet(label)
            if sheet == '':
                continue

            score = 0
            for reg, w in self.rescores:
                score += w * len(reg.findall(label))

            scores.append([label, sheet, score, p])

        df = pd.DataFrame(scores, columns=['label', 'sheet', 'score', 'p'])
        mins = df.groupby(by='sheet')['score'].min()
        mains = {}
        for sheet in mins.index:
            f = df[(df['sheet'] == sheet) &
                   (df['score'] == mins.loc[sheet])]

            f = f[f['p'] == f['p'].max()]

            for _, row in f.iterrows():
                mains[row['label']] = sheet

        return mains


def main():
    import json

    ms = MainSheets()
    a = json.loads("""[["15 Consolidated Balance Sheets (LLC)", 50], ["21 Consolidated Statements of Cash Flows (LLC)", 49], ["29 Consolidated Statements of Operations (LLC)", 62], ["30 Consolidated Statements of Operations (MEC)", 42]]""")
    labels = []
    priority = []
    for l, p in a:
        labels.append(l)
        priority.append(p)
    msl = ms.select_ms(labels, priority)
    print(msl)


if __name__ == '__main__':
    pass
