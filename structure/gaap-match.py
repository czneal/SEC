# -*- coding: utf-8 -*-
"""
Created on Fri May  3 10:48:17 2019

@author: Asus
"""
import json
from structure import taxonomy as tx
from structure import tree_operations as to
import pandas as pd


def count_chapters(structure):
    """
    return dictionary {'chapter label':set(tags)}
    """
    retval = {}
    structure = json.loads(structure)
    for label, chapter in structure.items():
        tags = set()
        for tag, tag_struct in chapter.items():
            tags.add(tag)
            for e in to._enumerate_tags_basic(tag_struct):
                if e[0].startswith('us-gaap'):
                    tags.add(e[0])
                if e[1].startswith('us-gaap'):
                    tags.add(e[1])

        retval[label] = tags

    return retval


def best_gaap_match(taxonomy, structure) -> pd.DataFrame:
    """
    taxonomy - dataframe returned by count_taxonomy
    structure - string with calculation scheme

    return [['chapter label', sheet, type, diff],...]
    best_gaap - best gaap chapter correspond to 'chapter label'
    diff - number of tags in chapter not present in gaap chapter
    """
    data = []
    for label, chapter_tags in count_chapters(structure).items():
        v = []
        for index, row in taxonomy.iterrows():
            diff = chapter_tags.difference(row['tags'])

            v.append(
                (index, len(diff) + 1.0 / len(row['tags']),
                 diff, len(diff)))

        v = sorted(v, key=lambda x: x[1], reverse=False)
        i, diff, diff_set, _ = v[0]
        data.append([label,
                     taxonomy.iloc[i]['sheet'], taxonomy.iloc[i]['type'],
                     diff, diff_set])

    return pd.DataFrame(
        data,
        columns=[
            'label',
            'sheet',
            'type',
            'diff',
            'diff_set'])


def count_taxonomy(taxonomy):
    """

    return dataframe columns=[sheet, type, set(tags)]
    """
    data = []
    for _, row in taxonomy.iterrows():
        c = count_chapters(row['structure'])
        for _, tags in c.items():
            data.append([row['sheet'], row['type'], tags])

    return pd.DataFrame(data, columns=['sheet', 'type', 'tags'])
