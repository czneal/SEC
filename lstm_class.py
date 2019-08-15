# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 14:15:03 2019

@author: Asus
"""
import numpy as np
from keras.preprocessing import sequence
from keras.models import load_model
import re
import pandas as pd
import copy

class ClassifierPool(object):
    def __init__(self):
        df = (pd.read_csv('mgparams/classifiers.csv', sep='\t')
                                .set_index('class_model')
                                .sort_values(by=['parent_class'], na_position='first')
                                )

        self.ref_table = df.where((pd.notnull(df)), None)

        self.pool = {}
        self._init_classifiers()
        return

    def _init_classifiers(self):
        for class_model, record in self.ref_table.iterrows():
            class_name = None
            if record['pc'] == 'pc':
                class_name = MulticlassPC
            else:
                class_name = TrimmerClassC

            parent_class = record['parent_class']
            if parent_class in self.pool:
                parent_class = self.pool[parent_class]

            self.pool[class_model] = class_name(fdict='mgparams/'+'dictionary.csv',
                fmodel = 'mgparams/'+class_model,
                max_len = record['max_len'],
                start_chapter = record['start_chapter'],
                start_tag = record['start_tag'],
                parent_class = parent_class,
                parent_class_id = record['parent_class_id'])

    def get_classifier(self, class_model):
        return self.pool[class_model]

    def predict_all(self, structure):
        for model_name, model in self.pool.items():
            if model.parent_class is None:
                model.predict_all(structure)
        for model_name, model in self.pool.items():
            if model.parent_class is not None:
                model.predict_all(structure)
        return

class ModelClassifier(object):
    def remove_prefix(tag):
        if ":" in tag:
            return tag.split(":")[-1]
        else:
            return tag

    def __init__(self, fdict, fmodel, max_len, start_chapter, start_tag,
                 parent_class, parent_class_id):
        self.predicted = None
        self.max_len = max_len
        self.start_chapter = start_chapter
        self.start_tag = start_tag
        self._load_dict(fdict)
        self._load_model(fmodel)
        self.parent_class = parent_class
        self.parent_class_id = parent_class_id
        self.walk_method = 0 #or 0 - 'leaf', 1 - 'full'
        self.structure_tab = None
        self.fmodel = fmodel.split('/')[-1]

    def description(self):
        return 'model: {0}\nchapter: {1}\ntag: {2}\nmax_len: {3}'.format(
                self.fmodel, self.start_chapter, self.start_tag,
                self.max_len)

    def concatenate_arrays(self, parent, child):
        half_len = int(self.max_len/2)
        conc_array = np.zeros((len(parent), self.max_len))
        conc_array[conc_array==0]=1
        for i in range(len(parent)):
            for j in range(0,min(half_len, len(parent[i]))):
                conc_array[i][j]=parent[i][j]
            for j in range(0,min(half_len, len(child[i]))):
                conc_array[i][j+half_len] = child[i][j]
        return(conc_array)

    def _load_dict(self, filename):
        with open(filename) as f:
            self.tag_to_code = {l.split("\t")[0]:l.replace("\n","").split("\t")[1] for l in f}

    def _load_model(self, filename):
        self.model = load_model(filename)

    def to_predict(self, tag):
        tag = ModelClassifier.remove_prefix(tag)

        t = re.findall('[A-Z][^A-Z]*', tag)
        k= [self.tag_to_code.get(j,1) for j in t]
        to_predict = np.zeros((1,len(k)))
        to_predict[0]=k
        return(to_predict)

    def predict(self, parent, child):
        to_predict = self.prepare_predict(parent, child)
        predicted = self.model.predict(to_predict)[0]

        if predicted.shape[0] == 1:
            if predicted>=0.5:
                return 1
            else:
                return 0

        w = np.argmax(predicted)
        return int(w)

    def _start_points(self, structure):
        if self.start_tag is None:
            for (_, _, _, start) in to.enumerate_chapter_tags(structure, self.start_chapter):
                yield start
        else:
            for (_, _, _, start) in to.enumerate_tags_basic(structure,
                                        chapter = self.start_chapter,
                                        tag = self.start_tag):
                yield start

    def _walk(self, start):
        for (p, c, w, _, _, leaf) in to.enumerate_tags_basic_leaf(start):
            if (self.walk_method == 0) and not leaf: continue
            yield (p, c, w, leaf)

    def enumerate_tags(self, structure):
        if self.parent_class is None:
            for start in self._start_points(structure):
                for (p, c, w, leaf) in self._walk(start):
                    yield (p, c, w, leaf)
        else:
            f = self.parent_class.predicted
            f = f[f['class_id'] == self.parent_class_id]
            for index, row in f.iterrows():
                yield (row['parent'], row['child'], row['w'], True)

    def predict_all(self, structure):
        '''
        make prediction for all tags in structure
        '''
        data = []
        for (p, c, w, leaf) in self.enumerate_tags(structure):
            class_id = self.predict(p, c)
            data.append([p,c,w,class_id])

        self.predicted = pd.DataFrame(data, columns=['parent', 'child', 'w', 'class_id'])
        self._structure_tab(structure)

        return

    def _structure_tab(self, structure):
        if self.parent_class is not None:
            self.structure_tab = self.parent_class.structure_tab
            return

        data = []
        index = 0
        for point in self._start_points(structure):
            data.append([index, point['name'], 1, ''])
            index += 1
            for (_, c, w, _, off) in to._enumerate_tags_basic(point):
                data.append([index, c, w, off+'  '])
                index += 1

        self.structure_tab = pd.DataFrame(data, columns = ['ord', 'sname', 'weight', 'offs'])

class MulticlassPC(ModelClassifier):
    def prepare_predict(self, parent, child):
        parent_to_predict = self.to_predict(parent)
        child_to_predict = self.to_predict(child)
        to_predict = self.concatenate_arrays(parent_to_predict, child_to_predict)
        to_predict = sequence.pad_sequences(to_predict, maxlen=self.max_len)

        return to_predict

class MulticlassC(ModelClassifier):
    def prepare_predict(self, parent, child):
        to_predict = self.to_predict(child)
        to_predict = sequence.pad_sequences(to_predict, maxlen=self.max_len)

        return(to_predict)

class TrimmerClass(ModelClassifier):
    def _trim_structure_req(self, parent, start):
        if self.predict(parent, start['name']) == 0:
            start['children'] = None
        else:
            if start['children'] is not None:
                for _, child in start['children'].items():
                    self._trim_structure_req(start['name'], child)
        return

    def _trim_structure(self, structure):
        new = copy.deepcopy(structure)
        for start in super()._start_points(new):
            self._trim_structure_req('', start)

        return new

    def predict_all(self, structure):
        trimmed = self._trim_structure(structure)

        data = []
        for (p, c, w, leaf) in self.enumerate_tags(trimmed):
            data.append([p, c, w, 1])

        self.predicted = pd.DataFrame(data, columns=['parent', 'child', 'w', 'class_id'])
        self._structure_tab(trimmed)
        return

class TrimmerClassC(MulticlassC, TrimmerClass):
    def __init__(self, fdict, fmodel, max_len, start_chapter, start_tag,
                 parent_class, parent_class_id):
        super().__init__(fdict, fmodel, max_len, start_chapter, start_tag,
                 parent_class, parent_class_id)
        self.walk_method = 1
        return
