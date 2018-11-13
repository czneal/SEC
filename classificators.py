# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:45:52 2018

@author: Asus
"""
import re
import numpy as np
import database_operations as do
import tree_operations as to
import json
import pandas as pd
from settings import Settings
from keras.preprocessing import sequence
from keras.models import load_model

class MainSheets(object):
    def __init__(self):
        self.rebs = re.compile('.*balance.*sheet.*|.*financial.*position.*|.*financial.*condition.*|.*statement.*condition.*|.*assets.*liabilities.*|.*statement.*assets.*', re.I)
        self.reis = re.compile('.*income.*statement.*|.*statement.*income.*|.*statement.*operation.*|.*statement.*earning.*|.*statement.*loss.*|.*result.*operation.*|.*comprehensive.*income.*|.*comprehensive.*loss.*', re.I)
        self.recf = re.compile('.*cash.*flow.*', re.I)
        self.detail = re.compile('.*\(detail.*\).*', re.I)
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
    
class ChapterClassificator(object):
    def match(chapter_name):
        chap = None
        if ChapterClassificator.match_balance_sheet(chapter_name):
            chap = "bs"
        if ChapterClassificator.match_cash_flow(chapter_name):
            chap = "cf"
        if ChapterClassificator.match_income_statement(chapter_name):
            chap = "is"
            
        return chap
    
    def match_balance_sheet(chapter_name):
        words = ["balance","financial","condition"]
        return ChapterClassificator.match_words(words, chapter_name)
    
    def match_income_statement(chapter_name):
        words = ["income", "operations", "earnings", "comprehensive*.loss"]
        return ChapterClassificator.match_words(words, chapter_name)    
            
    def match_cash_flow(chapter_name):
        words = ["cash.*flow"]
        return ChapterClassificator.match_words(words, chapter_name)    
    
    def match_words(words, chapter_name):
        for w in words:
            p = re.compile(".*"+w+".*", re.IGNORECASE)
            if p.match(chapter_name) is not None:
                return True
        return False

  
class Classifier(object):        
    def concatenate_arrays(parent, child, max_len):
        half_len = int(max_len/2)
        conc_array = np.zeros((len(parent) ,max_len))
        conc_array[conc_array==0]=1
        for i in range(len(parent)):
            for j in range(0,min(half_len,len(parent[i]))):
                conc_array[i][j]=parent[i][j]
            for j in range(0,min(half_len,len(child[i]))):
                conc_array[i][j+half_len]=child[i][j]        
        return(conc_array)
    
    def remove_prefix(tag):
        if ":" in tag:
            return tag.split(":")[-1]
        else:
            return tag
        
    def predict_value(self, nums, structure):
        
        return 0.0
    
class ModelClassifier(Classifier):
    def __init__(self, fdict, fmodel):        
        self._load_dict(fdict)
        self._load_model(fmodel)
        
    def _load_dict(self, filename):
        with open(filename) as f:
            self.tag_to_code = {l.split(";")[0]:l.replace("\n","").split(";")[1] for l in f}
    
    def _load_model(self, filename):
        self.model = load_model(filename)
    
class LiabClassStub(Classifier):
    def predict(self, p, c):
        return 1.0

class LiabClassSingle(ModelClassifier):            
    def predict(self, parent, tag):
        tag = LiabClassSingle.remove_prefix(tag)
        
        t = re.findall('[A-Z][^A-Z]*', tag)
        k= [self.tag_to_code.get(j,1) for j in t]
        to_predict = np.zeros((1,len(k)))
        to_predict[0]=k
        
        maxlen = 7  # обучал для последовательности из 7 кусков
        to_predict = sequence.pad_sequences(to_predict, maxlen=maxlen)# заполняем если короче

        predicted = self.model.predict(to_predict)
        
        return predicted
    
class LiabClassPC(ModelClassifier):    
    def predict(self, parent, child):
        parent = LiabClassPC.remove_prefix(parent)
        child = LiabClassPC.remove_prefix(child)
        
        t = re.findall('[A-Z][^A-Z]*', parent)
        k_parent= [self.tag_to_code.get(j,1) for j in t] 
        
        t = re.findall('[A-Z][^A-Z]*', child)
        k_child= [self.tag_to_code.get(j,1) for j in t]
        
        maxlen = 12 #word length len(parent) + len(child) 
        #max_features = 1314 #dict length
        
        to_predict_parent = np.zeros((1, len(k_parent)))
        to_predict_parent[0]= k_parent
        
        to_predict_child = np.zeros((1, len(k_child)))
        to_predict_child[0]= k_child
        to_predict  = Classifier.concatenate_arrays(to_predict_parent, 
                                                       to_predict_child,
                                                       maxlen)
        
        to_predict = sequence.pad_sequences(to_predict, maxlen=maxlen)
        predicted = self.model.predict(to_predict)
        
        return predicted

class LiabClassMixed(LiabClassPC):
    def __init__(self, fdict, fmodel):
        super().__init__(fdict, fmodel)
        self.stat_class = LiabilitiesStaticClassifier("LbClf/liab_stat.csv")
        
    def predict(self, parent, child):
        predict = self.stat_class.predict(parent, child)
        if predict > 0.5:
            return predict
        else:
            return super().predict(parent, child)

class StaticClassifier(Classifier):
    def __init__(self, filename, count=100):
        self.stat = set()
        with open(filename) as f:
            for l in f:
                l = l.split("\t")
                if int(l[1]) >= count:
                    self.stat.add(l[0])
                    
    def predict(self, parent, child):
        if child in self.stat:
            return 1.0
        else:
            return 0.0
        
class LiabilitiesStaticClassifier(StaticClassifier):
    def fill_stat(stat, structure):
        for elem in to._enumerate_tags_basic(structure, 
                                             tag="us-gaap:Liabilities", 
                                             chapter="bs"):
            for child in to._enumerate_tags_basic(elem[3]):
                if child[1] in stat:
                    stat[child[1]] += 1
                else:
                    stat[child[1]] = 1
                    
    def make(filename):
        try:
            con = do.OpenConnection()
            cur = con.cursor(dictionary = True)
            sql = ("select * from reports where fin_year between {0} and {1} ".
                       format(Settings.years()[0], Settings.years()[1]))
            cur.execute(sql + Settings.select_limit())
            stat = {"us-gaap:Liabilities":10000}
            for r in cur:
                structure = json.loads(r["structure"])
                LiabilitiesStaticClassifier.fill_stat(stat, structure)
            (pd.DataFrame.
                 from_dict(stat, orient='index', columns=["cnt"]).
                 sort_values("cnt", ascending=False).
                 to_csv(filename, sep='\t', header=False))
        except:
            raise
        finally:
            con.close()        
            
class LSHEDirectChildrenClassifier(StaticClassifier):
    def make(filename):
        try:
            con = do.OpenConnection()
            cur = con.cursor(dictionary = True)
            sql = ("select * from reports where fin_year between {0} and {1} ".
                       format(Settings.years()[0], Settings.years()[1]))
            cur.execute(sql + Settings.select_limit())
            stat = {}
            for r in cur:
                structure = json.loads(r["structure"])
                for elem in to._enumerate_tags_basic(structure, 
                                 tag="us-gaap:LiabilitiesAndStockHoldersEquity", 
                                 chapter="bs"):
                    if elem is None or elem[3] is None or elem[3]["children"] is None:
                        continue
                    
                    for child in elem[3]["children"]:
                        if child in stat:
                            stat[child] += 1
                        else:
                            stat[child] = 1
                            
            (pd.DataFrame.
                 from_dict(stat, orient='index', columns=["cnt"]).
                 sort_values("cnt", ascending=False).
                 to_csv(filename, sep='\t', header=False))
        except:
            raise
        finally:
            con.close()