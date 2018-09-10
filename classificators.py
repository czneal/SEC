# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:45:52 2018

@author: Asus
"""
import re
import numpy as np
from keras.preprocessing import sequence
from keras.models import load_model
from keras.models import Sequential
from keras.layers import Dense, Embedding
from keras.layers import LSTM


class ChapterClassificator(object):
    def match(chapter_name):
        chap = None
        if ChapterClassificator.match_balance_sheet(chapter_name):
            chap = "bs"
        if ChapterClassificator.match_cash_flow(chapter_name):
            chap = "cf"
        if ChapterClassificator.match_statement_income(chapter_name):
            chap = "si"
            
        return chap
    
    def match_balance_sheet(chapter_name):
        words = ["balance","financial","condition"]
        return ChapterClassificator.match_words(words, chapter_name)
    
    def match_statement_income(chapter_name):
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
    
class LiabilititesClassificator(object):
    def __init__(self):
        with open("LbClf/liabilities_class_dict_v2018-05-24.csv") as f:
            self.tag_to_code = {l.split(";")[0]:l.replace("\n","").split(";")[1] for l in f}
        self.model = load_model("LbClf/liabilities_class_v2018-05-24.h5")
        self.bs_stat = {}
        with open("LbClf/bs_stat.csv") as f:
            for l in f:
                tag = l.split("\t")[0].split(":")[1]
                pref = l.split("\t")[0].split(":")[0]
                if pref == "us-gaap":
                    self.bs_stat[tag] = int(l.replace("\n","").split("\t")[1])
        
            
    def predict(self, tag):        
        if tag in self.bs_stat:
            return 0.0
        t = re.findall('[A-Z][^A-Z]*', tag)
        k= [self.tag_to_code.get(j,1) for j in t]
        to_predict = np.zeros((1,len(k)))
        to_predict[0]=k
        
        maxlen = 7  # обучал для последовательности из 7 кусков
        to_predict = sequence.pad_sequences(to_predict, maxlen=maxlen)# заполняем если короче

        predicted = self.model.predict(to_predict)
        
        return predicted
    
class LiabilitiesClassificator2(object):
    def __init(self):
        with open("LbClf/liabilities_class_dict_v2018-08-17.csv") as f:
            self.tag_to_code = {l.split(";")[0]:l.replace("\n","").split(";")[1] for l in f}
        self.model = load_model("LbClf/liabilities_class_v2018-08-17.h5")
    
    def predict(self, parent, child):
        t = re.findall('[A-Z][^A-Z]*', parent)
        k_parent= [self.tag_to_code.get(j,1) for j in t] 
        
        t = re.findall('[A-Z][^A-Z]*', child)
        k_child= [self.tag_to_code.get(j,1) for j in t]
        
        maxlen = 12 #word length len(parent) + len(child) 
        max_features = 1314 #dict length
        
        to_predict_parent = np.zeros((1, len(k_parent)))
        to_predict_parent[0]= k_parent
        
        to_predict_child = np.zeros((1, len(k_child)))
        to_predict_child[0]= k_child
        to_predict  = concatenate_arrays( to_predict_parent, to_predict_child,maxlen)
        
        to_predict = sequence.pad_sequences(to_predict, maxlen=maxlen)
        predicted = self.model.predict(to_predict)
        
        return predicted