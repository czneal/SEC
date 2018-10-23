# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 13:58:54 2018

@author: Asus
"""

from sympy.combinatorics.free_groups import free_group, vfree_group, xfree_group
from sympy.combinatorics.fp_groups import FpGroup
import itertools as it
import timeit
import sympy as sp
import time
import pandas as pd

def Gn(n):
    params = ''
    for i in range(0, n):
        params += 'a' + str(i) + ','
    params = params[0:-1]
    
    t = free_group(params)
    F = t[0]
    a = t[1:]
    
    relations = []
    for i in range(0,n-1):
        relations.append(a[i]*a[i+1]*a[i]**(-1)*a[i+1]**(-1))
    relations.append(a[n-1]*a[0]*a[n-1]**(-1)*a[0]**(-1))
    
    G = FpGroup(F, relations)
    return G, a

def rho(word, G):
    #n even reflection no fixed points
    a = G.generators
    n = len(a)
    arr_form = list(word.array_form)
    for i, (letter, pw) in enumerate(arr_form):
        index = int(str(letter)[1:])
        arr_form[i] = (a[n - index - 1], pw)
    
    val = G.identity
    for letter, pw in arr_form:
        val = val*pow(letter, pw)
    
    return val

def rho1(word, G):
    #n even reflection 2 fixed points
    #n odd reflection 1 fixed point
    a = G.generators
    n = len(a)
    arr_form = list(word.array_form)
    for i, (letter, pw) in enumerate(arr_form):
        index = int(str(letter)[1:])
        arr_form[i] = (a[(n - index)%n], pw)
    val = G.identity
    for letter, pw in arr_form:
        val = val*pow(letter, pw)
    
    return val

def sigma(word, r, G):
    #rotation on r
    a = G.generators
    n = len(a)
    arr_form = list(word.array_form)
    for i, (letter, pw) in enumerate(arr_form):
        index = int(str(letter)[1:])
        arr_form[i] = (a[(index + r)%n], pw)
    
    val = G.identity
    for letter, pw in arr_form:
        val = val*pow(letter, pw)
        
    return val

def generate(length, G):
    b = []
    b.extend(G.generators)
    b.extend([e**-1 for e in G.generators])
    
    for current_word in it.product(b, repeat=length):
        w = G.identity
        for e in current_word: w = w*e
#        w = G.reduce(w)
        if len(w.letter_form) < length:
            continue
        
        yield w        
    return

def generate_custom(length, n):
    b = [(e,1) for e in range(n)]
    b.extend([(e,-1) for e in range(n)])
    for data in it.product(b, repeat=length):
        w = Word()
        w.symbols = data
        w = w.reduce()
        if w.length() < length:
            continue
        if w.totpow()<0:
            yield w.inverse().order()
        else:
            yield w
        
class FpGroupCycle(object):
    def __init__(self, n):
        self.cycle = n
    
class Word(object):
    def __init__(self, s_word=None):
        self.letter = 'a'
        self.symbols = []
        if s_word is not None:
            self.read(s_word)
        
    def __str__(self):
        s = ''
        for e in self.symbols:
            if e[1] == 1:
                s += self.letter + '{0}*'.format(e[0])
            else:
                s += self.letter + '{0}**{1}*'.format(e[0],e[1])
        return s[:-1]
    
    def read(self, s_word):
        s = s_word.replace('**','^')
        s = s.split('*')
        self.symbols = []
        
        for e in s:
            e = e.split('^')
            if len(e)==2:
                self.symbols.append((int(e[0][1:]), int(e[1])))
            else:
                self.symbols.append((int(e[0][1:]), 1))
        return
    
    def inverse(self):
        w = Word()
        w.symbols = [(e[0],-e[1]) for e in reversed(self.symbols)]
        return w
    
    def simplify(self):               
        simple = [a for a in self.symbols]
        
        dosmt = True
        while dosmt:
            simpler = []
            if len(simple) == 0:
                break
        
            dosmt = False
            cur_symb = list(simple[0])
            for i in range(1, len(simple)):
                if simple[i][0] == cur_symb[0]:
                    dosmt = True
                    cur_symb[1] += simple[i][1]
                else:
                    if cur_symb[1] != 0:
                        simpler.append((cur_symb[0], cur_symb[1]))
                    cur_symb = list(simple[i])
            
            if cur_symb[1] != 0: simpler.append(cur_symb)
            
            simple = simpler
        
        w = Word()
        w.symbols = simple
        return w
    
    def order(self, cycle):
        new_order = [e for e in self.symbols]
        for j in range(1, len(new_order)):
            for i in range(0, len(new_order)-j):
                if (new_order[i][0] - new_order[i+1][0] == 1 or
                   new_order[i][0] - new_order[i+1][0] == (cycle-1)):
                    new_order[i], new_order[i+1] = new_order[i+1], new_order[i]
        
        w = Word()
        w.symbols = new_order
        
        return w
        
    def mul(self, right):
        w = Word()
        w.symbols.extend(self.symbols)
        w.symbols.extend(right.symbols)
        return w
    
    def reduce(self, cycle):
        w = self.simplify()
        old_str = str(w)
        new_str = ''
        while old_str != new_str:
            old_str = str(w)
            w = w.order(cycle)
            w = w.simplify()
            new_str = str(w)        
        return w
    
    def totpow(self):
        return sum([pw for (e,pw) in self.symbols])
    
    def length(self):
        return sum([abs(pw) for (e,pw) in self.symbols])
    
    def isidentity(self):
        return (len(self.symbols) == 0)
    
def dictionary(length, G):
    data = []
    for index, word in enumerate(generate(length, G)):
        print('\r{0}'.format(index), end='')
        data.append([word])    
    print()
    
    df = pd.DataFrame(data, columns=['form'])
    df['form'] = df['form'].apply( lambda x: str(G.reduce(x)) )
    df.set_index('form', inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    
    df.to_csv('outputs/w{0}.csv'.format(length))
    
    return df

def rho_new(w, cycle):
    new = Word()
    new.symbols = []
    for (e, pw) in w.symbols:
        new.symbols.append((cycle - e -1, pw))
    return new

def dictionary_new(length, cycle):
    data = []
    for index, w in enumerate(generate_custom(length, cycle)):
        if index%100 == 0:
            print('\rw:{0}'.format(index), end='')
        data.append(str(w))
    print()
        
    df = pd.DataFrame(data, columns=['form']).set_index('form')
    df = df[~df.index.duplicated(keep='first')]
    df.to_csv('outputs/wn{0}.csv'.format(length))
    
    return df

def read_dictionary(length):
    frames = []
    for i in range(1, length+1):
        frames.append(pd.read_csv('outputs/wn{0}.csv'.format(i)))
    df = pd.concat(frames)
    df['word'] = df['form'].apply(lambda x: Word(x))
    return df

#w = Word('a0*a2*a3**3')
#v = rho_new(w, 6)
cycle = 6
length = 6
#df = read_dictionary(length)
data = []
for g_index, g_row in enumerate(df.iterrows()):
    g = g_row[1]['word']    
    for w_index, w_row in enumerate(df.iterrows()):
        w = w_row[1]['word']
        rho_w = rho_new(w, cycle)
        res = rho_w.inverse().mul(g.inverse()).mul(w).mul(g)
        res = res.reduce(cycle)
        if res.isidentity():
            data.append([str(w), str(g)])
            print()
            print(w, g)
            
        if w_index%1000: 
            print('\rg_index:{0} w_index:{1} w:{2} g:{3}               '.format(w_index, g_index, w, g), end='')
        





