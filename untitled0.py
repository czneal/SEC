# -*- coding: utf-8 -*-
"""
Created on Sun Oct 21 13:58:54 2018

@author: Asus
"""

from sympy.combinatorics.free_groups import free_group, vfree_group, xfree_group
from sympy.combinatorics.fp_groups import FpGroup
import itertools as it

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

def generate(length, G):
    b = []
    b.extend(G.generators)
    b.extend([e**-1 for e in G.generators])
    
    for current_word in it.product(b, repeat=length):
        w = G.identity
        for e in current_word: w = w*e
        w = G.reduce(w)
        if len(w.letter_form) < length:
            continue
        
        yield w        
    return

G, a = Gn(6)

rho(a[1]*a[2]**-2, G)

w_length = 6
g_length = 6
data = []
for i, w in enumerate(generate(w_length, G)):
    rho_w = rho(w, G)
    for length in range(1, g_length+1):
        for j, g in enumerate(generate(length, G)):
            print('\r{0}-{1}-g_length:{2}                '.format(i,j,length), end="")
            if G.equals(rho_w, G.reduce(g**-1*w*g)):
                data.append([w, g])
                print(w, g)

import pandas as pd
df = pd.DataFrame(data, columns=['w','g'])
df.to_csv('outputs/wg6.csv')
