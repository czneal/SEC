# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 13:38:27 2018

@author: Asus
"""

import numpy as np
import json
from sklearn.cluster import KMeans
from sklearn import metrics
from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA

with open("pre_columns_10") as f:
    columns = json.loads(f.read())
    
with open("pre_rows_10") as f:
    rows = json.loads(f.read())
rows = np.array(rows)

matrix = np.load("pre_data_10.npy")
X = np.float32(np.abs(matrix)>0)

  
#k = KMeans(n_clusters=10).fit(X)
#labels = k.labels_
#
#n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
#
#print('Estimated number of clusters: %d' % n_clusters_)
#
#print("Silhouette Coefficient: %0.3f"
#      % metrics.silhouette_score(X, labels))
#
#for l in range(n_clusters_):
#    w = (labels==l)
#    cluster = X[w]
#    print(cluster.shape)
#    n_tags = np.sum(cluster, axis=1)
#    total_tags = np.sum(np.sum(cluster, axis=0)>1)
#    print("Label:{0}, size:{5}, total tags:{1}, mean:{2}, min:{3}, max:{4}".format(l,
#          total_tags, np.mean(n_tags), np.min(n_tags), 
#          np.max(n_tags), cluster.shape[0]))
    
#pca = PCA()
#pca.fit(X)
#print(pca.explained_variance_[:10])
#print(pca.explained_variance_ratio_[:10])

S = np.sum(X, axis=1)
w = np.where(S!=0)
S = np.float32(S[w])
X = X[w]
rows = rows[w]
nom = np.float32(np.dot(X, X.T))

denom = np.zeros(nom.shape, dtype=np.float32)
for i in range(nom.shape[0]):
    denom[:,i] = (S+S[i])/2
    
res = 1-nom/denom

db = DBSCAN(eps=0.4, min_samples=3, metric="precomputed").fit(res)
labels = db.labels_

# Number of clusters in labels, ignoring noise if present.
n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)

print('Estimated number of clusters: %d' % n_clusters_)
print(len(np.where(labels==-1)[0]))

for l in range(n_clusters_):
    w = (labels==l)
    cluster = X[w]
    print(cluster.shape)
    n_tags = np.sum(cluster, axis=1)
    total_tags = np.sum(np.sum(cluster, axis=0)>=1)
    intersection = np.sum(np.sum(cluster, axis=0)>=cluster.shape[0])
    print("Label:{0}, size:{5}, total tags:{1}, inter:{2}, min:{3}, max:{4}".format(l,
          total_tags, intersection, np.min(n_tags), 
          np.max(n_tags), cluster.shape[0]))
    
w = (labels==32)
adsh = rows[w]
for i in range(adsh.shape[0]):
    print("'{0}',".format(adsh[i]))
