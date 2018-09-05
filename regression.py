# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 18:43:13 2017

@author: Asus
"""
import numpy as np
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score, median_absolute_error
from sklearn import preprocessing
from sklearn.model_selection import ShuffleSplit
from sklearn.feature_selection import VarianceThreshold
import sklearn.cluster
import sklearn.svm

def clusterization():
    data = np.load("LinearRegression/lc_yx.npy")
    data = data[np.where(data[:,0] != 0)]
    X = data[:,1:]
    
    vt = VarianceThreshold(threshold=0)
    vt.fit(X)
    X_var = vt.transform(X)
    w = np.where(X_var==0)
    print(X.shape, X_var.shape)
    
    sc = preprocessing.StandardScaler()
    sc.fit(X_var)
    X_scale = sc.transform(X_var)
    X_scale[w] = 0
    
    km = sklearn.cluster.DBSCAN(eps=0.3)
    km.fit(X_scale)
    print(km.core_sample_indices_.shape)
    print(np.where(km.labels_==-1)[0].shape)
    
def linear_regression():
    data = np.load("LinearRegression/l_yx.npy")
    data = data[np.where(data[:,0] != 0)]
    Y = data[:,0]
    print(np.max(Y), np.min(Y), np.mean(Y))
    X = data[:,1:]
    
    vt = VarianceThreshold(threshold=0.0*(1.0-0.0))
    vt.fit(X)
    X_var = vt.transform(X)
    
    Y = Y.reshape((Y.shape[0],))
    print(X.shape, X_var.shape)
    
    (m,n) = X_var.shape
    w = np.where(X_var==0)
    
    sc = preprocessing.StandardScaler()
    sc.fit(X_var)
    X_scale =np.ones((m,n))
    X_scale = sc.transform(X_var)
    X_scale[w] = 0
    
    
    rs = ShuffleSplit(n_splits=3, test_size=.20, random_state=0)
    
    
    for i_train, i_test in rs.split(X_scale):
        X_train = X_scale[i_train,:]
        Y_train = Y[i_train]
        X_test = X_scale[i_test,:]
        Y_test = Y[i_test]
        
#        reg = linear_model.LinearRegression(fit_intercept=False)
#        reg.fit(X_train, Y_train)
#        Y_pred_train = reg.predict(X_train)
#        Y_pred_test = reg.predict(X_test)
        svr = sklearn.svm.NuSVR()
        svr.fit(X_train, Y_train)
        Y_pred_train = svr.predict(X_train)
        Y_pred_test = svr.predict(X_test)
        
        print("Train Mean squared error: %.2f"  % np.sqrt(mean_squared_error(Y_train, Y_pred_train)))
        print('Train Variance score: %.2f' % r2_score(Y_train, Y_pred_train))
        
        print("Test Mean squared error: %.2f"  % np.sqrt(mean_squared_error(Y_test, Y_pred_test)))
        print('Test Variance score: %.2f' % r2_score(Y_test, Y_pred_test))
        
        idx = np.argsort(Y_train,0)
        s1 = Y_train[idx]
        s2 = Y_pred_train[idx]
        with open("graph.txt", "w") as f:
            for i in range(s1.shape[0]):
                f.write(str(s1[i]) + "\t" + str(s2[i]) + "\n")
            
        
    
    
linear_regression()
#clusterization()