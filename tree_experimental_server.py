# -*- coding: utf-8 -*-
"""
Created on Sat Apr  7 20:25:17 2018

@author: Stanislav
"""

#import pymssql
import numpy as np
#from random import shuffle
import json
import os
import datetime as dt

from keras.preprocessing import sequence
from keras.layers import Dense, Embedding, Dropout, Activation, Input
from keras.layers import LSTM,Flatten,Reshape,RepeatVector,TimeDistributed,Bidirectional
from keras.models import load_model
from keras.utils import to_categorical
from keras.models import Model
from keras.utils import plot_model
from keras import backend as K
import keras

accuracy = 0.0099

def hamming_distance(y_true, y_pred):
    T1 = K.abs(y_true - y_pred)-accuracy/2
    T2 = K.clip(T1, 0, 5)
    return(K.mean(T2))

def mixed(y_true, y_pred):
    a = hamming_distance(y_true, y_pred)
    b = keras.metrics.mean_squared_error(y_true, y_pred)

    return a*0.5 + b*0.5

def hamming_accuracy(y_true, y_pred):
    from keras import backend as K
    T1 = K.abs(y_true - y_pred)
    T1_a=K.less(T1,accuracy/2)
    T2 = K.cast(T1_a,'float32')
    T3 = K.mean(T2)
    return T3

def load_train_test(batch_size, train_name, test_name):
    encoder_train_array2d = np.loadtxt(train_name, dtype=float)
    encoder_test_array2d = np.loadtxt(test_name, dtype=float)

    train_x = int(encoder_train_array2d.shape[0]/batch_size)*batch_size
    test_x = int(encoder_test_array2d.shape[0]/batch_size)*batch_size
    encoder_train_array2d = encoder_train_array2d[0:train_x,]
    encoder_test_array2d = encoder_test_array2d[0:test_x,]

    return encoder_train_array2d, encoder_test_array2d

def model_with_dense(max_len, batch_size):
    array_reduced = Input(batch_shape=(batch_size,max_len,))
    array_expanded = Reshape(target_shape=(max_len,1), input_shape=(max_len,)) (array_reduced)
    #lstm1 = Bidirectional(LSTM(128, dropout=0.2, recurrent_dropout=0.2, return_sequences=True,stateful=False)) (array_expanded)
    lstm1 = Bidirectional(LSTM(256, dropout=0.2, recurrent_dropout=0.2, return_sequences=False, stateful=False)) (array_expanded)

    #lstm1, forward_h, forward_c, backward_h, backward_c = Bidirectional(LSTM(128, dropout=0.2, recurrent_dropout=0.2, return_sequences=True,stateful=True)) (array_expanded)
    #lstm2 = LSTM(256,  dropout=0.2, recurrent_dropout=0.2, stateful=False, return_sequences=False) (lstm1)

    #dense1 = Dense(128) (lstm2)
    encoder = (Dense(1)) (lstm1)
    #encoder = Reshape(target_shape=(max_len,), input_shape=(max_len,1)) (encoder)
    #encoder = lstm2

    decoder1 = RepeatVector(max_len)(encoder)
    decoder1 = LSTM(256, dropout=0.2, recurrent_dropout=0.2, return_sequences=True) (decoder1)
    decoder1 = TimeDistributed(Dense(1))(decoder1)
    #decoder1 = Flatten()(decoder1)
    decoder1 = Reshape(target_shape=(max_len,), input_shape=(max_len,1)) (decoder1)

    model = Model(inputs=array_reduced, outputs=decoder1)

    model.compile(optimizer='adam', loss=mixed, metrics=[hamming_distance, hamming_accuracy, 'mse'])

    return model

def model_cross_entropy(max_len, batch_size, max_features):

    # crossentropy shit speed learned by hot
    array_reduced = Input(batch_shape=(batch_size,max_len,1))
    lstm1 = Bidirectional(LSTM(512, dropout=0.1, recurrent_dropout=0.1, return_sequences=False,stateful=False)) (array_reduced)

    encoder = lstm1

    decoder1 = RepeatVector(max_len)(encoder)
    decoder1 = LSTM(256, dropout=0.1, recurrent_dropout=0.1, return_sequences=True) (decoder1)
    decoder1 = TimeDistributed(Dense(max_features, activation = 'softmax'))(decoder1)

    decoder2 = RepeatVector(max_len-1)(encoder)
    decoder2 = LSTM(256, dropout=0.1, recurrent_dropout=0.1, return_sequences=True) (decoder2)
    decoder2 = TimeDistributed(Dense(max_features, activation = 'softmax'))(decoder2)


    model = Model(inputs=array_reduced, outputs=[decoder1,decoder2])
    model.compile(optimizer='adam', loss='sparse_categorical_crossentropy',
                  metrics=['mse','sparse_categorical_crossentropy',hamming_accuracy])

    return model

def model_2_outputs(max_len, batch_size):

    #Model with 2 outputs learned by standard files
    array_reduced = Input(batch_shape=(batch_size,max_len,))
    array_expanded = Reshape(target_shape=(max_len,1), input_shape=(max_len,)) (array_reduced)
    lstm1 = Bidirectional(LSTM(256, dropout=0.2, recurrent_dropout=0.2,
                               return_sequences=False,stateful=False)) (array_expanded)

    encoder = lstm1

    decoder1 = RepeatVector(max_len)(encoder)
    decoder1 = LSTM(256, dropout=0.1, recurrent_dropout=0.1, return_sequences=True) (decoder1)
    decoder1 = TimeDistributed(Dense(1))(decoder1)
    decoder1 = Reshape(target_shape=(max_len,), input_shape=(max_len,1)) (decoder1)

    decoder2 = RepeatVector(max_len-1)(encoder)
    decoder2 = LSTM(256, dropout=0.1, recurrent_dropout=0.1, return_sequences=True) (decoder2)
    decoder2 = TimeDistributed(Dense(1))(decoder2)
    decoder2 = Reshape(target_shape=(max_len-1,), input_shape=(max_len-1,1)) (decoder2)

    model = Model(inputs=array_reduced, outputs=[decoder1,decoder2])
    model.compile(optimizer='adam', loss='mse', metrics=['acc',hamming_accuracy, hamming_distance])

    return model

def train(encoder_train_array2d, encoder_test_array2d,
          batch_size = 256, epochs = 5,
          max_len = 32, max_features = 1314):

    print('Build model...')
    model = model_with_dense(max_len, batch_size)

    print('Train...')
    history = model.fit(encoder_train_array2d, encoder_train_array2d,
                        batch_size=batch_size, epochs=epochs, verbose=1,
                        validation_data=(encoder_test_array2d, encoder_test_array2d))
    score = model.evaluate(encoder_test_array2d, encoder_test_array2d,
                                batch_size=batch_size)

    return history, score, model

def train_2_outputs(encoder_train_array2d, encoder_test_array2d,
          batch_size = 256, epochs = 5,
          max_len = 32, max_features = 1314):

    encoder_train_array2d_shorted = encoder_train_array2d[:,1:]
    encoder_test_array2d_shorted = encoder_test_array2d[:,1:]

    print('Build model...')
    model = model_2_outputs(max_len, batch_size)

    print('Train...')
    history=model.fit(encoder_train_array2d,
                      [encoder_train_array2d,encoder_train_array2d_shorted],
                      epochs=epochs, verbose=1, batch_size=batch_size,
                      validation_data=(encoder_test_array2d, [encoder_test_array2d,encoder_test_array2d_shorted]))
    score = model.evaluate(encoder_test_array2d, [encoder_test_array2d,encoder_test_array2d_shorted],
                            batch_size=batch_size)

    return history, score, model

def train_cross_entropy(encoder_train_array2d, encoder_test_array2d,
          batch_size = 256, epochs = 5,
          max_len = 32, max_features = 1314):

    print('Build model...')
    model = model_cross_entropy(max_len, batch_size, max_features)

    encoder_train_array2d_shorted = encoder_train_array2d[:,1:]
    encoder_test_array2d_shorted = encoder_test_array2d[:,1:]

    print('Train...')
    history=model.fit(encoder_train_array2d.reshape(-1,encoder_train_array2d.shape[1],1),
                      [encoder_train_array2d.reshape(*encoder_train_array2d.shape,1),
                       encoder_train_array2d_shorted.reshape(*encoder_train_array2d_shorted.shape,1)],
                       epochs=epochs, verbose=1,
                       validation_data=(encoder_test_array2d.reshape(-1,encoder_test_array2d.shape[1],1),
                                        [encoder_test_array2d.reshape(*encoder_test_array2d.shape,1),
                                         encoder_test_array2d_shorted.reshape(*encoder_test_array2d_shorted.shape,1)]),
                       batch_size = 64
                      )
    score = model.evaluate(encoder_test_array2d, [encoder_test_array2d,encoder_test_array2d_shorted],
                            batch_size=batch_size)

    return history, score, model

def save_results(history, score, model):
    print('Score:', score)
    print('Save results...')
    history_dict = history.history

    json.dump(history_dict, open('history_dump_{0}.json'.format(dt.datetime.now()), 'w'))
    model.save('tags_autoencoder_{0}.h5'.format(dt.datetime.now()))

def launch():
    batch_size = 64

    print('Load train and test set...')
    train_set, test_set = load_train_test(batch_size, 'rawdata/train_array2d_hot.txt', 'rawdata/test_array2d_hot.txt')
    print(train_set.shape, test_set.shape)
    #history, score, model = train(train_set, test_set, batch_size=batch_size, epochs=5)
    #history, score, model = train_2_outputs(train_set, test_set, batch_size=batch_size, epochs=300)
    history, score, model = train_cross_entropy(train_set, test_set, batch_size=batch_size, epochs=5)

    save_results(history, score, model)

launch()