# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 12:56:33 2017

@author: Asus
"""
import numpy as np
import tensorflow as tf
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn import preprocessing

def func(X, W):    
    return np.dot(X,W)

def get_mini_batch(X,Y,i, batch_size):
    m = X.shape[0]
    
    start = i*batch_size
    fin = (i+1)*batch_size
    if fin>m:
        return X[start:,:], Y[start:,:]
    else:
        return X[start:fin,:], Y[start:fin,:]


np.random.seed(1)

n = 1000
m = 1500
l1_size = 1

X = np.random.rand(m+50,n)
X_test = X[m:,:]
X = X[:m,:]
W = np.random.rand(X.shape[1],1)*2.0-1.0
Y = func(X, W)
Y_test = func(X_test, W)

# Python optimisation variables
learning_rate = 0.005
epochs = 170
batch_size = 50

reg = linear_model.LinearRegression()
reg.fit(X, Y)
Y_pred_test = reg.predict(X_test)
print("Mean squared error: %.2f"  % mean_squared_error(Y_test, Y_pred_test))
print('Variance score: %.2f' % r2_score(Y_test, Y_pred_test))

# declare the training data placeholders
# input x - for 28 x 28 pixels = 784
x = tf.placeholder(tf.float32, [None, n])
# now declare the output data placeholder - 10 digits
y = tf.placeholder(tf.float32, [None, 1])

# now declare the weights connecting the input to the hidden layer
W1 = tf.Variable(tf.random_normal([n, l1_size], stddev=0.03), name='W1')
b1 = tf.Variable(tf.random_normal([l1_size]), name='b1')
# and the weights connecting the hidden layer to the output layer
#W2 = tf.Variable(tf.random_normal([l1_size, 1], stddev=0.03), name='W2')
#b2 = tf.Variable(tf.random_normal([1]), name='b2')

# calculate the output of the hidden layer
hidden_out = tf.add(tf.matmul(x, W1), b1)
#hidden_out = tf.nn.relu(hidden_out)

# now calculate the hidden layer output - in this case, let's use a softmax activated
# output layer
#y_ = tf.add(tf.matmul(hidden_out, W2), b2)
#y_ = tf.reduce_sum(hidden_out, axis=1, keep_dims=True)
y_ = hidden_out

cost = tf.losses.mean_squared_error(y, y_)
#cross_entropy = -tf.reduce_mean(tf.reduce_sum(y * tf.log(y_clipped)
#                         + (1 - y) * tf.log(1 - y_clipped), axis=1))

# add an optimiser
optimiser = tf.train.AdamOptimizer(learning_rate=learning_rate).minimize(cost)

# finally setup the initialisation operator
init_op = tf.global_variables_initializer()

# define an accuracy assessment operation
#correct_prediction = tf.equal(tf.argmax(y, 1), tf.argmax(y_, 1))
#accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

# start the session
with tf.Session() as sess:
    # initialise the variables
    sess.run(init_op)
    total_batch = int(m / batch_size) + 1
    for epoch in range(epochs):
        avg_cost = 0
        for i in range(total_batch):
            batch_x, batch_y = get_mini_batch(X, Y, epoch, batch_size)
            c = sess.run([optimiser, cost], feed_dict={x: batch_x, y: batch_y})
            avg_cost += c[1] / total_batch
        print("Epoch:", (epoch + 1), "cost =", "{:.3f}".format(avg_cost))
    print(sess.run(cost, feed_dict={x: X_test, y: Y_test}))