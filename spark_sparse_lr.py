# By Jinliang Wei (jinlianw@cs.cmu.edu)
# Copyright (c) 2017 Carnegie Mellon University
# For use in 15-719 only
# All other rights reserved.

# This program trains a logistic regression model using the gradient descent
# algorithm.
# The program takes in the following command line arguments, which are all
# required:
# 1) data_path: the HDFS path where the training data set is stored
# 2) num_features: the total number of feature dimensions of the training data
# 3) num_iterations: the number of training iterations
# 4) step_size: the gradient descent step size
# 5) loss_file: a local path to store the cross-entropy loss for each iteration
#
# For example, run this program with:
# <path-to-spark-submit> spark_sparse_lr.py /kdda 20216830 10 2e-6 loss_kdda

import pyspark
import sys
import numpy as np
import math
from datetime import datetime
import scipy.sparse as sps
import string
from collections import defaultdict

# parse a line of the training data file to produce a data sample record
def parse_line(line):
    parts = line.split()
    label = int(parts[0])
    # the program requires binary labels in {0, 1}
    # the dataset may have binary labels -1 and 1, we convert all -1 to 0
    label = 0 if label == -1 else label
    feature_ids = []
    feature_vals = []
    for part in parts[1:]:
        feature = part.split(":")
        # the datasets have feature ids in [1, N] we convert them
        # to [0, N - 1] for array indexing
        feature_ids.append(int(feature[0]) -  1)
        feature_vals.append(float(feature[1]))
    return (label, (np.array(feature_ids), np.array(feature_vals)))

def sigmoid(x):
    return 1 / (1 + math.exp(-x))

# compute logarithm of a number but thresholding the number to avoid logarithm of
# 0
def safe_log(x):
    if x < 1e-15:
        x = 1e-15
    return math.log(x)

# compute the gradient descent updates and cross-entropy loss for an RDD partition
def gd_partition(sample):
    # print "in"
    # print sample
    local_updates = defaultdict(float)
    # local_weights_array = weights_array_bc.value
    cross_entropy_loss = 0

    # compute and accumulate updates for each data sample in the partition
    label = sample[1][0][0]

    features = sample[1][0][1]

    feature_ids = features[0]
    feature_vals = features[1]

    weights_array = sample[1][1]

    local_weights = []

    for idx in feature_ids:
        local_weights.append(weights_array[idx])

    pred = sigmoid(feature_vals.dot(local_weights))
    diff = label - pred

    local_weights = np.array(local_weights)
    feature_vals = np.array(feature_vals)

    # the L2-regularlized gradients
    gradient = diff * feature_vals + reg_param * local_weights

    sample_update = step_size * gradient
    # print sample_update

    for i in range(0, feature_ids.size):
        local_updates[feature_ids[i]] += sample_update[i]
        if label == 1:
            cross_entropy_loss -= safe_log(pred)
        else:
            cross_entropy_loss -= safe_log(1 - pred)


    # print cross_entropy_loss
    # print local_updates
    accumulated_updates = sps.csr_matrix( \
        (local_updates.values(), \
         local_updates.keys(), \
         [0, len(local_updates)]), \
        shape=(1, num_features))
    return [(cross_entropy_loss, accumulated_updates)]


    # for sample in samples:
    #     # label = sample[0]
    #     # features = sample[1][1]
    #     # feature_ids = features[0]
    #     # feature_vals = features[1]
    #
    #     # fetch the relevant weights for this sample as a numpy array
    #     local_weights = np.take(local_weights_array, feature_ids)
    #     # given the current weights, the probability of this sample belonging to
    #     # class '1'
    #     pred = sigmoid(feature_vals.dot(local_weights))
    #     diff = label - pred
    #     # the L2-regularlized gradients
    #     gradient = diff * feature_vals + reg_param * local_weights
    #     sample_update = step_size * gradient
    #
    #     for i in range(0, feature_ids.size):
    #         local_updates[feature_ids[i]] += sample_update[i]
    #
    #     # compute the cross-entropy loss, which is an indirect measure of the
    #     # objective function that the gradient descent algorithm optimizes for
    #     if label == 1:
    #         cross_entropy_loss -= safe_log(pred)
    #     else:
    #         cross_entropy_loss -= safe_log(1 - pred)
    # accumulated_updates = sps.csr_matrix(\
    #                                      (local_updates.values(), \
    #                                       local_updates.keys(), \
    #                                       [0, len(local_updates)]), \
    #                                      shape=(1, num_features))
    # return [(cross_entropy_loss, accumulated_updates)]

def func(index, iterator):
    return [(index, value) for value in iterator]

def add_weight(sample):
    dict = {}
    for x in sample[1]:
        dict[x] = weight_init_value

    return (sample[0], dict)



if __name__ == "__main__":
    data_path = sys.argv[1]
    num_features = int(sys.argv[2])
    num_iterations = int(sys.argv[3])
    step_size = float(sys.argv[4])
    loss_file = sys.argv[5]

    # for all test cases, your weights should all be initialized to this value
    weight_init_value = 0.001
    # the step size is multiplicatively decreased each iteration at this rate
    step_size_decay = 0.95
    # the L2 regularization parameter
    reg_param = 0.01

    # total number of cores of your Spark slaves
    num_cores = 1
    # for simplicity, the number of partitions is hardcoded
    # the number of partitions should be configured based on data size
    # and number of cores in your cluster
    num_partitions = num_cores * 5
    conf = pyspark.SparkConf().setAppName("SparseLogisticRegressionGD")
    sc = pyspark.SparkContext(conf=conf)

    text_rdd = sc.textFile(data_path, minPartitions=num_partitions)
    # the RDD that contains parsed data samples, which are reused during training
    samples_rdd = text_rdd.map(parse_line, preservesPartitioning=True)\
                 .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)
    num_samples = samples_rdd.count()

    global_fweights = sc.parallelize((i, weight_init_value) for i in range(9))
    #######################

    parId_samples_rdd = samples_rdd.mapPartitionsWithIndex(func)
    parId_fids_list = parId_samples_rdd.map(lambda x: (x[0], x[1][1][0])).reduceByKey(
        lambda x, y: np.unique(np.append(x, y)))


    # print parId_samples_rdd.collect()

    # force samples_rdd to be created
    # initialize weights as a local array
    weights_array_rdd = sc
    weights_array = np.ones(num_features) * weight_init_value


    loss_list = []
    for iteration in range(0, num_iterations):

        par_fid_wei = parId_fids_list.flatMap(lambda x: [(idx, x[0]) for idx in x[1]]).join(global_fweights).map(
            lambda x: (x[1][0], (x[0], x[1][1]))).groupByKey().map(lambda x: (x[0], dict(x[1])))

        joined = parId_samples_rdd.join(par_fid_wei)

        loss_updates_rdd = joined.map(gd_partition).collect()

        loss_sum = loss_updates_rdd.map(lambda x: x[0]).reduce(lambda x, y: x + y)

        # write the cross-entropy loss to a local file
        with open(loss_file, "w") as loss_fobj:
                loss_fobj.write(str(loss_sum) + "\n")
        loss_fobj.close()

        feature_updates = loss_updates_rdd.map(lambda x: x[1].items()).reduceByKey(lambda x, y: x + y)
        global_fweights = global_fweights.join(feature_updates).reduceByKey(lambda x, y: x + y)

        # decay step size to ensure convergence
        step_size *= 0.95
        print "iteration: %d, cross-entropy loss: %f" % (iteration, loss_sum)

    # write the cross-entropy loss to a local file
    with open(loss_file, "w") as loss_fobj:
        for loss in loss_list:
            loss_fobj.write(str(loss) + "\n")
    print loss_list
