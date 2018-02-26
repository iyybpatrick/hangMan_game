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
def gd_partition(samples):
    # (parId, ({(fid, weight)}, [(label, ([fids], [vals])])))
    local_updates = defaultdict(float)
    cross_entropy_loss = 0
    # compute and accumulate updates for each data sample in the partition
    for sample in samples[1][1]:

        label = sample[0]
        features = sample[1]
        feature_ids = features[0]
        feature_vals = features[1]
        local_weights = np.array([])
        # local_weights.append()
        for fid in feature_ids:
            local_weights = np.append(local_weights, samples[1][0][fid])

        # given the current weights, the probability of this sample belonging to
        # class '1'
        pred = sigmoid(feature_vals.dot(local_weights))
        diff = label - pred

        # the L2-regularlized gradients
        gradient = diff * feature_vals + reg_param * local_weights
        sample_update = step_size * gradient

        for i in range(0, feature_ids.size):
            local_updates[feature_ids[i]] += sample_update[i]

        # compute the cross-entropy loss, which is an indirect measure of the
        # objective function that the gradient descent algorithm optimizes for
        if label == 1:
            cross_entropy_loss -= safe_log(pred)
        else:
            cross_entropy_loss -= safe_log(1 - pred)

    return (cross_entropy_loss, local_updates.items())

def get_par_sample(index, iterator):
        return [(index, [sample for sample in iterator])]

def add_weight(sample):
    dict = {}
    for x in sample[1]:
        dict[x] = weight_init_value

    return (sample[0], dict)

# ([fids], parId)
def get_fid_pid(samples):
    res_set = set()
    for sample in samples:
        for ele in sample[0]:
            res_set.add((ele, sample[1]))
    return res_set

def global_feature_weight(parId, unit_num, last_unit_num):
    list = []
    if parId != num_partitions - 1:
        for i in range(unit_num):
            list.append((parId * unit_num + i, weight_init_value))
    else:
        for i in range(last_unit_num):
            list.append((parId * unit_num + i, weight_init_value))
    return list

def get_loss_updates(sample):
    loss_acc.add(sample[0])
    return sample[1]

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
    num_cores = 64
    # for simplicity, the number of partitions is hardcoded
    # the number of partitions should be configured based on data size
    # and number of cores in your cluster
    num_partitions = num_cores * 3
    unit_num = int(num_features / num_partitions)
    last_unit_num = num_features - unit_num * num_partitions + unit_num
    conf = pyspark.SparkConf().setAppName("SparseLogisticRegressionGD")
    sc = pyspark.SparkContext(conf=conf)

    text_rdd = sc.textFile(data_path, minPartitions=num_partitions)
    # the RDD that contains parsed data samples, which are reused during training
    samples_rdd = text_rdd.map(parse_line)

    # (parId, [(label, ([fids],[vals])]))
    pid_label_fids_vals = samples_rdd.mapPartitionsWithIndex(get_par_sample, preservesPartitioning=True)\
				 .partitionBy(numPartitions=num_partitions)\
                 .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)
    # [(fid, weight)]
    global_fweights = pid_label_fids_vals.flatMap(lambda x : (global_feature_weight(x[0], unit_num, last_unit_num)))\
                                         .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)

    # ([fids], parId)
    parId_samples_rdd = pid_label_fids_vals.map(lambda x: [(v[1][0], x[0]) for v in x[1]], preservesPartitioning=True)\
                                           .flatMap(get_fid_pid, preservesPartitioning=True)\
                                           .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)

    # num_samples = pid_label_fids_vals.count()
    loss_fobj = open(loss_file, 'a+')

    for iteration in range(0, num_iterations):
        # compute gradient descent updates in parallel

        # (parId, [(label, ([fids],[vals])]))
        pid_fid_wht = parId_samples_rdd.join(global_fweights)\
            .map(lambda x: (x[1][0], (x[0], x[1][1])), preservesPartitioning=True)\
            .groupByKey(numPartitions=num_partitions, preservesPartitioning=True)\
            .map(lambda x: (x[0], dict(x[1])), preservesPartitioning=True)
        #pid_fid_wht
        joined = pid_fid_wht.join(pid_label_fids_vals, numPartitions=num_partitions)

        loss_updates_rdd = joined.map(gd_partition).persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)

        loss_acc = sc.accumulator(0)

        updates_rdd = loss_updates_rdd.flatMap(get_loss_updates)\
                                      .reduceByKey(lambda x,y : x + y, numPartitions=num_partitions)

        new_global_fweights = global_fweights.join(updates_rdd, numPartitions=num_partitions)\
                                         .map(lambda x : (x[0],x[1][0] + x[1][1]))\
                                         .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)

        new_global_fweights.count()
        loss_updates_rdd.unpersist()
        global_fweights.unpersist()
        global_fweights = new_global_fweights

        # with open(loss_file, "w") as loss_fobj:
        loss_fobj.write(str(loss_acc.value) + "\n")
        loss_fobj.flush()

        # decay step size to ensure convergence
        step_size *= 0.95
        print "iteration: %d, cross-entropy loss: %f" % (iteration, loss_acc.value)
