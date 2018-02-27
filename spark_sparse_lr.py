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
def safe_log(x):
    if x < 1e-15:
        x = 1e-15
    return math.log(x)

    # (parId, ([(label, ([fids], [vals])], [(fid, weight)])))
def gd_partition(iterator):
    local_updates = {}
    cross_entropy_loss = 0

    for samples_list in iterator:
        samples = samples_list[1][0]
        fid_weht_dict = dict(samples_list[1][1])


        for sample in samples:
            label = sample[0]
            features = sample[1]
            feature_ids = features[0]
            feature_vals = features[1]

            # local_weights = np.array([])
            # for fid in feature_ids:
            #     local_weights = np.append(local_weights, fid_weht_dict[fid])

            local_weights = np.array([fid_weht_dict[feature_id]
                                      for feature_id in feature_ids])

            # given the current weights, the probability of this sample belonging to
            # class '1'
            pred = sigmoid(feature_vals.dot(local_weights))
            diff = label - pred

            # the L2-regularlized gradients
            gradient = diff * feature_vals + reg_param * local_weights
            sample_update = step_size * gradient

            for i in range(0, feature_ids.size):
                if feature_ids[i] in local_updates:
                    local_updates[feature_ids[i]] = (local_updates[feature_ids[i]][0]
                                                    + sample_update[i], local_updates[feature_ids[i]][1])
                else:
                    local_updates[feature_ids[i]] = (sample_update[i], fid_weht_dict[feature_ids[i]])

            # compute the cross-entropy loss, which is an indirect measure of the
            # objective function that the gradient descent algorithm optimizes for
            if label == 1:
                cross_entropy_loss -= safe_log(pred)
            else:
                cross_entropy_loss -= safe_log(1 - pred)
    return [(cross_entropy_loss, local_updates.items())]


def get_loss(samples):
    loss_acc.add(samples[0])
    return samples[1]
# bound together partition id with each partition
def get_par_sample(index, iterator):
        return [(index, [sample for sample in iterator])]

# (parId, [(label, ([fids],[vals])]))
def get_fid_pid(samples):
    parid = samples[0]
    res_set = set()
    for sample in samples[1]:
        fids = sample[1][0]
        for fid in fids:
            res_set.add((fid, parid))
    return res_set

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
    num_cores = 4
    # for simplicity, the number of partitions is hardcoded
    # the number of partitions should be configured based on data size
    # and number of cores in your cluster
    num_partitions = num_cores * 1

    conf = pyspark.SparkConf().setAppName("SparseLogisticRegressionGD")
    sc = pyspark.SparkContext(conf=conf)

    text_rdd = sc.textFile(data_path, minPartitions=num_partitions)
    # the RDD that contains parsed data samples, which are reused during training
    samples_rdd = text_rdd.map(parse_line)

    # (parId, [(label, ([fids],[vals])]))
    pid_label_fids_vals = samples_rdd.mapPartitionsWithIndex(get_par_sample, preservesPartitioning=True) \
                                     .partitionBy(numPartitions=num_partitions) \
                                     .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)

    # ((fids, [parId])
    parId_samples_rdd = pid_label_fids_vals.flatMap(get_fid_pid, preservesPartitioning=True)\
                                           .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)

    # print parId_samples_rdd.collect()
    global_fweights = parId_samples_rdd.map(lambda x:(x[0], weight_init_value), preservesPartitioning=True)\
                                       .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)

    loss_fobj = open(loss_file, 'a+')
    # print global_fweights.collect()
    for iteration in range(0, num_iterations):
        # compute gradient descent updates in parallel
        pid_fid_wht = parId_samples_rdd.join(global_fweights, numPartitions=num_partitions)\
                                       .map(lambda x: (x[1][0], (x[0], x[1][1]))) \
                                       .groupByKey(numPartitions=num_partitions)\



        # joined ; (parId, ([(fid, weight)], [(label, ([fids], [vals])])))
        joined = pid_label_fids_vals.join(pid_fid_wht, numPartitions=num_partitions)

        # print joined.collect()

        loss_acc = sc.accumulator(0)
        loss_updates_rdd = joined.mapPartitions(gd_partition, preservesPartitioning=True)\
                                 .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)
        # loss_updates_rdd.count()

        new_global_fweights = loss_updates_rdd.flatMap(get_loss, preservesPartitioning=True)\
                                              .reduceByKey(lambda x,y : (x[0]+y[0], x[1]), numPartitions=num_partitions)\
                                              .map(lambda x :(x[0],x[1][0] + x[1][1]))\
                                              .persist(pyspark.storagelevel.StorageLevel.MEMORY_AND_DISK)

        print new_global_fweights.collect()
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
