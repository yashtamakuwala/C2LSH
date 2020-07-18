from pyspark import SparkContext, SparkConf
from time import time
import pickle
import submission


def createSC():
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("C2LSH")
    sc = SparkContext(conf=conf)
    return sc


with open("toy/toy_hashed_data", "rb") as file:
    data = pickle.load(file)

with open("toy/toy_hashed_query", "rb") as file:
    query_hashes = pickle.load(file)

import random


def generate(dimension, count, seed, start=-1000, end=1000):
    random.seed(seed)

    data = [
        [
            random.randint(start, end)
            for _ in range(dimension)
        ]
        for i in range(count)
    ]

    query = [random.randint(start, end) for _ in range(dimension)]

    return data, query


def generate2(dimension, count, seed, start=0, end=100):
    data = [
        [n] * dimension
        for n in range(start, end)
        for i in range(count)
    ]

    query = [seed] * dimension

    return data, query


def evaluation(dimension, count, seed, start=0, end=100):
    data = [
        [k + j for j in range(dimension)]
        for k in range(start, end)
        for i in range(count)
    ]

    query = [seed] * dimension

    return data, query


alpha_m, beta_n = 11, 50
data, query5 = evaluation(14, 19, 100, 0, 110)
query_hashes = query5

# alpha_m, beta_n = 10, 10
# data, query2 = generate(10, 20000, 0, 0, 1000)
# query_hashes = query2

# alpha_m, beta_n = 10, 50
# data, query3 = generate(13, 200, 100, -50000, 50000)
# query_hashes = query3

# alpha_m, beta_n = 10, 49
# data, query4 = generate2(13, 9, 100, 0, 120)
# query_hashes = query4

# alpha_m, beta_n = 10, 500
# data, query6 = generate( 13, 2_000, 100, -230_000, 50_000)
# query_hashes = query6

sc = createSC()
data_hashes = sc.parallelize([(index, x) for index, x in enumerate(data)])
start_time = time()
res = submission.c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
end_time = time()
print('Time:', end_time - start_time)
sc.stop()

# print('running time:', end_time - start_time)
print('Number of candidate: ', len(res))
print('set of candidate: ', set(res))
