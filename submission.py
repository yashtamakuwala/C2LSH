## import modules here
from pyspark import SparkContext, SparkConf
from time import time
import pickle

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


def generate3(dimension, count, seed, start=0, end=100):
    data = [
        [k + j for j in range(dimension)]
        for k in range(start, end)
        for i in range(count)
    ]

    query = [seed] * dimension

    return data, query

def createSC():
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("C2LSH")
    sc = SparkContext(conf = conf)
    return sc

with open("toy/toy_hashed_data", "rb") as file:
    data = pickle.load(file)

with open("toy/toy_hashed_query", "rb") as file:
    query_hashes = pickle.load(file)

alpha_m = 10
beta_n = 10

sc = createSC()



def countCol(data_hash, query_hashes, offset, length):
    count = 0
    hash_val = data_hash.data[0]
    for i in range(length):
        h, q = hash_val[i], query_hashes[i]
        if abs(h - q) <= offset:
            count += 1

    return count


def c(data_hash, query_hashes, offset, length, alpha_m):
    count = 0
    k, v = data_hash

    for i in range(length):
        if abs(v[i] - query_hashes[i]) <= offset:
            count += 1

    if count >= alpha_m:
        return k


def sub(data_hash, query_hashes, offset, length, alpha_m):
    count = 0
    k, v = data_hash

    for i in range(length):
        v[i] = abs(v[i] - query_hashes[i])

    return k, v

########## Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset, cand = 0, 0

    #     query_hashes.cache()
    #     cand_set = set()
    length = len(query_hashes)
    a = None
    isFirst = True

    t = data_hashes.map(lambda x: sub(x, query_hashes, offset, length, alpha_m))
    while True:
        #         for t, v in data_hashes.groupByKey().collect():

        #             if count(v, query_hashes, offset, length) >= alpha_m:
        #                 cand_set.add(t)

        k = t.map(lambda x: (x[0], list(filter(lambda y: y <= offset, x[1]))))
        k = k.map(lambda x: (x[0], len(x[1])))
        k = k.filter(lambda x: x[1] >= alpha_m)

        if k.count() < beta_n:
            offset += 1
            # t = t.filter(lambda x: )
        else:
            break
        '''
        if isFirst:
            d = data_hashes.map(lambda x: c(x, query_hashes, offset, length, alpha_m))
            a = d.filter(lambda x: x is not None)
            isFirst = False
            # print(a.collect())
        else:
            d = data_hashes.map(lambda x: c(x, query_hashes, offset, length, alpha_m))
            d = d.filter(lambda x: x is not None)
            a = a.union(d)
            # print(type(a))

        if a.count() < beta_n:
            offset += 1
            b = a.map(lambda x: (x, None))
            data_hashes = data_hashes.subtractByKey(b)
        else:
            break
        '''
    #     rdd = sc.parallelize(cand_set)
    return k.map(lambda x: x[0])



alpha_m, beta_n = 10, 10
# data2, query2 = generate(10, 20000, 0, 0, 1000)
# query_hashes = query2
data_hashes = sc.parallelize([(index, x) for index, x in enumerate(data)])

start_time = time()
res = c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
print(res)
end_time = time()
print('time: ', end_time - start_time)
sc.stop()