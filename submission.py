## import modules here
from pyspark import SparkContext, SparkConf
from time import time
import pickle


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
data_hashes = sc.parallelize([(index, x) for index, x in enumerate(data)])


def countCol(data_hash, query_hashes, offset, length):
    count = 0
    hash_val = data_hash.data[0]
    for i in range(length):
        h, q = hash_val[i], query_hashes[i]
        if abs(h - q) <= offset:
            count += 1

    return count


########## Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset, cand = 0, 0

    #     query_hashes.cache()
    cand_set = set()
    length = len(query_hashes)

    while True:
        for t, v in data_hashes.groupByKey().collect():

            if t not in cand_set and countCol(v, query_hashes, offset, length) >= alpha_m:
                cand_set.add(t)

        if len(cand_set) < beta_n:
            offset += 1
        else:
            break

    rdd = sc.parallelize(cand_set)
    return rdd


start_time = time()
res = c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
print(res)
end_time = time()
sc.stop()