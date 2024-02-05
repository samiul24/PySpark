#https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDDBarrier.mapPartitionsWithIndex.html
rdd = sc.parallelize([1, 2, 3, 4], 4)
print(rdd.getNumPartitions())
print(rdd.glom().collect())

def f(Index, iterator):
    yield Index

#https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.RDD.barrier.html
barrier = rdd.barrier()
barrier

barrier.mapPartitionsWithIndex(f).sum()