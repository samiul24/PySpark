#PySpark and RDD
#sc = SparkContext
my_list=[1,2,3,4,5,6,7,8,9,10]
rdd=sc.parallelize(my_list,4) #make parallalize

#How to see number of partions
print(rdd.getNumPartitions())

#To see the data set of each partition
rdd=rdd.glom()
print(rdd.collect())