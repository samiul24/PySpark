RawData=sc.textFile('/FileStore/tables/ebook.txt',4) 
print('Number of partitions: ' + str(RawData.getNumPartitions()))


=====

#flatMap in PySpark
#https://sparkbyexamples.com/pyspark/pyspark-flatmap-transformation/
WordsFlatMap=RawData.flatMap(lambda x:x.split(' '))
WordsFlatMap.collect()

display(WordsFlatMap)
WordsMap=WordsFlatMap.map(lambda x: (x,1))
WordsMap.collect()

WordsReduceByKey=WordsMap.reduceByKey(lambda x,y:x+y)
WordsReduceByKey.collect()