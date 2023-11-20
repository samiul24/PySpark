#Read Weather CSV file
weatherRdd = sc.textFile('/FileStore/tables/weather.csv', 5)
print(weatherRdd.getNumPartitions()) #Number of partition
print(weatherRdd.glom()) #Process to see the each partition data
print(weatherRdd.glom().collect()) #Process to see the each partition data

=============

#Read Zip City CSV file
zipCityRdd = sc.textFile('/FileStore/tables/zips_city.csv')
print(zipCityRdd.getNumPartitions()) #Number of partition
print(zipCityRdd.glom()) #Process to see the each partition data
print(zipCityRdd.glom().collect()) #Process to see the each partition data

=============


# Create function for zip code wise temperature
def getZipTemp(input_record):
    _, zipCode, temp = input_record.split(',')
    return (int(zipCode), int(temp))

print('Zip code wise temperature.')
zipCodeTempRdd = weatherRdd.map(getZipTemp)
print(zipCodeTempRdd.collect(), '\n')

print('Zip code wise max temperature.')
zipCodeMaxTempRdd = zipCodeTempRdd.reduceByKey(lambda x,y:max(x,y))
print(zipCodeMaxTempRdd.collect(), '\n')

zipCitySplitRdd = zipCityRdd.map(lambda x:(int(x.split(',')[0]), x.split(',')[1]))
print(zipCitySplitRdd.collect())

#RDD Join
#https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.join.html
#https://sparkbyexamples.com/spark/spark-rdd-join/
joinedRdd = zipCitySplitRdd.join(zipCodeMaxTempRdd)
print('\n RDD Join')
print(joinRdd.collect())

print('\n After RDD join, data formating')
resultRdd = joinedRdd.map(lambda x: (x[1][0], x[1][1]))
print(resultRdd.collect())
display(resultRdd.collect())