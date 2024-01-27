from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Create some sample data
data = [
  ('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200), 
  ('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200),
  ('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200),
  ('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200),
]

#Create column name of the above sample data
columns = ["firstname","lastname","gender","salary"]

#Create a data frame of sample data and show the sample data into data frame
df = spark.createDataFrame(data=data, schema = columns)
df.show()

#Cretae rdd of the above data frame and apply partition as well as see the total partions 
df_rdd = df.rdd.repartition(100)
print('No of partitions: ' +str(df_rdd.getNumPartitions()))

# Display the contents of each partition
partition_data = df_rdd.glom().collect()
for partition_number, data in enumerate(partition_data):
    print(f"Partition {partition_number}:")
    for item in data:
        print(item)


#Example 1 mapPartitions()
def reformat(partitionData):
    for row in partitionData:
        yield [row.firstname+","+row.lastname,row.salary*10/100]
df2=df_rdd.mapPartitions(reformat).toDF(["name","bonus"])
df2.show()