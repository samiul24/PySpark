#Source file directory
nyse_daily_data_file = "/FileStore/tables/nyse_daily.tsv"
nyse_dividends_data_file = "/FileStore/tables/nyse_dividends.tsv"

#Read data from source file using spark context with default partion and see the data
daily_rdd = sc.textFile(nyse_daily_data_file)
daily_rdd.collect()

#Read data from source file using spark context with specific partion and check the number of partions
daily_rdd = sc.textFile(nyse_daily_data_file,1000)
print('Number of partitions: ' + str(daily_rdd.getNumPartitions()))

#How to see which partion are containing what data and how many data
daily_rdd_glom = daily_rdd.glom()
daily_rdd_glom.collect()

