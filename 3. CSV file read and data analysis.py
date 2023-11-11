#csv file directory /FileStore/tables/weather-2.csv

#Read csv file
rdd_csv=sc.textFile('/FileStore/tables/weather-2.csv',2)
print('Number of partitions: ' + str(rdd_csv.getNumPartitions()))

#To see the data set of each partition
rdd=rdd_csv.glom()
print(rdd.collect())

#Use map for transformation & split for data segment
rdd_csv_select_year_temp=rdd_csv.map(lambda full_row:(full_row.split('-')[0],full_row.split(',')[-1]))

#Use collect() as Action
print('\nCollect year & temperature')
print(rdd_csv_select_year_temp.collect())

#Max tempeature by year
max_temp_by_year=rdd_csv_select_year_temp.reduceByKey(lambda x,y:x if x>y else y)

print('\nYear wise max temperature')
print(max_temp_by_year.collect())


====

#Wrapper & partition object with function name
print(f'Max temp partion name: {max_temp_by_year}')
print(f'Max temp partion name: {max_temp_by_year.partitioner}')
print(f'Max temp partion name: {max_temp_by_year.partitioner.partitionFunc}')

====

#Max temp sort by key
year_wise_max_temp_sort_by_key=max_temp_by_year.sortByKey()
print(f'Year wise max temp sort by key:')
print(year_wise_max_temp_sort_by_key.collect())

====

%fs 
rm -r /FileStore/output/max_temperature

%fs
rm -r /FileStore/output/sorted_max_temperature

====

#Save 
max_temp_by_year.saveAsTextFile('/FileStore/output/max_temperature')
max_temp_by_year.saveAsTextFile('/FileStore/output/year_wise_max_temp_sort_by_key')

====

%fs
ls /FileStore/output/year_wise_max_temp_sort_by_key

%fs
ls /FileStore/output/year_wise_max_temp_sort_by_key

%fs
head /FileStore/output/year_wise_max_temp_sort_by_key/part-00006