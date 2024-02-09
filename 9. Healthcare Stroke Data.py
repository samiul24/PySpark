#Read the health care data file and crate rdd using spark context
healthcare_data_file = "/FileStore/tables/healthcare_dataset_stroke_data.csv"
healthcare_rdd = sc.textFile(healthcare_data_file,8)
print(type(healthcare_rdd))
print(healthcare_rdd.count())


#Number of partitions and see each partition data using glom() and collect()
print(healthcare_rdd.getNumPartitions())
print(type(healthcare_rdd))
print(type(healthcare_rdd.glom()))
print(type(healthcare_rdd.glom().collect()))
#print(healthcare_rdd.glom().collect())

#Write a function to skip the header of the first partition
def skipHeader(index, partition):
    column_number = -1
    for record in partition:
        column_number += 1
        if index==0 and column_number==0:
            continue
        yield record

healthcare_rdd_with_header_remove = healthcare_rdd.mapPartitionsWithIndex(skipHeader)
healthcare_rdd_with_header_remove.count()

#See the records of rdd before skip the header and after skip the header
healthcare_rdd.take(2)
healthcare_rdd_with_header_remove.take(2)




