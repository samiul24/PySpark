#Source file directory
nyse_daily_data_file = "/FileStore/tables/nyse_daily.tsv"
nyse_dividends_data_file = "/FileStore/tables/nyse_dividends.tsv"

#Read data from source file using spark context and create RDD with default partion and see the data
daily_rdd = sc.textFile(nyse_daily_data_file)
print(type(daily_rdd))
daily_rdd.collect()

#Read data from source file using spark context and create RDD with number of 
#specific partion and check the number of partions
daily_rdd = sc.textFile(nyse_daily_data_file,100)
print('Number of partitions: ' + str(daily_rdd.getNumPartitions()))

#How to see which partion are containing what data and how many data?
#Display the contents of each partition
for partition_number, data in enumerate(daily_rdd.glom().collect()):
    print(f"Partition {partition_number}:")
    for item in data:
        print(item)

#Write a function that will parse nyse daily records
def getParseDailyRecords(daily):
    daily_list  =   daily.split("\t")
    exchange    =   daily_list[0]
    symbol      =   daily_list[1]
    date        =   daily_list[2]
    open        =   daily_list[3]
    high        =   daily_list[4]
    close       =   daily_list[6]
    return ((exchange, symbol, date), open, close)

#Process the daily_rdd using the above function that means using the function "getParseDailyRecords", nyse daily data #will be processed.
daily_pari_rdd = daily_rdd.map(getParseDailyRecords)
daily_pari_rdd.collect()


#Write a function that will parse nyse dividends data
def generateDividendsDictionary(dividend):
    dividend_list  =   dividend.split("\t")
    exchange       =   dividend_list[0]
    symbol         =   dividend_list[1]
    date           =   dividend_list[2]
    dividends      =   dividend_list[3]
    return ((exchange, symbol, date), dividends)

#Create a rdd for dividends data file
dividends_rdd       =   sc.textFile(nyse_dividends_data_file)

#Pass the RDD (basically data of each parition) to a function
dividends_pair_rdd  =   dividends_rdd.map(generateDividendsDictionary)

#Convert to dictionary
#dividends_pair_rdd.collect()
dividends_dict      =   dict(dividends_pair_rdd.collect())
print(dividends_dict)
print(type(dividends_dict))

#Create a broadcast variable and print the variable to see the object description
dividends_bcv   =   sc.broadcast(dividends_dict)
print('\n',  dividends_bcv)

#Write a function that will parse nyse daily rdd partition and process the data
def getDiffAndDividends(daily_pari_iter):
    for daily_pari in daily_pari_iter:
        key, open, close = daily_pari
        print(key)
        dividends_value = dividends_dict.get(key)
        print(dividends_value)
        if dividends_value is None:
            continue
        else:
            exchange, symbol, date = key
            yield "{} {} {} {} {}".format(exchange, symbol, date, float(close)-float(open), dividends_value)

#daily_pari_rdd now passing to mapPartition
result_rdd = daily_pari_rdd.mapPartitions(getDiffAndDividends)
for result in result_rdd.take(5):
    print(result)