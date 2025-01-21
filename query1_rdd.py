# Spark RDD code
from pyspark.sql import SparkSession
import time

sc = SparkSession \
    .builder \
    .appName("RDD query 1 execution") \
    .getOrCreate() \
    .sparkContext

start_time = time.time()

crimes_df1 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True)
crimes_df2 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv", header=True)


crimes_rdd1 = crimes_df1.rdd
crimes_rdd2 = crimes_df2.rdd

crimes1_header = crimes_rdd1.first()
crimes_rdd1_headerless = crimes_rdd1.filter(lambda line: line != crimes1_header)

crimes2_header = crimes_rdd2.first()
crimes_rdd2_headerless = crimes_rdd2.filter(lambda line: line != crimes2_header)

crimes = crimes_rdd1_headerless.union(crimes_rdd2_headerless)

aggravatedAssaultCrimes = crimes.filter(lambda x: "AGGRAVATED ASSAULT" in x[9])

print(aggravatedAssaultCrimes.count())

def getVictimAgeGroup(row):
    age = int(row[11])
    if age < 18:
        victimAgeCategory = "Children"
    elif 18 <= age <= 24:
        victimAgeCategory = "Young Adults"
    elif 25 <= age <= 64:
        victimAgeCategory = "Adults"
    elif 64 < age:
        victimAgeCategory = "Elderly"
    else:
        victimAgeCategory = "Unknown"
    return tuple(row) + (victimAgeCategory,)


resultRDD = aggravatedAssaultCrimes.map(getVictimAgeGroup)

orderedRDD = resultRDD.sortBy(lambda row: int(row[3]) , ascending=False)

def getAgeCategoryCount(row):
    ageCategory = row[-1]  # Last element
    return ageCategory, 1  # Tuple with the victimAgeCategory count 1

counted_rdd = resultRDD.map(getAgeCategoryCount).reduceByKey(lambda x, y: x + y)

sorted_counted_rdd = counted_rdd.sortBy(lambda x: x[1],ascending=False)

result = sorted_counted_rdd.collect()

for victimAgeCategory, count in result:
    print(f"{victimAgeCategory}: {count}")
    
    
print(f"Execution time: {time.time() - start_time}")
