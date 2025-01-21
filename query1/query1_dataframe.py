# Spark DataFrame code
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType,DoubleType,TimestampType
from pyspark.sql.functions import col,year,month,row_number,when
import time

spark = SparkSession \
    .builder \
    .appName("DF query 1 execution") \
    .getOrCreate()

start_time = time.time()

crimes_df1 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True)
crimes_df2 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv", header=True)

combined_df = crimes_df1.union(crimes_df2).dropDuplicates()

print(combined_df.count())

#print the schema of the DataFrame:
# crimes_df.printSchema()

crimes_df = combined_df.withColumn("Vict Age", col("Vict Age").cast("int"))

crimes_df = combined_df.filter(col("Crm Cd Desc").like("%AGGRAVATED ASSAULT%"))

childrenCondition = (col("Vict Age") < 18)
youngAdultCondition = ((col("Vict Age") >= 18) & (col("Vict Age") <= 24))
adultCondition = ((col("Vict Age") >= 25) & (col("Vict Age") <= 64))
elderlyCondition = (col("Vict Age") > 64)
                    
# Create a new column "salary_category" based on the conditions
categorized_crimes_df = crimes_df.withColumn("Vict Age Category", 
                                            when(childrenCondition,"Children")
                                            .when(youngAdultCondition, "Young Adult")
                                            .when(adultCondition, "Adult")
                                            .when(elderlyCondition, "Elderly")
                                            .otherwise("Unknown"))


selected_columns = ["Vict Age Category"]
df = categorized_crimes_df.select(*selected_columns)

result_df = df.groupBy("Vict Age Category").count()
sorted_result_df = result_df.orderBy(col("count").desc())

sorted_result_df.printSchema()

sorted_result_df.show(10)

print(f"Execution time: {time.time() - start_time}")
                    
