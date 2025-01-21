from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("DF query 2execution") \
    .getOrCreate()

crimes_df1 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True)
crimes_df2 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv", header=True)

crimes_df = crimes_df1.union(crimes_df2).dropDuplicates()

crimes_df.coalesce(1).write.mode("overwrite").parquet("s3://groups-bucket-dblab-905418150721/group41/Query2")


from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType,DoubleType,TimestampType
from pyspark.sql.functions import col,year,month,row_number,when
from pyspark.sql.functions import to_date,to_timestamp
from pyspark.sql.window import Window
import time

# Initialize Spark session
spark = SparkSession.builder.appName("DF query 2 execution").getOrCreate()

# Start time for performance measurement
start_time = time.time()

# Load the data
crimes_df_parquet = spark.read.parquet("s3://groups-bucket-dblab-905418150721/group41/Query2/part-00000-1b347611-dd2f-4660-a804-913f5c9ed6ae-c000.snappy.parquet")

crimes_df_parquet = crimes_df_parquet.withColumn("Date Rptd", to_timestamp(crimes_df_parquet["Date Rptd"], "MM/dd/yyyy hh:mm:ss a"))
crimes_df_parquet = crimes_df_parquet.withColumn("DATE OCC", to_timestamp(crimes_df_parquet["Date OCC"], "MM/dd/yyyy hh:mm:ss a"))
crimes_df_parquet = crimes_df_parquet.withColumn("Vict Age", col("Vict Age").cast("int"))
crimes_df_parquet = crimes_df_parquet.withColumn("LAT", col("LAT").cast("double"))
crimes_df_parquet = crimes_df_parquet.withColumn("LON", col("LON").cast("double"))

# Check number of records
print(crimes_df_parquet.count())

# Select columns and extract Year
df = crimes_df_parquet.select("DATE OCC", "AREA NAME", "Status Desc") \
    .withColumn("Year", year("DATE OCC"))

# Count all cases
all_cases_df = df.groupBy("Year", "AREA NAME").count().withColumnRenamed("count", "All Cases")

# Filter out closed cases and count
closed_cases_df = df.filter(~col("Status Desc").isin("Invest Cont", "UNK")) \
    .groupBy("Year", "AREA NAME").count().withColumnRenamed("count", "Closed Cases")

# Join All Cases and Closed Cases and calculate the percentage
result_df = all_cases_df.join(closed_cases_df, on=["Year", "AREA NAME"], how="left") \
    .fillna(0, subset=["Closed Cases"]) \
    .withColumn("Percentage", (col("Closed Cases") / col("All Cases")) * 100)

# Rank the results by Percentage (descending)
window_spec = Window.partitionBy("Year").orderBy(col("Percentage").desc())
df_ranked = result_df.withColumn("rank", row_number().over(window_spec))

# Get top 3 rows per year
df_top3 = df_ranked.filter(col("rank") <= 3)

# Final DataFrame with selected columns
final_df = df_top3.select(
    col("Year"),
    col("AREA NAME"),
    col("Percentage").alias("Total Crimes"),
    col("rank").alias("#")
)

# Filter null years and display the results
filtered_final_df = final_df.filter(col("Year").isNotNull())
filtered_final_df.printSchema()
filtered_final_df.show(50)

# Print execution time
print(f"Execution time: {time.time() - start_time}")
