# Spark DataFrame code
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType,DoubleType,TimestampType
from pyspark.sql.functions import col,year,month,row_number,when
from pyspark.sql.functions import to_date,to_timestamp
from pyspark.sql.window import Window
import time

spark = SparkSession \
    .builder \
    .appName("DF query 2 execution") \
    .getOrCreate()

start_time = time.time()

crimes_df1 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True)
crimes_df2 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv", header=True)

crimes_df = crimes_df1.union(crimes_df2).dropDuplicates()

crimes_df = crimes_df.withColumn("Date Rptd", to_timestamp(crimes_df["Date Rptd"], "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("DATE OCC", to_timestamp(crimes_df["Date OCC"], "MM/dd/yyyy hh:mm:ss a"))
crimes_df = crimes_df.withColumn("Vict Age", col("Vict Age").cast("int"))
crimes_df = crimes_df.withColumn("LAT", col("LAT").cast("double"))
crimes_df = crimes_df.withColumn("LON", col("LON").cast("double"))

# All cases
selected_columns = ["DATE OCC", "AREA NAME"]
df = crimes_df.select(*selected_columns)
df = df.withColumn("Year", year(df["DATE OCC"]))

all_cases_df = df.groupBy("Year", "AREA NAME").count().withColumnRenamed("count", "All Cases")


# Closed Cases
crimes_df = crimes_df.filter(~col("Status Desc").isin("Invest Cont", "UNK"))
selected_columns = ["DATE OCC", "AREA NAME"]
df = crimes_df.select(*selected_columns)
df = df.withColumn("Year", year(df["DATE OCC"]))

closed_cases_df = df.groupBy("Year", "AREA NAME").count().withColumnRenamed("count", "Closed Cases")


# Join the total cases with closed cases
result_df = all_cases_df.join(
    closed_cases_df, 
    on=["Year", "AREA NAME"], 
    how="left"
).fillna(0, subset=["Closed Cases"])

result_df = result_df.withColumn("Percentage", (col("Closed Cases") / col("All Cases")) * 100)



# # Define a window specification partitioned by "Year" and ordered by "Value" in descending order
window_spec = Window.partitionBy("Year").orderBy(col("Percentage").desc())

# # Add a "rank" column to the DataFrame based on the window specification
df_ranked = result_df.withColumn("rank", row_number().over(window_spec))

# # Keep only the top 3 rows for each year
df_top3 = df_ranked.filter(col("rank") <= 3)

final_df = df_top3.select(
    col("year").alias("Year"),
    col("AREA NAME").alias("AREA NAME"),
    col("Percentage").alias("Total Crimes"),
    col("rank").alias("#")
)

filtered_final_df = final_df.filter(col("year").isNotNull())

filtered_final_df.printSchema()
# # Show the updated DataFrame
filtered_final_df.show(50)

print(f"Execution time: {time.time() - start_time}")
