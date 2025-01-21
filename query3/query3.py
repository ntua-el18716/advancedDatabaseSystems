from sedona.spark import *
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Create spark Session
spark = SparkSession.builder \
    .appName("GeoJSON read") \
    .getOrCreate()

# Create sedona context
sedona = SedonaContext.create(spark)
# Read the file from s3
geojson_path = "s3://initial-notebook-data-bucket-dblab-905418150721/2010_Census_Blocks.geojson"
blocks_df = sedona.read.format("geojson") \
            .option("multiLine", "true").load(geojson_path) \
            .selectExpr("explode(features) as features") \
            .select("features.*")
# Formatting magic
flattened_df = blocks_df.select( \
                [col(f"properties.{col_name}").alias(col_name) for col_name in \
                blocks_df.schema["properties"].dataType.fieldNames()] + ["geometry"]) \
            .drop("properties") \
            .drop("type")
# Print schema
flattened_df.printSchema()

# Spark DataFrame code
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType,DoubleType,TimestampType
from pyspark.sql.functions import col,year,month,row_number,when
from pyspark.sql.functions import to_date,to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import col, sum, avg


spark = SparkSession \
    .builder \
    .appName("DF query 2 execution") \
    .getOrCreate()

med_household_income_df = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/LA_income_2015.csv", header=True)

crimes_df = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True)


# Join DataFrames on ZIP Code
combined_df = flattened_df.join(
    med_household_income_df,
    flattened_df["ZCTA10"] == med_household_income_df["Zip Code"],
    how="inner"
)

# Clean Median Income
combined_df = combined_df.withColumn(
    "Median Income",
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
)



# Group By Community and Aggregate

combined_df = combined_df.groupBy("COMM").agg(
    sum(col("POP_2010")).alias("Total Population"),
    sum(col("HOUSING10")).alias("Household Population"),
    avg(col("Median Income")).alias("Estimated Median Income")
)

combined_df = combined_df.withColumn("Median Income Per Capita", ((col("Household Population") * col("Estimated Median Income")) / col("Total Population")))

combined_df = combined_df.withColumn(
    "Median Income Per Capita",
    when(col("Total Population") > 0, (col("Household Population") * col("Estimated Median Income")) / col("Total Population"))
    .otherwise(None)
)


# Show Results
combined_df.show(5)

from sedona.spark import *


flattened_df = flattened_df.filter(col("CITY") == "Los Angeles") \
                .groupBy("COMM") \
                .agg(ST_Union_Aggr("geometry").alias("geometry"))

crimes_df = crimes_df.filter(col("DATE OCC").like("%2010%"))

# Create a geometry type from (Lat, Long) coordinate pairs
crimes_df = crimes_df.withColumn("geom", ST_Point(col("LON"), col("LAT")))


selected_columns = ["DR_NO", "LON", "LAT", "geom"]
crimes_df = crimes_df.select(*selected_columns)

joined_df = crimes_df.join(
    flattened_df, 
    ST_Within(crimes_df["geom"], flattened_df["geometry"]), 
    "inner"
)

selected_columns = ["DR_NO", "LON", "LAT", "geom", "geometry", "COMM"]
joined_df = joined_df.select(*selected_columns)

joined_df = joined_df.groupBy("COMM").count().withColumnRenamed("count", "Number of Cases")


joined_df.show(15)

# Print schema
joined_df.printSchema()

result_df = joined_df.join(
    combined_df, 
    on=["COMM"], 
    how="inner"
)

result_df = result_df.withColumn("Crimes Per Capita", (col("Number of Cases") / col("Total Population")))

selected_columns = ["COMM", "Total Population", "Median Income Per Capita", "Number of Cases", "Crimes Per Capita"]
result_df = result_df.select(*selected_columns).orderBy(col("Median Income Per Capita").desc())
    
print(result_df.count())
result_df.show(20)
