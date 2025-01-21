%%configure -f
{
    "conf": {
        "spark.executor.instances": "2",
        "spark.executor.memory": "4g",
        "spark.executor.cores": "2",
        "spark.driver.memory": "2g"
    }
}

# Spark DataFrame code
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType,DoubleType,TimestampType
from pyspark.sql.functions import col, count, upper, avg
from sedona.spark import *
import time

sedona = SedonaContext.create(spark)

from pyspark.sql.functions import upper

start_time = time.time()

LA_police_stations = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/LA_Police_Stations.csv", header=True)
crimes_df = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True)

crimes_df = crimes_df.filter(col("LAT") != "0")
crimes_df = crimes_df.filter(col("LON") != "0")


crimes_df = crimes_df.withColumn("LON", col("LON").cast("double")).withColumn("LAT", col("LAT").cast("double"))

crimes_df = crimes_df.join(
    LA_police_stations,
    on=upper(crimes_df["AREA NAME"]) == LA_police_stations["DIVISION"],
    how="left"
)

crimes_LA_df = crimes_df.withColumn("police_station_geom", ST_Point(col("X"), col("Y"))).withColumn("crime_geom", ST_Point(col("LON"), col("LAT")))


crimes_LA_df = crimes_LA_df.withColumn("distance_km", ST_DistanceSphere("crime_geom", "police_station_geom")/1000) # divide with 1000 to conver into km

crimes_LA_df = crimes_LA_df.filter(col("distance_km").isNotNull())

crimes_LA_df = crimes_LA_df.groupBy("AREA NAME").agg(
    avg(col("distance_km")).alias("Avg Distance"),
    count("*").alias("#")
)

                                    
crimes_LA_df.printSchema()

crimes_LA_df.orderBy(col("#").desc()).show()

print(f"Execution time: {time.time() - start_time}")
