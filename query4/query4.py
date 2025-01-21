# Query 4

%%configure -f
{
    "conf": {
        "spark.executor.instances": "2",
        "spark.executor.memory": "1g",
        "spark.executor.cores": "1",
        "spark.driver.memory": "2g"
    }
}

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

combined_df_asc_top3 = combined_df_asc.agg(ST_Union_Aggr("geometry").alias("geometry"))
print(combined_df_asc_top3.show())

combined_df_desc_bot3 = combined_df_desc.agg(ST_Union_Aggr("geometry").alias("geometry"))
print(combined_df_desc_bot3.show())

from sedona.spark import *

crimes_df = crimes_df.filter(col("DATE OCC").like("%2015%")  & col("Vict Descent").isNotNull())
# Create a geometry type from (Lat, Long) coordinate pairs
crimes_df = crimes_df.withColumn("geom", ST_Point(col("LON"), col("LAT")))


selected_columns = ["DR_NO", "LON", "Vict Descent", "LAT", "geom"]
crimes_df = crimes_df.select(*selected_columns)

joined_df = crimes_df.join(
    combined_df_asc_top3, 
    ST_Within(crimes_df["geom"], combined_df_asc_top3["geometry"]), 
    "inner"
)

selected_columns = ["DR_NO", "Vict Descent", "geom", "geometry"]
joined_df = joined_df.select(*selected_columns)

result_df = joined_df.join(
    race_df, 
    on="Vict Descent", 
    how="left"
)

joined_df = result_df.groupBy("Vict Descent Full").count().withColumnRenamed("count", "Victims Per Race/Ethnic Group").orderBy(col("Victims Per Race/Ethnic Group").desc())


joined_df.show()

# Print schema
joined_df.printSchema()

# Perform a spatial join
joined_df = crimes_df.join(
    combined_df_desc_bot3, 
    ST_Within(crimes_df["geom"], combined_df_desc_bot3["geometry"]), 
    "inner"
)

selected_columns = ["DR_NO", "Vict Descent", "geom", "geometry"]
joined_df = joined_df.select(*selected_columns)

result_df = joined_df.join(
    race_df, 
    on="Vict Descent", 
    how="left"
)

joined_df = result_df.groupBy("Vict Descent Full").count().withColumnRenamed("count", "Victims Per Race/Ethnic Group").orderBy(col("Victims Per Race/Ethnic Group").desc())


joined_df.show()

# Print schema
joined_df.printSchema()
