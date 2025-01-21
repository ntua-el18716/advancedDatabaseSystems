from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, year
import time

# Initialize SparkSession
spark = SparkSession.builder.appName("SQL API Crime Analysis").getOrCreate()

start_time = time.time()

# Load data
crimes_df1 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2010_to_2019_20241101.csv", header=True)
crimes_df2 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/CrimeData/Crime_Data_from_2020_to_Present_20241101.csv", header=True)

crimes_df1.createOrReplaceTempView("crimes1")
crimes_df2.createOrReplaceTempView("crimes2")


crimes_query = """
        SELECT * FROM crimes1
        UNION
        SELECT * FROM crimes2
"""

    
crimes_df = spark.sql(crimes_query)


crimes_df = crimes_df.withColumn("DATE OCC", to_timestamp(col("Date OCC"), "MM/dd/yyyy hh:mm:ss a")) \


# Create temporary view
crimes_df.createOrReplaceTempView("crimes")

all_cases_query = """
    SELECT 
        YEAR(`DATE OCC`) AS Year, 
        `AREA NAME`, 
        COUNT(*) AS `All Cases`
    FROM crimes
    GROUP BY YEAR(`DATE OCC`), `AREA NAME`
"""

all_cases_df = spark.sql(all_cases_query)

# SQL: Filter for closed cases and group by year and area
closed_cases_query = """
    SELECT 
        YEAR(`DATE OCC`) AS Year, 
        `AREA NAME`, 
        COUNT(*) AS `Closed Cases`
    FROM crimes
    WHERE `Status Desc` NOT IN ('Invest Cont', 'UNK')
    GROUP BY YEAR(`DATE OCC`), `AREA NAME`
"""
closed_cases_df = spark.sql(closed_cases_query)

# Create temporary views for joins
all_cases_df.createOrReplaceTempView("all_cases")
closed_cases_df.createOrReplaceTempView("closed_cases")

# SQL: Join all cases with closed cases and calculate percentage
result_query = """
    SELECT 
        a.Year, 
        a.`AREA NAME`, 
        a.`All Cases`, 
        COALESCE(c.`Closed Cases`, 0) AS `Closed Cases`,
        ROUND((COALESCE(c.`Closed Cases`, 0) / a.`All Cases`) * 100, 2) AS `Percentage`
    FROM all_cases a
    LEFT JOIN closed_cases c
    ON a.Year = c.Year AND a.`AREA NAME` = c.`AREA NAME`
"""
result_df = spark.sql(result_query)

# Create temporary view for ranking
result_df.createOrReplaceTempView("result")

# SQL: Rank areas by closure percentage and filter top 3 per year
top3_query = """
    WITH RankedData AS (
        SELECT 
            Year, 
            `AREA NAME`, 
            `All Cases`, 
            `Closed Cases`, 
            `Percentage`,
            ROW_NUMBER() OVER (PARTITION BY Year ORDER BY `Percentage` DESC) AS rank
        FROM result
    )
    SELECT 
        Year, 
        `AREA NAME`, 
        `Percentage` AS `Total Crimes`, 
        rank AS `#`
    FROM RankedData
    WHERE rank <= 3
"""
final_df = spark.sql(top3_query)

# Show the final results
final_df.show(50)
print(f"Execution time: {time.time() - start_time}")
