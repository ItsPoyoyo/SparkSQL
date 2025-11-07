from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max
import os
import shutil

def removeDir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

outputDirectory: str = "outputMinMaxClose"
removeDir(outputDirectory)

# SparkSession
ss = SparkSession.builder.appName("MinMaxClose").getOrCreate()

# Corrected path: removed '../' as the file is in the current working directory.
path: str = "stock_details_5_years.csv"
schema: str = """
    date string,
    open double,
    high double,
    low double,
    close double,
    volume double,
    dividends double,
    stockSplits double,
    company string
"""

# read CSV
df = ss.read.option("header", True).schema(schema).csv(path)

# register temp view to use SQL
df.createOrReplaceTempView("stocks")

# Spark SQL: get min/max close and corresponding dates
result = ss.sql("""
    SELECT
        company,
        MIN(close) AS min_close,
        MAX(close) AS max_close
    FROM stocks
    GROUP BY company
""")

# save output
result.write.mode("overwrite").json(outputDirectory)
