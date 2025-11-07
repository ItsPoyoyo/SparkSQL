from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, date_format
import os
import shutil

def removeDir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

outputDirectory: str = "outputVolumePerCompanyMonth"
removeDir(outputDirectory)

# Spark Session
ss = SparkSession.builder.appName("VolumePerCompanyMonth").getOrCreate()

path: str = "stock_details_5_years.csv"
schema: str = """
    date string,
    open double,
    high double,
    low double,
    close double,
    volume string,
    dividends double,
    stockSplits double,
    company string
"""

# Read CSV as DataFrame
df = ss.read.option("header", True).schema(schema).csv(path)

# Convert volume to int (remove commas)
df = df.withColumn("volume_int", col("volume").cast("int"))

# Add year-month column (YYYY-MM format)
df = df.withColumn("year_month", date_format(col("date"), "yyyy-MM"))

# Register as SQL table
df.createOrReplaceTempView("stocks")

# Spark SQL to aggregate volume per company per month
result = ss.sql("""
    SELECT
        company,
        year_month,
        SUM(volume_int) AS total_volume
    FROM stocks
    GROUP BY company, year_month
    ORDER BY company, year_month
""")

result.show(50, truncate=False)

# Save final result
result.write.mode("overwrite").json(outputDirectory)

print("✅ Output saved to:", outputDirectory)
