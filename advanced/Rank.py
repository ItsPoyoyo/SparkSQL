from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc, format_string, concat_ws, trim, row_number, concat, lit
from pyspark.sql.window import Window
import os
import shutil

def removeDir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

outputDirectory : str = "outputRank"

removeDir(outputDirectory)

ss = SparkSession.builder.getOrCreate()
path : str = "../stock_details_5_years.csv"
schema : str = "date string, open double, high double, low double, close double, volume double, dividends double, stockSplits double, company string"
df = ss.read.option("header", True).schema(schema).csv(path)
df_clean = df.filter(
    col("open").isNotNull() & col("close").isNotNull() & (col("open") != 0)
).withColumn("company", trim(col("company")))
df_ratio = df_clean.withColumn("ratio", col("close") / col("open"))
agg = df_ratio.groupBy("company").agg(avg("ratio").alias("avg_ratio"))
window = Window.orderBy(desc("avg_ratio"))
ranked = agg.withColumn("rank", row_number().over(window)).orderBy(desc("avg_ratio"))
ranked.select(
      concat(
          lit("Rank: "), col("rank").cast("string"),
          lit(" | Company: "), col("company"),
          lit(" | Avg: "), format_string("%.6f", col("avg_ratio"))
      ).alias("value")
).write.mode("overwrite").text(outputDirectory)
