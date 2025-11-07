from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
import os
import shutil

def removeDir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

outputDirectory : str = "outputAllVale"

removeDir(outputDirectory)

ss = SparkSession.builder.getOrCreate()
path :str = "../stock_details_5_years.csv"
schema :str = "date string, open double, high double, low double, close double, volume double, dividends double, stockSplits double, company string" 
df = ss.read.option("header", True).schema(schema).csv(path)
df2 = df.filter(upper(col("company")) == "VALE")
df2.write.mode("overwrite").json(outputDirectory)
