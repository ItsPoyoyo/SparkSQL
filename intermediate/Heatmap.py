from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, date_format, col, array, struct, explode, lit,
    avg, concat, format_number
)
import os
import shutil

def removeDir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

outputDirectory : str = "outputHeatmap"

removeDir(outputDirectory)

ss = SparkSession.builder.getOrCreate()
path :str = "../stock_details_5_years.csv"
df = ss.read.option("header", True).option("inferSchema", True).csv(path)
df = df.withColumn("dt", to_date(col("date").substr(1, 10), "yyyy-MM-dd"))
periods = array(
    struct(lit("Month").alias("period_type"), date_format(col("dt"), "MMMM").alias("period_value")),
    struct(lit("Day").alias("period_type"), date_format(col("dt"), "EEEE").alias("period_value"))
)
df2 = df.withColumn("period", explode(periods)) \
        .select(
            col("period.period_type").alias("period_type"),
            col("period.period_value").alias("period_value"),
            col("company"),
            col("volume").cast("double").alias("volume")
        )
result = df2.groupBy("period_type", "period_value", "company") \
            .agg(avg("volume").alias("avg_volume"))
output = result.select(
    concat(
        col("period_type"), lit(": "), col("period_value"),
        lit(" | Company: "), col("company"),
        lit(" | Average: "), format_number(col("avg_volume"), 2)
    ).alias("value")
).write.mode("overwrite").json(outputDirectory)
