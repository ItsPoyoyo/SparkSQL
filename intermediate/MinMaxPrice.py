import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, min as spark_min, max as spark_max, col
import shutil, os

spark = SparkSession.builder \
    .appName("MinMaxPricePerCompanyYear_SQL") \
    .master("local[*]") \
    .getOrCreate()

input_path = "stock_details_5_years.csv"
output_path = "outputMinMaxPrice_SQL"

if os.path.exists(output_path):
    shutil.rmtree(output_path)

df = spark.read.csv(input_path, header=True, inferSchema=True)

dados = df.select(
    col(df.columns[0]).alias("date"),
    col(df.columns[4]).alias("close"),
    col(df.columns[-1]).alias("company")
)

dados = dados.withColumn("year", year(col("date")))
dados.createOrReplaceTempView("stocks")

resultado = spark.sql("""
    SELECT
        company,
        year,
        MIN(close) AS min_price,
        MAX(close) AS max_price
    FROM stocks
    GROUP BY company, year
    ORDER BY company, year
""")

resultado.show(truncate=False)

resultado.rdd.map(lambda row: f"{row['company']}\t{row['year']}\tMin: {row['min_price']:.2f}\tMax: {row['max_price']:.2f}") \
    .saveAsTextFile(output_path)

spark.stop()
