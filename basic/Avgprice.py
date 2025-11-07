import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("PrecoMedioPorEmpresa_SQL") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("stock_details_5_years.csv", header=True, inferSchema=True)

dados = df.select(
    col(df.columns[8]).alias("empresa"),
    col(df.columns[1]).alias("open"),
    col(df.columns[2]).alias("high"),
    col(df.columns[3]).alias("low"),
    col(df.columns[4]).alias("close")
)

dados = dados.withColumn("media", (col("open") + col("high") + col("low") + col("close")) / 4)

dados.createOrReplaceTempView("stocks")

resultado = spark.sql("""
    SELECT
        empresa,
        AVG(media) AS preco_medio
    FROM stocks
    GROUP BY empresa
    ORDER BY empresa
""")

resultado.show(truncate=False)

spark.stop()
