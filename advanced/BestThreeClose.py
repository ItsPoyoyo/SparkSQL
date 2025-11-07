from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, avg, row_number
from pyspark.sql.window import Window
import os, shutil

def remove_dir(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)

saida = "outputBestThreeClose"
remove_dir(saida)

# Create session
spark = SparkSession.builder.appName("BestThreeCloseSQL").getOrCreate()

# Load CSV and infer schema
df = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("stock_details_5_years.csv")
)

# Preprocessing: extract year-month
df = df.withColumn("ano_mes", date_format(col("Date"), "yyyy-MM"))

df.createOrReplaceTempView("stocks")

# 1) average "close" per company per year-month
media = spark.sql("""
    SELECT
        company,
        ano_mes,
        AVG(close) AS avg_close
    FROM stocks
    GROUP BY company, ano_mes
""")

media.createOrReplaceTempView("media_close")

# 2) add ranking and pick best top 3 months per company
resultado = spark.sql("""
    SELECT
        company,
        ano_mes,
        avg_close
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY company ORDER BY avg_close DESC) AS rn
        FROM media_close
    )
    WHERE rn <= 3
    ORDER BY company, avg_close DESC
""")

resultado.show(truncate=False)

# Save result
resultado.write.mode("overwrite").json(saida)
print("Resultado salvo em", saida)
