import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Criação da SparkSession
spark = SparkSession.builder \
    .appName("VolatilityPerMonth") \
    .master("local[*]") \
    .getOrCreate()

# Leitura do CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("stock_details_5_years.csv")

# Criação de coluna 'year_month' a partir da data
df = df.withColumn("year_month", F.date_format(F.to_date("Date", "yyyy-MM-dd"), "yyyy-MM"))

# Registro da view temporária para SQL
df.createOrReplaceTempView("stocks")

# Consulta SQL para calcular a volatilidade (máximo - mínimo do Close) por empresa e mês
volatility_df = spark.sql("""
    SELECT 
        Company,
        year_month,
        MAX(Close) - MIN(Close) AS Volatility
    FROM stocks
    GROUP BY Company, year_month
    ORDER BY Company, year_month
""")

# Exibição e salvamento do resultado
volatility_df.show()
volatility_df.write.mode("overwrite").csv("output_sparksql")
