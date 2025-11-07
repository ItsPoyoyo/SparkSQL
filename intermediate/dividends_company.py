import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inicializa a SparkSession
spark = SparkSession.builder \
    .appName("DividendsCompany") \
    .master("local[*]") \
    .getOrCreate()

# Lê o arquivo CSV com cabeçalho
df = spark.read.option("header", True).option("inferSchema", True).csv("../stock_details_5_years.csv")

# Extrai o ano da coluna Date
df = df.withColumn("year", F.year(F.to_date("Date", "yyyy-MM-dd")))

# Cria uma tabela temporária
df.createOrReplaceTempView("stocks")

# Consulta SQL para total e média de Dividendos por empresa e ano
dividends_df = spark.sql("""
    SELECT 
        Company,
        year,
        ROUND(SUM(Dividends), 2) AS Total_Dividends,
        ROUND(AVG(Dividends), 4) AS Avg_Per_Day
    FROM stocks
    GROUP BY Company, year
    ORDER BY Company, year
""")

# Formata o texto como no seu RDD
formatted_df = dividends_df.withColumn(
    "Formatted",
    F.concat(
        F.lit("Total Dividends: $"),
        F.format_number(F.col("Total_Dividends"), 2),
        F.lit(", Avg per Day: $"),
        F.format_number(F.col("Avg_Per_Day"), 4)
    )
)

# Mostra o resultado
formatted_df.show(truncate=False)

# Salva em CSV
formatted_df.write.mode("overwrite").csv("output_sparksql")

print("✅ Resultado salvo em 'output_sparksql/'")
