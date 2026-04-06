# =============================================================================
# load_raw_zones_to_clickhouse.py
# Пакетная Spark-задача: silver -> ClickHouse raw_zones
#
# Что делает:
#   1. Читает очищенные зоны из бакета silver (Parquet, после clean_zones_in_s3.py)
#   2. Грузит в таблицу nyc_taxi.raw_zones через JDBC
# =============================================================================

from pyspark.sql import SparkSession

SILVER_INPUT_PATH       = "s3a://silver/zones"
CLICKHOUSE_JDBC_URL     = "jdbc:ch://clickhouse:8123/nyc_taxi"
CLICKHOUSE_TARGET_TABLE = "raw_zones"
CLICKHOUSE_USER         = "admin"
CLICKHOUSE_PASSWORD     = "admin12345"

# S3A-настройки берутся из spark-defaults.conf
spark = (
    SparkSession.builder
    .appName("nyc_taxi_load_raw_zones")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(">>> Читаем зоны из silver...")
zones_df = spark.read.parquet(SILVER_INPUT_PATH)
print(f">>> Прочитано строк: {zones_df.count()}")

print(">>> Грузим в ClickHouse...")
(
    zones_df
    .write
    .format("jdbc")
    .option("url",      CLICKHOUSE_JDBC_URL)
    .option("dbtable",  CLICKHOUSE_TARGET_TABLE)
    .option("user",     CLICKHOUSE_USER)
    .option("password", CLICKHOUSE_PASSWORD)
    .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver")
    .mode("append")
    .save()
)

print(">>> Готово.")
spark.stop()
