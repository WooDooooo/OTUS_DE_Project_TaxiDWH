# =============================================================================
# load_to_clickhouse.py
# Пакетная Spark-задача: silver -> ClickHouse raw_trips
#
# Что делает:
#   1. Читает очищенные Parquet из бакета silver
#   2. Грузит в таблицу nyc_taxi.raw_trips через JDBC
# =============================================================================

from pyspark.sql import SparkSession

SILVER_INPUT_PATH       = "s3a://silver/trips"
CLICKHOUSE_JDBC_URL     = "jdbc:ch://clickhouse:8123/nyc_taxi"
CLICKHOUSE_TARGET_TABLE = "raw_trips"
CLICKHOUSE_USER         = "admin"
CLICKHOUSE_PASSWORD     = "admin12345"

# S3A-настройки берутся из spark-defaults.conf
spark = (
    SparkSession.builder
    .appName("nyc_taxi_load_to_clickhouse")
    .master("spark://spark-master:7077")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(">>> Читаем данные из silver...")
silver_trips_df = spark.read.parquet(SILVER_INPUT_PATH)
print(f">>> Прочитано строк: {silver_trips_df.count()}")

# trip_id и partition_month — технические поля, которые мы добавили в silver,
# но в raw_trips их нет, поэтому убираем их перед загрузкой
trips_for_clickhouse_df = silver_trips_df.drop("trip_id", "partition_month")

print(">>> Грузим в ClickHouse...")
(
    trips_for_clickhouse_df
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
