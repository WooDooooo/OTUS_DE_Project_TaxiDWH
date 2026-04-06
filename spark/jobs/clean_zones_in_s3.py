# =============================================================================
# clean_zones_in_s3.py
# Пакетная Spark-задача: bronze -> silver (справочник зон такси)
#
# Что делает:
#   1. Читает taxi_zone_lookup.csv из бакета bronze
#   2. Приводит типы к целевой схеме
#   3. Отделяет невалидные записи в quarantine
#   4. Пишет очищенные данные в silver
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

BRONZE_INPUT_PATH      = "s3a://bronze/taxi_zone_lookup.csv"
SILVER_OUTPUT_PATH     = "s3a://silver/zones"
QUARANTINE_OUTPUT_PATH = "s3a://quarantine/zones"

# Целевая схема — читаем CSV строго, без infer schema.
# Если поле не приводится к типу, Spark подставит null, и запись уйдёт в карантин.
ZONES_SCHEMA = StructType([
    StructField("LocationID",   IntegerType(), True),
    StructField("Borough",      StringType(),  True),
    StructField("Zone",         StringType(),  True),
    StructField("service_zone", StringType(),  True),
])

spark = (
    SparkSession.builder
    .appName("nyc_taxi_clean_zones")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(">>> Читаем taxi_zone_lookup.csv из bronze...")
raw_zones_df = (
    spark.read
    .option("header", "true")
    .schema(ZONES_SCHEMA)
    .csv(BRONZE_INPUT_PATH)
)
print(f">>> Прочитано строк: {raw_zones_df.count()}")

# Невалидная запись — нарушает хотя бы одно правило.
invalid_records_condition = (
    F.col("LocationID").isNull()                   |  # нет ID зоны
    (F.col("LocationID") <= 0)                     |  # некорректный ID
    F.col("Borough").isNull()                      |  # нет боро
    (F.trim(F.col("Borough"))      == F.lit(""))   |  # пустое боро
    F.col("Zone").isNull()                         |  # нет названия зоны
    (F.trim(F.col("Zone"))         == F.lit(""))   |  # пустое название
    F.col("service_zone").isNull()                 |  # нет типа зоны
    (F.trim(F.col("service_zone")) == F.lit(""))      # пустой тип зоны
)

quarantine_zones_df = raw_zones_df.filter(invalid_records_condition)
clean_zones_df      = raw_zones_df.filter(~invalid_records_condition)

print(f">>> Чистых записей: {clean_zones_df.count()}")
print(f">>> В карантине:    {quarantine_zones_df.count()}")

print(">>> Пишем чистые данные в silver...")
(
    clean_zones_df
    .write
    .mode("overwrite")
    .parquet(SILVER_OUTPUT_PATH)
)

print(">>> Пишем карантинные записи в quarantine...")
(
    quarantine_zones_df
    .write
    .mode("overwrite")
    .parquet(QUARANTINE_OUTPUT_PATH)
)

print(">>> Готово.")
spark.stop()
