# =============================================================================
# clean_trips.py
# Пакетная Spark-задача: bronze -> silver
#
# Что делает:
#   1. Читает raw Parquet из бакета bronze
#   2. Выравнивает схему между месяцами
#   3. Отделяет невалидные записи в quarantine
#   4. Пишет очищенные данные в silver
# =============================================================================

from functools import reduce
from pathlib import PurePosixPath

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_INPUT_PATH     = "s3a://bronze/*.parquet"
SILVER_OUTPUT_PATH    = "s3a://silver/trips"
QUARANTINE_OUTPUT_PATH = "s3a://quarantine/trips"

# Целевая схема bronze-слоя после выравнивания типов.
# Нужна потому, что parquet-файлы за разные месяцы имеют дрейф схемы:
# отличаются типы полей и даже регистр имени airport_fee/Airport_fee.
RAW_TRIPS_SCHEMA = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
])


def extract_source_month(file_path):
    filename = PurePosixPath(file_path).name
    # Ожидаем формат имени yellow_tripdata_YYYY-MM.parquet.
    prefix = "yellow_tripdata_"
    suffix = ".parquet"

    if filename.startswith(prefix):
        filename = filename[len(prefix):]
    if filename.endswith(suffix):
        filename = filename[:-len(suffix)]

    return filename


def normalize_raw_trips(df, file_path):
    # Сначала приводим колонку airport_fee к одному имени.
    if "Airport_fee" in df.columns and "airport_fee" not in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")

    # Затем гарантируем, что у каждого месячного файла будет один и тот же
    # набор колонок и одинаковые типы.
    for field in RAW_TRIPS_SCHEMA:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

    return (
        df.select([field.name for field in RAW_TRIPS_SCHEMA])
        .withColumn("source_month", F.lit(extract_source_month(file_path)))
    )


def load_bronze_trips(spark):
    # Читаем файлы по одному, а не всей маской сразу.
    # Так Spark не падает на несовместимых физических parquet-схемах.
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(BRONZE_INPUT_PATH)
    fs = path.getFileSystem(hadoop_conf)
    statuses = fs.globStatus(path)

    if not statuses:
        raise FileNotFoundError(f"Не найдено parquet-файлов по маске {BRONZE_INPUT_PATH}")

    bronze_files = sorted(status.getPath().toString() for status in statuses)
    normalized_dfs = [
        normalize_raw_trips(spark.read.parquet(file_path), file_path)
        for file_path in bronze_files
    ]

    return reduce(lambda left, right: left.unionByName(right), normalized_dfs)

# S3A-настройки берутся из spark/conf/spark-defaults.conf.
# Здесь оставляем только настройки, относящиеся к самому Spark job.
spark = (
    SparkSession.builder
    .appName("nyc_taxi_clean_trips")
    .master("spark://spark-master:7077")
    # На смешанной parquet-схеме vectorized reader часто ломается.
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(">>> Читаем данные из bronze...")
raw_trips_df = load_bronze_trips(spark)
print(f">>> Прочитано строк: {raw_trips_df.count()}")

# После объединения месячных файлов приводим бизнес-поля к удобным типам
# и сразу вычисляем partition_month для дальнейшей валидации.
prepared_trips_df = (
    raw_trips_df
    .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
    .withColumn("RatecodeID",      F.col("RatecodeID").cast(IntegerType()))
    .withColumn("payment_type",    F.col("payment_type").cast(IntegerType()))
    # Стабильный технический ключ поездки.
    .withColumn(
        "trip_id",
        F.sha2(
            F.concat_ws("|",
                F.col("VendorID"),
                F.col("tpep_pickup_datetime").cast("string"),
                F.col("tpep_dropoff_datetime").cast("string"),
                F.col("PULocationID"),
                F.col("DOLocationID"),
                F.col("total_amount")
            ),
            256
        )
    )
    # Партиция для удобной записи и чтения по месяцам.
    .withColumn("partition_month", F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM"))
)

# Плохая запись - та, которая нарушает хотя бы одно базовое правило качества.
# Дополнительно проверяем, что поездка относится к тому же year-month,
# который заявлен в имени исходного parquet-файла.
invalid_records_condition = (
    (F.col("passenger_count").isNull())        |  # нет пассажиров
    (F.col("passenger_count") <= 0)            |  # отрицательное число пассажиров
    (F.col("passenger_count") > 9)             |  # слишком много пассажиров
    (F.col("trip_distance") <= 0)              |  # нулевое расстояние
    (F.col("fare_amount") <= 0)                |  # нулевой тариф
    (F.col("tpep_pickup_datetime").isNull())   |  # нет времени посадки
    (F.col("tpep_dropoff_datetime").isNull())  |  # нет времени высадки
    (F.col("tpep_pickup_datetime") >=          # высадка раньше посадки
     F.col("tpep_dropoff_datetime"))           |
    (F.col("partition_month") != F.col("source_month"))  # не тот месяц файла
)

quarantine_trips_df = prepared_trips_df.filter(invalid_records_condition)
clean_trips_df      = prepared_trips_df.filter(~invalid_records_condition)

print(f">>> Чистых записей:   {clean_trips_df.count()}")
print(f">>> В карантине:      {quarantine_trips_df.count()}")

clean_trips_df = clean_trips_df.drop("source_month")
quarantine_trips_df = quarantine_trips_df.drop("source_month")

print(">>> Пишем чистые данные в silver...")
(
    clean_trips_df
    .write
    .mode("overwrite")
    .partitionBy("partition_month")
    .parquet(SILVER_OUTPUT_PATH)
)

print(">>> Пишем карантинные записи в quarantine...")
(
    quarantine_trips_df
    .write
    .mode("overwrite")
    .parquet(QUARANTINE_OUTPUT_PATH)
)

print(">>> Готово.")
spark.stop()
