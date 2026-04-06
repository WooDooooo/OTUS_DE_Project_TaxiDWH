
-- 01_raw_tables.sql
-- Слой raw в ClickHouse — сюда Spark грузит данные из Minio silver.
-- Эти таблицы dbt не трогает напрямую — только читает.
-- Моделирование поверх них делает dbt (staging -> marts).


CREATE DATABASE IF NOT EXISTS nyc_taxi;

CREATE TABLE IF NOT EXISTS nyc_taxi.raw_trips
(
    VendorID              Int64,
    tpep_pickup_datetime  DateTime64(6),
    tpep_dropoff_datetime DateTime64(6),
    passenger_count       Nullable(Int32),
    trip_distance         Float64,
    RatecodeID            Nullable(Int32),
    store_and_fwd_flag    String,
    PULocationID          Int64,
    DOLocationID          Int64,
    payment_type          Int64,
    fare_amount           Float64,
    extra                 Float64,
    mta_tax               Float64,
    tip_amount            Float64,
    tolls_amount          Float64,
    improvement_surcharge Float64,
    total_amount          Float64,
    congestion_surcharge  Nullable(Float64),
    airport_fee           Nullable(Float64)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(tpep_pickup_datetime)  -- партиция по месяцу
ORDER BY (tpep_pickup_datetime, PULocationID) -- сортировка по времени и зоне
;

-- -----------------------------------------------------------------------------
-- Справочник зон
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS nyc_taxi.raw_zones
(
    LocationID   Int32,
    Borough      String,
    Zone         String,
    service_zone String
)
ENGINE = MergeTree()
ORDER BY LocationID
;
