# Организация DWH службы такси NYC

Учебный проект-стенд по курсу "OTUS Data Engineer" 2025-2026 (автор Калетник А.В.) на данных `NYC TLC Yellow Taxi (Q1 2023)`.

Общая идея - строим пайплайн от загрузки данных из источника в виде parquet до построения дашборда по gold-витрине:

Source URLs -> MinIO (bronze) -> Spark (MinIO silver/quarantine) -> ClickHouse (raw) -> dbt (stg/int/mart) -> Superset

Инфраструктура поднимается через Docker Compose, а pipeline запускается вручную из Airflow.

Структура проекта

- `docker/stage1_s3_minio.yml`
  MinIO для слоёв `bronze`, `silver`, `quarantine`.

- `docker/stage2_spark.yml`
  Spark master/worker для очистки и загрузки данных.

- `docker/stage3_clickhouse.yml`
  ClickHouse с raw-слоем.

- `docker/stage4_airflow.yml`
  Airflow и metadata Postgres для оркестрации.

- `docker/stage5_superset.yml`
  Superset для BI.

- `airflow/dags/nyc_taxi_pipeline.py`
  Основной DAG: загрузка исходных данных, очистка, загрузка в ClickHouse, `dbt run`.

- `spark/jobs/clean_trips_in_s3.py`
  Очистка и валидация поездок.

- `spark/jobs/clean_zones_in_s3.py`
  Очистка и валидация справочника зон.

- `spark/jobs/load_raw_trips_to_clickhouse.py`
  Загрузка `silver/trips` в `raw_trips`.

- `spark/jobs/load_raw_zones_to_clickhouse.py`
  Загрузка `silver/zones` в `raw_zones`.

- `scripts/load_raw_to_bronze.py`
  Загрузка исходных файлов в `bronze`.

- `clickhouse/init/01_raw_tables.sql`
  Создание базы `nyc_taxi` и raw-таблиц.

- `dbt_project/models/staging/stg_trips.sql`
  Слой staging для поездок.

- `dbt_project/models/staging/stg_zones.sql`
  Слой staging для зон.

- `dbt_project/models/intermediate/int_trips_with_zones.sql`
  Обогащение поездок зонами.

- `dbt_project/models/marts/mart_revenue_by_borough.sql`
  Витрина для BI по borough.

- `superset/superset_config.py`
  Конфигурация Superset.

Слои данных: 

- `bronze`
  Сырые исходные файлы в S3

- `silver`
  Очищенные и провалидированные данные в S3 

- `quarantine`
  Ошибочные и аномальные записи в S3 - для дальнейшего разбора

- `raw`
  Таблицы ClickHouse после Spark-загрузки из silver S3

- `stg / int / mart`
  dbt-слои в ClickHouse для анлитики и  BI.

Как запускать и останавливать:
./start_project.sh
./stop_project.sh

Проект запускается поэтапно:

1. `Stage 1` - объектное хранилище MinIO
2. `Stage 2` - кластер Spark
3. `Stage 3` - ClickHouse
4. `Stage 4` - Airflow + metadata Postgres + dbt
5. `Stage 5` - Superset + Redis

Скрипт ./stop_project.sh останавливает их в обратном порядке, чтобы зависимые сервисы завершались раньше базовых.



После запуска инфраструктуры нужно запустить вручную из интерфейса Airflow DAG `nyc_taxi_pipeline`.

Интерфейсы и сервисы

- Консоль MinIO: `http://localhost:9011`
- Интерфейс Spark: `http://localhost:8080`
- Интерфейс Airflow: `http://localhost:8082`
- Интерфейс Superset: `http://localhost:8088`
- HTTP ClickHouse: `http://localhost:8123`

Порты
- MinIO API: `9010`
- MinIO Console: `9011`
- Spark Master: `7077`
- Spark UI Master: `8080`
- Spark UI Worker: `8081`
- ClickHouse HTTP: `8123`
- ClickHouse TCP: `9009`
- Airflow Webserver: `8082`
- Superset: `8088`

Локальные зависимости Spark
- JAR-файлы в `spark/jars/` не включены в репозиторий.
- Это локальные runtime-зависимости для S3A и JDBC.
