from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# Этот DAG связывает уже готовые этапы пайплайна в одну batch-цепочку:
# 1. Скрипт загрузки переносит исходные из URL в MinIO bronze
# 2. Spark очищает bronze -> Minio silver/quarantine
# 3. Spark грузит Minio silver -> ClickHouse raw
# 4. dbt уже дальше строит слой staging / intermediate / mart

# Spark-задачи запускаются через `docker exec nyc_spark_master ...`.
# Поэтому в stage4 Airflow-контейнерам проброшен Docker socket и установлен Docker CLI. 


SPARK_SUBMIT_BASE = (
    "docker exec nyc_spark_master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--jars /opt/spark/extra_jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark/extra_jars/aws-java-sdk-bundle-1.12.367.jar"
)

SPARK_SUBMIT_WITH_JDBC = (
    "docker exec nyc_spark_master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--jars /opt/spark/extra_jars/clickhouse-jdbc-0.6.0-shaded.jar,"
    "/opt/spark/extra_jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark/extra_jars/aws-java-sdk-bundle-1.12.367.jar "
    "--driver-class-path /opt/spark/extra_jars/clickhouse-jdbc-0.6.0-shaded.jar"
)


with DAG(
    dag_id="nyc_taxi_pipeline",
    description="NYC Taxi batch pipeline: source -> MinIO -> Spark -> ClickHouse -> dbt",
    start_date=datetime(2026, 3, 22),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "spark", "clickhouse", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")

    load_raw_to_bronze = BashOperator(
        task_id="load_raw_to_bronze",
        bash_command=(
            "python /opt/airflow/scripts/load_raw_to_bronze.py "
            "--endpoint http://minio:9000"
        ),
    )

    clean_trips_in_s3 = BashOperator(
        task_id="clean_trips_in_s3",
        bash_command=(
            f"{SPARK_SUBMIT_BASE} "
            "/opt/spark/jobs/clean_trips_in_s3.py"
        ),
    )

    clean_zones_in_s3 = BashOperator(
        task_id="clean_zones_in_s3",
        bash_command=(
            f"{SPARK_SUBMIT_BASE} "
            "/opt/spark/jobs/clean_zones_in_s3.py"
        ),
    )

    ensure_raw_schema = BashOperator(
        task_id="ensure_raw_schema",
        bash_command=(
            "docker exec nyc_clickhouse bash -lc "
            "'clickhouse-client --user admin --password admin12345 "
            "--multiquery < /docker-entrypoint-initdb.d/01_raw_tables.sql'"
        ),
    )

    truncate_raw_tables = BashOperator(
        task_id="truncate_raw_tables",
        bash_command=(
            "docker exec nyc_clickhouse clickhouse-client "
            "--user admin --password admin12345 "
            "--query \"TRUNCATE TABLE IF EXISTS nyc_taxi.raw_trips; "
            "TRUNCATE TABLE IF EXISTS nyc_taxi.raw_zones;\" --multiquery"
        ),
    )

    load_raw_trips_to_clickhouse = BashOperator(
        task_id="load_raw_trips_to_clickhouse",
        bash_command=(
            f"{SPARK_SUBMIT_WITH_JDBC} "
            "/opt/spark/jobs/load_raw_trips_to_clickhouse.py"
        ),
    )

    load_raw_zones_to_clickhouse = BashOperator(
        task_id="load_raw_zones_to_clickhouse",
        bash_command=(
            f"{SPARK_SUBMIT_WITH_JDBC} "
            "/opt/spark/jobs/load_raw_zones_to_clickhouse.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --project-dir /opt/airflow/dbt_project "
            "--profiles-dir /opt/airflow/dbt_project"
        ),
    )

    finish = EmptyOperator(task_id="finish")

    start >> load_raw_to_bronze >> [clean_trips_in_s3, clean_zones_in_s3]
    [clean_trips_in_s3, clean_zones_in_s3] >> ensure_raw_schema >> truncate_raw_tables
    truncate_raw_tables >> load_raw_trips_to_clickhouse
    truncate_raw_tables >> load_raw_zones_to_clickhouse
    [load_raw_trips_to_clickhouse, load_raw_zones_to_clickhouse] >> dbt_run >> finish
