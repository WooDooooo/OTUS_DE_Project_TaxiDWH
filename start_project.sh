#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

ENV_FILE=".env"
if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE=".env.example"
fi

BUILD_FLAG=""
if [[ "${1:-}" == "--build" ]]; then
  BUILD_FLAG="--build"
fi

if ! docker network inspect nyc_data_net >/dev/null 2>&1; then
  echo "Создание сети Docker nyc_data_net..."
  docker network create nyc_data_net >/dev/null
fi

# Для чистого клона заранее готовим bind mount-каталоги на хосте,
# которые ожидают сервисы.
mkdir -p \
  airflow/logs \
  airflow/logs/scheduler \
  airflow/logs/dag_processor_manager \
  dbt_project

# Airflow и Superset внутри контейнеров работают не от uid/gid текущего
# пользователя хоста, поэтому открываем только корневые точки монтирования.
chmod 777 \
  airflow/logs \
  airflow/logs/scheduler \
  airflow/logs/dag_processor_manager \
  dbt_project || true

echo "Проверка локальных JAR-файлов Spark..."
"$ROOT_DIR/scripts/download_spark_jars.sh"

echo "Запуск этапа 1: MinIO"
docker compose --env-file "$ENV_FILE" -f docker/stage1_s3_minio.yml up -d $BUILD_FLAG

echo "Запуск этапа 2: Spark"
docker compose -f docker/stage2_spark.yml up -d $BUILD_FLAG

echo "Запуск этапа 3: ClickHouse"
docker compose --env-file "$ENV_FILE" -f docker/stage3_clickhouse.yml up -d $BUILD_FLAG

echo "Запуск этапа 4: Airflow"
docker compose -f docker/stage4_airflow.yml up -d $BUILD_FLAG

echo "Запуск этапа 5: Superset"
docker compose --env-file "$ENV_FILE" -f docker/stage5_superset.yml up -d $BUILD_FLAG

cat <<'EOF'

Проект запущен.
API MinIO:        http://localhost:9010
Консоль MinIO:    http://localhost:9011
Интерфейс Spark Master:  http://localhost:8080
Интерфейс Spark Worker:  http://localhost:8081
HTTP ClickHouse:  http://localhost:8123
Интерфейс Airflow:       http://localhost:8082
Интерфейс Superset:      http://localhost:8088

Чтобы всё остановить:
./stop_project.sh
EOF
