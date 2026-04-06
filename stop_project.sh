#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

ENV_FILE=".env"
if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE=".env.example"
fi

echo "Остановка этапа 5: Superset"
docker compose --env-file "$ENV_FILE" -f docker/stage5_superset.yml down

echo "Остановка этапа 4: Airflow"
docker compose -f docker/stage4_airflow.yml down

echo "Остановка этапа 3: ClickHouse"
docker compose --env-file "$ENV_FILE" -f docker/stage3_clickhouse.yml down

echo "Остановка этапа 2: Spark"
docker compose -f docker/stage2_spark.yml down

echo "Остановка этапа 1: MinIO"
docker compose --env-file "$ENV_FILE" -f docker/stage1_s3_minio.yml down

echo "Проект остановлен."
