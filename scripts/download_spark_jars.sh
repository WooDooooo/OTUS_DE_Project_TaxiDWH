#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JARS_DIR="$ROOT_DIR/spark/jars"

mkdir -p "$JARS_DIR"

download_if_missing() {
  local filename="$1"
  local url="$2"
  local target="$JARS_DIR/$filename"

  if [[ -f "$target" ]]; then
    echo "JAR уже существует: $filename"
    return 0
  fi

  echo "Скачивание JAR: $filename"
  curl -L --fail --show-error --silent "$url" -o "$target"
}

download_if_missing \
  "aws-java-sdk-bundle-1.12.367.jar" \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar"

download_if_missing \
  "hadoop-aws-3.3.4.jar" \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

download_if_missing \
  "clickhouse-jdbc-0.6.0-shaded.jar" \
  "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-shaded.jar"

echo "Все JAR-файлы Spark готовы."
