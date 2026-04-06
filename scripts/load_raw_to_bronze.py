#!/usr/bin/env python3

"""Загружает исходные файлы NYC TLC напрямую в слой bronze в MinIO.
Файлы приходят с кривой структурой и датами поэтому чистка обязательна

1. Получить сырые входные данные из внешнего источника по HTTP.
2. Дождаться готовности MinIO как S3-совместимого хранилища.
3. Создать бакет bronze, если он ещё не существует.
4. Загрузить в bronze parquet-файлы поездок за Q1 2023 и справочник зон.

Как работает:
- читает S3-учётные данные из окружения или из файла .env;
- использует HTTP-запросы с AWS Signature V4 для работы с MinIO;
- проверяет, существует ли объект в бакете, чтобы не загружать его повторно;
- по флагу --force может перезалить объекты принудительно.

Роль в пайплайне:
- это входная точка ingestion-слоя;
- после него Spark читает сырые данные из bronze и строит silver/quarantine;
- сам скрипт ничего не очищает и не преобразует, а только доставляет raw-данные
  из внешнего источника в объектное хранилище проекта.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import quote, urlparse

import requests


NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MONTHS_TO_DOWNLOAD = ("01", "02", "03")
DEFAULT_BUCKET = "bronze"
DEFAULT_REGION = "us-east-1"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


@dataclass(frozen=True)
class SourceObject:
    object_name: str
    source_url: str


def load_env_file(env_path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not env_path.exists():
        return env

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip("'").strip('"')
    return env


def resolve_settings() -> tuple[str, str]:
    env_values = load_env_file(Path(".env"))
    access_key = os.environ.get("S3_ACCESS_KEY") or env_values.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY") or env_values.get("S3_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError("S3_ACCESS_KEY / S3_SECRET_KEY не заданы в окружении или в .env")

    return access_key, secret_key


def canonical_uri(path: str) -> str:
    return quote(path if path.startswith("/") else f"/{path}", safe="/-_.~")


def sign_request(
    *,
    method: str,
    url: str,
    access_key: str,
    secret_key: str,
    payload_hash: str,
    extra_headers: dict[str, str] | None = None,
    region: str = DEFAULT_REGION,
    service: str = "s3",
) -> dict[str, str]:
    parsed = urlparse(url)
    host = parsed.netloc
    timestamp = datetime.now(timezone.utc)
    amz_date = timestamp.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = timestamp.strftime("%Y%m%d")

    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if extra_headers:
        headers.update({k.lower(): v for k, v in extra_headers.items()})

    canonical_headers = "".join(f"{key}:{headers[key].strip()}\n" for key in sorted(headers))
    signed_headers = ";".join(sorted(headers))
    canonical_request = "\n".join(
        [
            method,
            canonical_uri(parsed.path or "/"),
            parsed.query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )

    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )

    def _sign(key: bytes, message: str) -> bytes:
        return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()

    k_date = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        "AWS4-HMAC-SHA256 "
        f"Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    signed = {
        "Authorization": authorization,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if extra_headers:
        signed.update(extra_headers)
    return signed


def request_with_sigv4(
    *,
    method: str,
    url: str,
    access_key: str,
    secret_key: str,
    payload_hash: str,
    extra_headers: dict[str, str] | None = None,
    data=None,
    timeout: int = 30,
) -> requests.Response:
    headers = sign_request(
        method=method,
        url=url,
        access_key=access_key,
        secret_key=secret_key,
        payload_hash=payload_hash,
        extra_headers=extra_headers,
    )
    return requests.request(method, url, headers=headers, data=data, timeout=timeout)


def wait_for_minio(endpoint: str, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    health_url = f"{endpoint.rstrip('/')}/minio/health/live"

    while time.time() < deadline:
        try:
            response = requests.get(health_url, timeout=5)
            if response.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)

    raise TimeoutError(f"MinIO не стал доступен в течение {timeout_seconds} секунд")


def ensure_bucket(endpoint: str, bucket: str, access_key: str, secret_key: str) -> None:
    bucket_url = f"{endpoint.rstrip('/')}/{bucket}"
    response = request_with_sigv4(
        method="PUT",
        url=bucket_url,
        access_key=access_key,
        secret_key=secret_key,
        payload_hash=EMPTY_SHA256,
        timeout=30,
    )

    if response.status_code in (200, 204, 409):
        return

    raise RuntimeError(
        f"Не удалось гарантировать наличие бакета {bucket}: {response.status_code} {response.text}"
    )


def object_exists(endpoint: str, bucket: str, object_name: str, access_key: str, secret_key: str) -> bool:
    object_url = f"{endpoint.rstrip('/')}/{bucket}/{quote(object_name, safe='/-_.~')}"
    response = request_with_sigv4(
        method="HEAD",
        url=object_url,
        access_key=access_key,
        secret_key=secret_key,
        payload_hash=EMPTY_SHA256,
        timeout=30,
    )

    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False

    raise RuntimeError(
        f"Не удалось проверить объект {bucket}/{object_name}: {response.status_code} {response.text}"
    )


def download_to_tempfile(source_url: str, object_name: str) -> tuple[Path, str]:
    suffix = Path(object_name).suffix or ".bin"
    with requests.get(source_url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            sha256 = hashlib.sha256()
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                tmp.write(chunk)
                sha256.update(chunk)
            return Path(tmp.name), sha256.hexdigest()


def upload_file(
    endpoint: str,
    bucket: str,
    object_name: str,
    file_path: Path,
    payload_hash: str,
    access_key: str,
    secret_key: str,
) -> None:
    object_url = f"{endpoint.rstrip('/')}/{bucket}/{quote(object_name, safe='/-_.~')}"
    headers = {
        "Content-Length": str(file_path.stat().st_size),
        "Content-Type": "application/octet-stream",
    }
    with file_path.open("rb") as handle:
        response = request_with_sigv4(
            method="PUT",
            url=object_url,
            access_key=access_key,
            secret_key=secret_key,
            payload_hash=payload_hash,
            extra_headers=headers,
            data=handle,
            timeout=300,
        )

    if response.status_code not in (200, 204):
        raise RuntimeError(
            f"Не удалось загрузить {bucket}/{object_name}: {response.status_code} {response.text}"
        )


def source_objects() -> Iterable[SourceObject]:
    for month in MONTHS_TO_DOWNLOAD:
        filename = f"yellow_tripdata_2023-{month}.parquet"
        yield SourceObject(
            object_name=filename,
            source_url=f"{NYC_TLC_BASE_URL}/{filename}",
        )

    yield SourceObject(
        object_name="taxi_zone_lookup.csv",
        source_url=TAXI_ZONE_URL,
    )


def ingest(endpoint: str, bucket: str, force: bool) -> None:
    access_key, secret_key = resolve_settings()
    wait_for_minio(endpoint)
    ensure_bucket(endpoint, bucket, access_key, secret_key)

    for src in source_objects():
        if not force and object_exists(endpoint, bucket, src.object_name, access_key, secret_key):
            print(f"[skip] {bucket}/{src.object_name} already exists")
            continue

        print(f"[download] {src.source_url}")
        temp_path, payload_hash = download_to_tempfile(src.source_url, src.object_name)

        try:
            print(f"[upload] s3://{bucket}/{src.object_name}")
            upload_file(
                endpoint=endpoint,
                bucket=bucket,
                object_name=src.object_name,
                file_path=temp_path,
                payload_hash=payload_hash,
                access_key=access_key,
                secret_key=secret_key,
            )
        finally:
            temp_path.unlink(missing_ok=True)

    print("[done] слой bronze готов")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Загружает исходные файлы NYC TLC напрямую в MinIO bronze")
    parser.add_argument("--endpoint", default="http://localhost:9010", help="S3-эндпоинт MinIO, доступный с хоста")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="Целевой бакет MinIO для сырых исходных объектов")
    parser.add_argument("--force", action="store_true", help="Перезагрузить объекты, даже если они уже существуют")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    ingest(endpoint=args.endpoint, bucket=args.bucket, force=args.force)


if __name__ == "__main__":
    main()
