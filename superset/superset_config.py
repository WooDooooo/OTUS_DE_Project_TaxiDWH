import os


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "dev-superset-secret-key")

# Для учебного локального проекта достаточно встроенной базы metadata в superset_home.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Redis используется как backend кэша и broker для фоновых задач Superset.
REDIS_HOST = "redis"
REDIS_PORT = 6379

RESULTS_BACKEND = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}

# Подсказка для ручного добавления ClickHouse в интерфейсе Superset:
# clickhousedb://admin:admin12345@clickhouse:8123/nyc_taxi
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "nyc_taxi")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin12345")

CLICKHOUSE_SQLALCHEMY_URI = (
    f"clickhousedb://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}"
    f"@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
)
