"""
### Silver Layer: HH.ru Vacancies Processor (DuckDB Engine)

DAG обрабатывает сырые данные (Bronze) за конкретную дату и формирует Silver слой.

Движок: **DuckDB**
Преимущества перед Polars в этом кейсе:
1. **Out-of-Core Processing:** Умеет сбрасывать данные на диск при нехватке RAM (решение OOM).
2. **Robust JSON Reader:** read_json_auto отлично справляется с вложенными структурами и schema evolution.
3. **SQL Expressiveness:** Удобный синтаксис (EXCLUDE, QUALIFY) для трансформаций.

Логика:
1. Читает ВСЕ файлы из Bronze за переданную дату.
2. Дедуплицирует (по id) используя Window Functions.
3. Конвертирует типы и даты.
4. Пишет результат в локальный Parquet -> Грузит в S3.
"""

import logging
import os
import shutil
import uuid
from datetime import datetime, timedelta

import duckdb
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

# --- КОНФИГУРАЦИЯ ---
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"

logger = logging.getLogger("airflow.task")

# --- SCHEMA DEFINITION (DuckDB Types) ---
# Мы перевели Polars типы в SQL типы DuckDB.
# STRUCT(...) - вложенная структура
# TYPE[] - массив (LIST)
# "from" в кавычках, т.к. это зарезервированное слово SQL
TARGET_SCHEMA = {
    "id": "VARCHAR",
    "name": "VARCHAR",
    "description": "VARCHAR",
    "branded_description": "VARCHAR",
    "key_skills": "STRUCT(name VARCHAR)[]",  # List of structs
    "schedule": "STRUCT(id VARCHAR, name VARCHAR)",
    "accept_handicapped": "BOOLEAN",
    "accept_kids": "BOOLEAN",
    "experience": "STRUCT(id VARCHAR, name VARCHAR)",
    "address": "STRUCT(city VARCHAR, street VARCHAR, building VARCHAR, lat DOUBLE, lng DOUBLE, description VARCHAR, raw VARCHAR, metro STRUCT(station_name VARCHAR, line_name VARCHAR, station_id VARCHAR, line_id VARCHAR, lat DOUBLE, lng DOUBLE), metro_stations STRUCT(station_name VARCHAR, line_name VARCHAR, station_id VARCHAR, line_id VARCHAR, lat DOUBLE, lng DOUBLE)[], id VARCHAR)",
    "employer": 'STRUCT(id VARCHAR, name VARCHAR, url VARCHAR, alternate_url VARCHAR, logo_urls STRUCT(original VARCHAR, "90" VARCHAR, "240" VARCHAR), vacancies_url VARCHAR, country_id BIGINT, accredited_it_employer BOOLEAN, trusted BOOLEAN)',
    "salary": 'STRUCT("from" BIGINT, "to" BIGINT, currency VARCHAR, gross BOOLEAN)',
    "archived": "BOOLEAN",
    "response_url": "VARCHAR",
    "specializations": "STRUCT(id VARCHAR, name VARCHAR, profarea_id VARCHAR, profarea_name VARCHAR)[]",
    "professional_roles": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "driver_license_types": "STRUCT(id VARCHAR)[]",
    "contacts": "STRUCT(name VARCHAR, email VARCHAR, phones STRUCT(country VARCHAR, city VARCHAR, number VARCHAR, comment VARCHAR)[])",
    "created_at": "VARCHAR",  # Будет скастовано в timestamp позже
    "published_at": "VARCHAR",
    "response_letter_required": "BOOLEAN",
    "type": "STRUCT(id VARCHAR, name VARCHAR)",
    "has_test": "BOOLEAN",
    "test": "STRUCT(required BOOLEAN)",
    "alternate_url": "VARCHAR",
    "working_days": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "working_time_intervals": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "working_time_modes": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "accept_temporary": "BOOLEAN",
    "languages": "STRUCT(id VARCHAR, name VARCHAR, level STRUCT(id VARCHAR, name VARCHAR))[]",
    # Проблемные колонки (теперь они жестко заданы)
    "immediate_redirect_url": "VARCHAR",
    "video_vacancy": "STRUCT(cover_picture STRUCT(resized_path VARCHAR, resized_width BIGINT, resized_height BIGINT), snippet_picture STRUCT(url VARCHAR), video STRUCT(upload_id VARCHAR, url VARCHAR), snippet_video STRUCT(upload_id VARCHAR, url VARCHAR), video_url VARCHAR, snippet_video_url VARCHAR, snippet_picture_url VARCHAR)",
    "brand_snippet": 'STRUCT(logo VARCHAR, logo_xs VARCHAR, logo_scalable STRUCT("default" STRUCT(width BIGINT, height BIGINT, url VARCHAR), xs STRUCT(width BIGINT, height BIGINT, url VARCHAR)), picture VARCHAR, picture_xs VARCHAR, picture_scalable STRUCT("default" STRUCT(width BIGINT, height BIGINT, url VARCHAR), xs STRUCT(width BIGINT, height BIGINT, url VARCHAR)), background STRUCT(color VARCHAR, gradient STRUCT(angle DOUBLE, color_list STRUCT(color VARCHAR, position DOUBLE)[])))',
    # Остальные поля для полноты (можно добавлять по мере необходимости)
    "is_adv_vacancy": "BOOLEAN",
    "internship": "BOOLEAN",
    "department": "STRUCT(id VARCHAR, name VARCHAR)",
    "show_contacts": "BOOLEAN",
    "apply_alternate_url": "VARCHAR",
    "work_format": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "relations": "VARCHAR[]",  # List of nulls usually
    "accept_incomplete_resumes": "BOOLEAN",
    "premium": "BOOLEAN",
    "employment": "STRUCT(id VARCHAR, name VARCHAR)",
    "employment_form": "STRUCT(id VARCHAR, name VARCHAR)",
    "adv_context": "VARCHAR",
    "sort_point_distance": "DOUBLE",
    "branding": "STRUCT(type VARCHAR, tariff VARCHAR)",
    "insider_interview": "STRUCT(id VARCHAR, url VARCHAR)",
    "fly_in_fly_out_duration": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "url": "VARCHAR",
    "salary_range": 'STRUCT("from" BIGINT, "to" BIGINT, currency VARCHAR, gross BOOLEAN, mode STRUCT(id VARCHAR, name VARCHAR), frequency STRUCT(id VARCHAR, name VARCHAR))',
    "work_schedule_by_days": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "show_logo_in_search": "BOOLEAN",
    "area": "STRUCT(id VARCHAR, name VARCHAR, url VARCHAR)",
    "snippet": "STRUCT(requirement VARCHAR, responsibility VARCHAR)",
    "night_shifts": "BOOLEAN",
    "working_hours": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "adv_response_url": "VARCHAR",
}


# --- ФУНКЦИИ ---
def setup_duckdb(db_path=":memory:", memory_limit="2GB"):
    """Инициализация DuckDB с поддержкой S3 (httpfs)."""
    con = duckdb.connect(db_path)

    # Установка расширений (обычно встроены, но на всякий случай)
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Настройка S3
    # Важно: path_style access нужен для MinIO
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT.replace('http://', '')}';")
    con.execute(f"SET s3_access_key_id='{ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")

    # Настройка памяти и потоков
    # Ограничиваем память, чтобы не получить OOM от OS. Остальное уйдет в /tmp
    con.execute(f"SET memory_limit='{memory_limit}';")
    # con.execute("SET threads=4;") # Можно настроить под воркер

    return con


def get_bronze_schema_columns(con, s3_path_glob):
    """
    Легкая проверка схемы (Drift Check).
    Читает 1 строку, чтобы понять, какие колонки видит DuckDB.
    """
    try:
        # DESCRIBE возвращает структуру таблицы
        # read_json_auto сам выведет схему
        query = f"DESCRIBE SELECT * FROM read_json_auto('{s3_path_glob}', ignore_errors=true) LIMIT 1"
        schema_info = con.execute(query).fetchall()
        columns = [row[0] for row in schema_info]
        return set(columns)
    except Exception as e:
        logger.warning(f"Could not infer schema: {e}")
        return set()


def calculate_dq_metrics(con, table_name="silver_view"):
    """Считает метрики качества прямо внутри DuckDB."""
    try:
        query = f"""
        SELECT
            count(*) as row_count,
            -- Проверяем заполненность (Completeness)
            count(salary) FILTER (WHERE salary IS NULL) / count(*)::DOUBLE as salary_null_rate,
            count(employer) FILTER (WHERE employer IS NULL) / count(*)::DOUBLE as employer_null_rate,

            -- Проверяем даты (Timeliness)
            min(published_at_utc) as min_date,
            max(published_at_utc) as max_date
        FROM {table_name}
        """
        metrics = con.execute(query).fetchone()
        logger.info("--- DQ METRICS (DuckDB) ---")
        logger.info(f"Rows: {metrics[0]}")
        logger.info(f"Salary Null Rate: {metrics[1]:.2%}")
        logger.info(f"Date Range: {metrics[3]} - {metrics[4]}")
        logger.info("---------------------------")
    except Exception as e:
        logger.error(f"DQ Metrics failed: {e}")


@task(task_id="process_bronze_to_silver")
def process_bronze_to_silver(logical_date=None):
    import pendulum

    # 1. Определяем target_date (Batch Date)
    msk_tz = pendulum.timezone("Europe/Moscow")
    run_date_msk = logical_date.in_timezone(msk_tz)
    target_date = run_date_msk.start_of("day").subtract(days=1)
    date_str = target_date.strftime("%Y-%m-%d")

    # Пути
    bronze_glob = f"s3://{BUCKET_BRONZE}/hh/vacancies/date={date_str}/*.jsonl.gz"

    # Hive-style partitioning path для S3
    partition_suffix = (
        f"year={target_date.year}/month={target_date.month}/day={target_date.day}"
    )
    silver_prefix = f"hh/vacancies/{partition_suffix}"

    # Временный локальный файл
    local_filename = f"part-{uuid.uuid4()}.parquet"
    local_path = f"/tmp/{local_filename}"

    logger.info(f"Processing Batch: {date_str}")
    logger.info(f"Source: {bronze_glob}")

    # Генерация строки колонок для DuckDB
    # Формат: {'col1': 'TYPE', 'col2': 'TYPE'}
    columns_definition = ", ".join([f"'{k}': '{v}'" for k, v in TARGET_SCHEMA.items()])
    columns_sql = f"{{{columns_definition}}}"

    # Очистка S3
    s3_hook = S3Hook(aws_conn_id="aws_default")
    if s3_hook.check_for_bucket(BUCKET_SILVER):
        keys = s3_hook.list_keys(BUCKET_SILVER, prefix=silver_prefix)
        if keys:
            logger.info(f"Cleaning {len(keys)} old files...")
            s3_hook.delete_objects(BUCKET_SILVER, keys)
    else:
        s3_hook.create_bucket(BUCKET_SILVER)

    con = setup_duckdb()

    try:
        logger.info("Executing DuckDB Transformation with Schema Enforcement...")

        # Основной запрос
        # read_json(..., columns={...}) - это ключ к успеху.
        # Он заставляет DuckDB искать конкретные поля. Если поля нет - ставит NULL.
        # Лишние поля игнорируются.
        transform_query = f"""
        COPY (
            WITH raw_data AS (
                SELECT *
                FROM read_json(
                    '{bronze_glob}',
                    columns={columns_sql}, -- Явное указание схемы
                    format='newline_delimited', -- Важно для NDJSON (JSONL)
                    ignore_errors=true,
                    filename=false
                )
            ),
            deduplicated AS (
                SELECT *
                FROM raw_data
                WHERE id IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY published_at DESC) = 1
            ),
            enriched AS (
                SELECT
                    * EXCLUDE (published_at, created_at),
                    (published_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP as published_at_utc,
                    (created_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP as created_at_utc,

                    date_diff('minute',
                              (published_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP,
                              published_at::TIMESTAMP
                    )::SMALLINT as published_at_offset,

                    date_diff('minute',
                              (created_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP,
                              created_at::TIMESTAMP
                    )::SMALLINT as created_at_offset
                FROM deduplicated
            )
            SELECT * FROM enriched
        ) TO '{local_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);
        """

        con.execute(transform_query)
        logger.info(f"Transformation complete. Saved to {local_path}")

        # DQ check (опционально, на готовом файле)
        con.execute(
            f"CREATE OR REPLACE VIEW metrics_view AS SELECT * FROM parquet_scan('{local_path}')"
        )
        calculate_dq_metrics(con, "metrics_view")

    except Exception as e:
        logger.error(f"DuckDB failed: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        raise e
    finally:
        con.close()

    # Upload
    if os.path.exists(local_path) and os.path.getsize(local_path) > 100:
        s3_key = f"{silver_prefix}/{local_filename}"
        logger.info(f"Uploading to {s3_key}...")
        s3_hook.load_file(local_path, s3_key, BUCKET_SILVER, replace=True)
        os.remove(local_path)
    else:
        logger.warning("File empty.")

    logger.info("Done.")


@dag(
    dag_id="silver_hh_processor",
    description="Transforms Bronze JSONL to Silver Parquet (DuckDB Engine)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # Защита от OOM, запускаем один DagRun в одно время
    tags=["silver", "hh", "etl", "duckdb"],
)
def silver_dag():
    process_bronze_to_silver()


silver_dag()
