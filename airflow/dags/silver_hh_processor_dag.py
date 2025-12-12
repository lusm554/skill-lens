"""
### Silver Layer: HH.ru Vacancies Processor (DuckDB Engine)

DAG обрабатывает сырые данные (Bronze) за конкретную дату и формирует Silver слой.

Движок: **DuckDB**
Изменения: Схема жестко синхронизирована с Polars Schema + добавлены UTC даты и оффсеты.
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
# Схема адаптирована под предоставленную Polars структуру.
# Удалены лишние поля (description, key_skills, etc).
# Добавлены сложные вложенные структуры (video_vacancy, brand_snippet).
TARGET_SCHEMA = {
    "id": "VARCHAR",
    "name": "VARCHAR",
    # "description": "VARCHAR",  <-- REMOVED (Not in Polars Schema)
    # "key_skills": "STRUCT(name VARCHAR)[]", <-- REMOVED
    "schedule": "STRUCT(id VARCHAR, name VARCHAR)",
    "working_time_modes": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "working_time_intervals": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "working_days": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "working_hours": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "work_schedule_by_days": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "fly_in_fly_out_duration": "STRUCT(id VARCHAR, name VARCHAR)[]",
    # Ad-related / Booleans
    "is_adv_vacancy": "BOOLEAN",
    "internship": "BOOLEAN",
    "accept_temporary": "BOOLEAN",
    "accept_incomplete_resumes": "BOOLEAN",
    "premium": "BOOLEAN",
    "has_test": "BOOLEAN",
    "show_contacts": "BOOLEAN",
    "response_letter_required": "BOOLEAN",
    "show_logo_in_search": "BOOLEAN",
    "archived": "BOOLEAN",
    "night_shifts": "BOOLEAN",
    # Dates (Source strings)
    "created_at": "VARCHAR",
    "published_at": "VARCHAR",
    # URLs
    "url": "VARCHAR",
    "alternate_url": "VARCHAR",
    "apply_alternate_url": "VARCHAR",
    "response_url": "VARCHAR",
    "adv_response_url": "VARCHAR",
    "immediate_redirect_url": "VARCHAR",
    # Structs
    "salary": 'STRUCT("from" BIGINT, "to" BIGINT, currency VARCHAR, gross BOOLEAN)',
    "salary_range": 'STRUCT("from" BIGINT, "to" BIGINT, currency VARCHAR, gross BOOLEAN, mode STRUCT(id VARCHAR, name VARCHAR), frequency STRUCT(id VARCHAR, name VARCHAR))',
    "employer": 'STRUCT(id VARCHAR, name VARCHAR, url VARCHAR, alternate_url VARCHAR, logo_urls STRUCT(original VARCHAR, "90" VARCHAR, "240" VARCHAR), vacancies_url VARCHAR, country_id BIGINT, accredited_it_employer BOOLEAN, trusted BOOLEAN)',
    "department": "STRUCT(id VARCHAR, name VARCHAR)",
    "type": "STRUCT(id VARCHAR, name VARCHAR)",
    "employment": "STRUCT(id VARCHAR, name VARCHAR)",
    "employment_form": "STRUCT(id VARCHAR, name VARCHAR)",
    "experience": "STRUCT(id VARCHAR, name VARCHAR)",
    "area": "STRUCT(id VARCHAR, name VARCHAR, url VARCHAR)",
    # Address (Complex)
    "address": "STRUCT(city VARCHAR, street VARCHAR, building VARCHAR, lat DOUBLE, lng DOUBLE, description VARCHAR, raw VARCHAR, metro STRUCT(station_name VARCHAR, line_name VARCHAR, station_id VARCHAR, line_id VARCHAR, lat DOUBLE, lng DOUBLE), metro_stations STRUCT(station_name VARCHAR, line_name VARCHAR, station_id VARCHAR, line_id VARCHAR, lat DOUBLE, lng DOUBLE)[], id VARCHAR)",
    # Lists
    "work_format": "STRUCT(id VARCHAR, name VARCHAR)[]",
    "relations": "VARCHAR[]",  # List(Null) -> Empty List of Strings
    "professional_roles": "STRUCT(id VARCHAR, name VARCHAR)[]",
    # Contacts (Polars says List(Null) for phones, but we keep structure just in case data arrives)
    "contacts": "STRUCT(name VARCHAR, email VARCHAR, phones STRUCT(country VARCHAR, city VARCHAR, number VARCHAR, comment VARCHAR)[])",
    # Nullable / Misc
    "adv_context": "VARCHAR",
    "sort_point_distance": "DOUBLE",
    "snippet": "STRUCT(requirement VARCHAR, responsibility VARCHAR)",
    "branding": "STRUCT(type VARCHAR, tariff VARCHAR)",
    "insider_interview": "STRUCT(id VARCHAR, url VARCHAR)",
    # Complex Video Vacancy
    "video_vacancy": "STRUCT(cover_picture STRUCT(resized_path VARCHAR, resized_width BIGINT, resized_height BIGINT), snippet_picture STRUCT(url VARCHAR), video STRUCT(upload_id VARCHAR, url VARCHAR), snippet_video STRUCT(upload_id VARCHAR, url VARCHAR), video_url VARCHAR, snippet_video_url VARCHAR, snippet_picture_url VARCHAR)",
    # Complex Brand Snippet
    "brand_snippet": 'STRUCT(logo VARCHAR, logo_xs VARCHAR, logo_scalable STRUCT("default" STRUCT(width BIGINT, height BIGINT, url VARCHAR), xs STRUCT(width BIGINT, height BIGINT, url VARCHAR)), picture VARCHAR, picture_xs VARCHAR, picture_scalable STRUCT("default" STRUCT(width BIGINT, height BIGINT, url VARCHAR), xs STRUCT(width BIGINT, height BIGINT, url VARCHAR)), background STRUCT(color VARCHAR, gradient STRUCT(angle DOUBLE, color_list STRUCT(color VARCHAR, position DOUBLE)[])))',
}


# --- ФУНКЦИИ ---
def setup_duckdb(db_path=":memory:", memory_limit="2GB"):
    """Инициализация DuckDB с поддержкой S3 (httpfs)."""
    con = duckdb.connect(db_path)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT.replace('http://', '')}';")
    con.execute(f"SET s3_access_key_id='{ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET memory_limit='{memory_limit}';")
    return con


def check_schema_drift(con, s3_path_glob):
    """
    Проверяет, есть ли в файлах колонки, которых нет в TARGET_SCHEMA.
    """
    try:
        # Читаем без схемы, чтобы DuckDB вывел все, что видит в JSON
        # read_json_auto автоматически сэмплирует файл для определения схемы
        query = f"DESCRIBE SELECT * FROM read_json_auto('{s3_path_glob}', ignore_errors=true, hive_partitioning=false) LIMIT 1"
        schema_info = con.execute(query).fetchall()

        # Колонки, которые есть в файле
        file_cols = set([row[0] for row in schema_info])
        # Колонки, которые мы ожидаем
        expected_cols = set(TARGET_SCHEMA.keys())

        # Новые колонки = В файле - Ожидаемые
        new_cols = file_cols - expected_cols

        if new_cols:
            logger.warning(
                f"⚠️ SCHEMA DRIFT DETECTED! Found new columns in source: {new_cols}"
            )
            # Пайплайн не роняем, так как в основной загрузке мы эти поля просто отбросим
        else:
            logger.info("✅ Schema Check Passed: No new columns detected.")

        # Каких колонок НЕТ в файле (будут NULL)
        missing_cols = expected_cols - file_cols
        if missing_cols:
            logger.info(
                f"ℹ️ Missing columns in source (will be NULL): {len(missing_cols)} columns"
            )

    except Exception as e:
        logger.warning(f"Could not perform drift check: {e}")


def calculate_dq_metrics(con, table_name="silver_view"):
    """Считает метрики качества прямо внутри DuckDB."""
    try:
        query = f"""
        SELECT
            count(*) as row_count,
            count(salary) FILTER (WHERE salary IS NULL) / count(*)::DOUBLE as salary_null_rate,
            min(published_at_utc) as min_date,
            max(published_at_utc) as max_date
        FROM {table_name}
        """
        metrics = con.execute(query).fetchone()
        logger.info("--- DQ METRICS (DuckDB) ---")
        logger.info(f"Rows: {metrics[0]}")
        logger.info(f"Salary Null Rate: {metrics[1]:.2%}")
        logger.info(f"Date Range: {metrics[2]} - {metrics[3]}")
        logger.info("---------------------------")
    except Exception as e:
        logger.error(f"DQ Metrics failed: {e}")


@task(task_id="process_bronze_to_silver")
def process_bronze_to_silver(logical_date=None):
    import pendulum

    msk_tz = pendulum.timezone("Europe/Moscow")
    run_date_msk = logical_date.in_timezone(msk_tz)
    target_date = run_date_msk.start_of("day").subtract(days=1)
    date_str = target_date.strftime("%Y-%m-%d")

    bronze_glob = f"s3://{BUCKET_BRONZE}/hh/vacancies/date={date_str}/*.jsonl.gz"
    partition_suffix = (
        f"year={target_date.year}/month={target_date.month}/day={target_date.day}"
    )
    silver_prefix = f"hh/vacancies/{partition_suffix}"

    local_filename = f"part-{uuid.uuid4()}.parquet"
    local_path = f"/tmp/{local_filename}"

    logger.info(f"Processing Batch: {date_str}")
    logger.info(f"Source: {bronze_glob}")

    # Генерация строки колонок для DuckDB
    columns_definition = ", ".join([f"'{k}': '{v}'" for k, v in TARGET_SCHEMA.items()])
    columns_sql = f"{{{columns_definition}}}"

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
        logger.info("Running Schema Drift Check...")
        check_schema_drift(con, bronze_glob)

        logger.info("Executing DuckDB Transformation...")

        # SQL LOGIC:
        # 1. Читаем JSON со строгой схемой (TARGET_SCHEMA).
        # 2. Дедуплицируем.
        # 3. Трансформируем даты:
        #    - Исходные (published_at, created_at) удаляем через EXCLUDE.
        #    - Создаем *_utc (Timestamp) через конвертацию Timezone.
        #    - Создаем *_offset (Int) как разницу в минутах между UTC версией и локальной.
        transform_query = f"""
        COPY (
            WITH raw_data AS (
                SELECT *
                FROM read_json(
                    '{bronze_glob}',
                    columns={columns_sql},
                    format='newline_delimited',
                    ignore_errors=true,
                    filename=false,
                    hive_partitioning=false -- в пути есть поле date=YYYY-mm-dd, duckdb добавляет его как отдельную колонку
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
                    -- Исключаем старые строковые даты
                    * EXCLUDE (published_at, created_at),

                    -- Создаем правильные UTC timestamp
                    (published_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP as published_at_utc,
                    (created_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP as created_at_utc,

                    -- Вычисляем смещение (offset) в минутах
                    -- Логика: published_at (ISO String с оффсетом) - published_at_utc
                    date_diff('minute',
                        (published_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP,
                        published_at::TIMESTAMPTZ::TIMESTAMP
                    )::SMALLINT as published_at_offset,

                    date_diff('minute',
                        (created_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP,
                        created_at::TIMESTAMPTZ::TIMESTAMP
                    )::SMALLINT as created_at_offset
                FROM deduplicated
            )
            SELECT * FROM enriched
        ) TO '{local_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);
        """

        con.execute(transform_query)
        logger.info(f"Transformation complete. Saved to {local_path}")

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
    max_active_runs=1,
    tags=["silver", "hh", "etl", "duckdb"],
)
def silver_dag():
    process_bronze_to_silver()


silver_dag()
