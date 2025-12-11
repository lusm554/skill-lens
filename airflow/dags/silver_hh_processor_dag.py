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
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

# --- КОНФИГУРАЦИЯ ---
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"

BRONZE_DATASET = Dataset("s3://bronze/hh/vacancies")

logger = logging.getLogger("airflow.task")

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
def process_bronze_to_silver(**context):
    import pendulum

    triggering_asset_events = context["triggering_asset_events"]
    first_event = list(triggering_asset_events.values())[0][0]
    logger.info(first_event)
    logical_date = first_event.source_dag_run.data_interval_start

    # logical_date = dag_run.logical_date
    logger.info(f"Context Logical Date: {logical_date} (Type: {type(logical_date)})")

    logger.info(f"Context key {','.join(context.keys())}")

    return

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

    # 2. Очистка S3 (Idempotency)
    s3_hook = S3Hook(aws_conn_id="aws_default")
    if s3_hook.check_for_bucket(BUCKET_SILVER):
        keys = s3_hook.list_keys(BUCKET_SILVER, prefix=silver_prefix)
        if keys:
            logger.info(f"Cleaning {len(keys)} old files in {silver_prefix}...")
            s3_hook.delete_objects(BUCKET_SILVER, keys)
    else:
        s3_hook.create_bucket(BUCKET_SILVER)

    # 3. DuckDB Processing
    con = setup_duckdb()

    try:
        # --- A. Drift Check ---
        # Просто логируем, какие колонки мы нашли.
        # Если в будущем захотим strict check, можно сравнить с эталонным списком.
        detected_cols = get_bronze_schema_columns(con, bronze_glob)
        logger.info(f"Detected {len(detected_cols)} columns in source files.")

        # --- B. Transformation Query ---
        # Используем мощь DuckDB 1.x
        logger.info("Executing DuckDB Transformation...")

        # 1. Читаем JSON (read_json_auto очень быстр)
        # 2. QUALIFY - дедупликация "на лету" без джойнов
        # 3. EXCLUDE - убираем лишние колонки
        # 4. Вычисляем смещения дат

        transform_query = f"""
        COPY (
            WITH raw_data AS (
                SELECT *
                FROM read_json_auto(
                    '{bronze_glob}',
                    ignore_errors=true,
                    union_by_name=true,  -- Склеивает файлы даже если где-то нет полей
                    filename=false       -- Не добавлять имя файла
                )
            ),
            deduplicated AS (
                SELECT *
                FROM raw_data
                WHERE id IS NOT NULL
                -- Оставляем последнюю версию вакансии по дате публикации
                QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY published_at DESC) = 1
            ),
            enriched AS (
                SELECT
                    -- Оставляем все поля, КРОМЕ сырых дат
                    * EXCLUDE (published_at, created_at),

                    -- Конвертация дат в UTC (TIMESTAMPTZ -> UTC -> TIMESTAMP)
                    (published_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP as published_at_utc,
                    (created_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP as created_at_utc,

                    -- Вычисляем Offset (разница между строкой и UTC временем)
                    -- Попытка распарсить строку 'YYYY-MM-DDTHH:MM:SS+0300' как timestamp
                    -- DuckDB date_diff('minute', start, end)
                    date_diff('minute',
                              (published_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::TIMESTAMP,
                              published_at::TIMESTAMP -- Wall Clock Time
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

        # Запускаем монстра
        con.execute(transform_query)
        logger.info(f"Transformation complete. File saved to {local_path}")

        # --- C. DQ Metrics (Optional) ---
        # Считаем метрики уже по готовому Parquet файлу (это очень быстро)
        # Создаем view поверх файла
        con.execute(
            f"CREATE OR REPLACE VIEW metrics_view AS SELECT * FROM parquet_scan('{local_path}')"
        )
        calculate_dq_metrics(con, "metrics_view")

    except Exception as e:
        logger.error(f"DuckDB execution failed: {e}")
        # Если упало, чистим и пробрасываем ошибку
        if os.path.exists(local_path):
            os.remove(local_path)
        raise e
    finally:
        con.close()

    # 4. Upload to S3
    # Проверяем, что файл не пустой
    if os.path.getsize(local_path) < 100:
        logger.warning("Resulting parquet file is empty or too small!")
    else:
        s3_key = f"{silver_prefix}/{local_filename}"
        logger.info(f"Uploading to S3: s3://{BUCKET_SILVER}/{s3_key}")

        s3_hook.load_file(
            filename=local_path, key=s3_key, bucket_name=BUCKET_SILVER, replace=True
        )

    # 5. Cleanup
    if os.path.exists(local_path):
        os.remove(local_path)
    logger.info("Done.")


@dag(
    dag_id="silver_hh_processor",
    description="Transforms Bronze JSONL to Silver Parquet (DuckDB Engine)",
    # schedule="0 1 * * *",
    schedule=[BRONZE_DATASET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "hh", "etl", "duckdb"],
)
def silver_dag():
    process_bronze_to_silver()


silver_dag()
