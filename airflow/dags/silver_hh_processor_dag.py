"""
### Silver Layer: HH.ru Vacancies Processor

DAG обрабатывает сырые данные (Bronze) за конкретную дату и формирует Silver слой.

Особенности архитектуры:
- **Partitioning Strategy:** Используется "Batch Date" (дата выгрузки по MSK).
  Это решает проблему часовых поясов (когда 00:00 MSK = 21:00 UTC предыдущего дня).
  Все данные из одного Bronze-файла гарантированно попадают в одну Silver-партицию.
- **Idempotency:** Перед записью целевая партиция полностью очищается.
- **Schema Evolution:** Перед чтением проверяется дрифт схемы на сэмпле данных.
"""

import logging
import os
from datetime import datetime, timedelta

import polars as pl
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

# --- КОНФИГУРАЦИЯ ---
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

print(S3_ENDPOINT, ACCESS_KEY, SECRET_KEY)

BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"

logger = logging.getLogger("airflow.task")

storage_options = {
    "aws_endpoint_url": S3_ENDPOINT,
    "aws_access_key_id": ACCESS_KEY,
    "aws_secret_access_key": SECRET_KEY,
    "aws_region": "us-east-1",
    "aws_allow_http": "true",
}

# --- MASTER SCHEMA ---
FULL_SCHEMA = pl.Schema(
    {
        "is_adv_vacancy": pl.Boolean,
        "internship": pl.Boolean,
        "department": pl.Struct({"id": pl.String, "name": pl.String}),
        "working_time_intervals": pl.List(
            pl.Struct({"id": pl.String, "name": pl.String})
        ),
        "show_contacts": pl.Boolean,
        "salary": pl.Struct(
            {
                "from": pl.Int64,
                "to": pl.Int64,
                "currency": pl.String,
                "gross": pl.Boolean,
            }
        ),
        "apply_alternate_url": pl.String,
        "created_at": pl.String,
        "work_format": pl.List(pl.Struct({"id": pl.String, "name": pl.String})),
        "response_url": pl.String,
        "response_letter_required": pl.Boolean,
        "relations": pl.List(pl.Null),
        "professional_roles": pl.List(pl.Struct({"id": pl.String, "name": pl.String})),
        "accept_incomplete_resumes": pl.Boolean,
        "premium": pl.Boolean,
        "has_test": pl.Boolean,
        "contacts": pl.Struct(
            {"name": pl.Null, "email": pl.Null, "phones": pl.List(pl.Null)}
        ),
        "employment": pl.Struct({"id": pl.String, "name": pl.String}),
        "employment_form": pl.Struct({"id": pl.String, "name": pl.String}),
        "adv_context": pl.Null,
        "sort_point_distance": pl.Null,
        "name": pl.String,
        "address": pl.Struct(
            {
                "city": pl.String,
                "street": pl.String,
                "building": pl.String,
                "lat": pl.Float64,
                "lng": pl.Float64,
                "description": pl.Null,
                "raw": pl.String,
                "metro": pl.Struct(
                    {
                        "station_name": pl.String,
                        "line_name": pl.String,
                        "station_id": pl.String,
                        "line_id": pl.String,
                        "lat": pl.Float64,
                        "lng": pl.Float64,
                    }
                ),
                "metro_stations": pl.List(
                    pl.Struct(
                        {
                            "station_name": pl.String,
                            "line_name": pl.String,
                            "station_id": pl.String,
                            "line_id": pl.String,
                            "lat": pl.Float64,
                            "lng": pl.Float64,
                        }
                    )
                ),
                "id": pl.String,
            }
        ),
        "branding": pl.Struct({"type": pl.String, "tariff": pl.String}),
        "insider_interview": pl.Struct({"id": pl.String, "url": pl.String}),
        "working_days": pl.List(pl.Struct({"id": pl.String, "name": pl.String})),
        "fly_in_fly_out_duration": pl.List(
            pl.Struct({"id": pl.String, "name": pl.String})
        ),
        "url": pl.String,
        "employer": pl.Struct(
            {
                "id": pl.String,
                "name": pl.String,
                "url": pl.String,
                "alternate_url": pl.String,
                "logo_urls": pl.Struct(
                    {"original": pl.String, "90": pl.String, "240": pl.String}
                ),
                "vacancies_url": pl.String,
                "country_id": pl.Int64,
                "accredited_it_employer": pl.Boolean,
                "trusted": pl.Boolean,
            }
        ),
        "accept_temporary": pl.Boolean,
        "published_at": pl.String,
        "salary_range": pl.Struct(
            {
                "from": pl.Int64,
                "to": pl.Int64,
                "currency": pl.String,
                "gross": pl.Boolean,
                "mode": pl.Struct({"id": pl.String, "name": pl.String}),
                "frequency": pl.Struct({"id": pl.String, "name": pl.String}),
            }
        ),
        "work_schedule_by_days": pl.List(
            pl.Struct({"id": pl.String, "name": pl.String})
        ),
        "id": pl.String,
        "show_logo_in_search": pl.Boolean,
        "type": pl.Struct({"id": pl.String, "name": pl.String}),
        "experience": pl.Struct({"id": pl.String, "name": pl.String}),
        "area": pl.Struct({"id": pl.String, "name": pl.String, "url": pl.String}),
        "archived": pl.Boolean,
        "working_time_modes": pl.List(pl.Struct({"id": pl.String, "name": pl.String})),
        "alternate_url": pl.String,
        "snippet": pl.Struct({"requirement": pl.String, "responsibility": pl.String}),
        "schedule": pl.Struct({"id": pl.String, "name": pl.String}),
        "night_shifts": pl.Boolean,
        "working_hours": pl.List(pl.Struct({"id": pl.String, "name": pl.String})),
        "adv_response_url": pl.Null,
        "video_vacancy": pl.Struct(
            {
                "cover_picture": pl.Struct(
                    {
                        "resized_path": pl.String,
                        "resized_width": pl.Int64,
                        "resized_height": pl.Int64,
                    }
                ),
                "snippet_picture": pl.Struct({"url": pl.String}),
                "video": pl.Struct({"upload_id": pl.String, "url": pl.String}),
                "snippet_video": pl.Struct({"upload_id": pl.String, "url": pl.String}),
                "video_url": pl.String,
                "snippet_video_url": pl.String,
                "snippet_picture_url": pl.String,
            }
        ),
        "brand_snippet": pl.Struct(
            {
                "logo": pl.Null,
                "logo_xs": pl.Null,
                "logo_scalable": pl.Struct(
                    {
                        "default": pl.Struct(
                            {"width": pl.Int64, "height": pl.Int64, "url": pl.String}
                        ),
                        "xs": pl.Struct(
                            {"width": pl.Int64, "height": pl.Int64, "url": pl.String}
                        ),
                    }
                ),
                "picture": pl.Null,
                "picture_xs": pl.Null,
                "picture_scalable": pl.Struct(
                    {
                        "default": pl.Struct(
                            {"width": pl.Int64, "height": pl.Int64, "url": pl.String}
                        ),
                        "xs": pl.Struct(
                            {"width": pl.Int64, "height": pl.Int64, "url": pl.String}
                        ),
                    }
                ),
                "background": pl.Struct(
                    {
                        "color": pl.String,
                        "gradient": pl.Struct(
                            {
                                "angle": pl.Float64,
                                "color_list": pl.List(
                                    pl.Struct(
                                        {"color": pl.String, "position": pl.Float64}
                                    )
                                ),
                            }
                        ),
                    }
                ),
            }
        ),
        "immediate_redirect_url": pl.String,
    }
)


def check_drift_on_sample(s3_path_glob: str):
    """Проверка дрифта схемы на сэмпле."""
    try:
        sample_df = pl.read_ndjson(
            s3_path_glob,
            n_rows=1000,
            storage_options=storage_options,
            ignore_errors=True,
            infer_schema_length=1000,
        )
        new_cols = set(sample_df.columns) - set(FULL_SCHEMA.names())
        if new_cols:
            logger.warning(f"⚠️ SCHEMA DRIFT DETECTED! New columns: {new_cols}")
        else:
            logger.info("Schema integrity check passed.")
    except Exception as e:
        logger.warning(f"Could not perform drift check: {e}")


def calculate_dq_metrics(df_lazy: pl.LazyFrame):
    """Расчет метрик качества."""
    metrics_exprs = [
        pl.len().alias("row_count"),
        (pl.col("salary").struct.field("from").null_count() / pl.len()).alias(
            "salary_null_ratio"
        ),
        (pl.col("employer").struct.field("id").null_count() / pl.len()).alias(
            "employer_id_null_ratio"
        ),
        pl.col("published_at_utc").min().alias("min_published_at"),
        pl.col("published_at_utc").max().alias("max_published_at"),
    ]
    try:
        metrics = df_lazy.select(metrics_exprs).collect().to_dicts()[0]
        logger.info("--- DQ METRICS ---")
        logger.info(f"Rows: {metrics['row_count']}")
        logger.info(f"Salary Nulls: {metrics['salary_null_ratio']:.2%}")
        logger.info(
            f"Date Range: {metrics['min_published_at']} - {metrics['max_published_at']}"
        )
        logger.info("------------------")
    except Exception as e:
        logger.error(f"DQ Metrics failed: {e}")


@task(task_id="process_bronze_to_silver")
def process_bronze_to_silver(logical_date=None):
    import uuid

    import pendulum

    # 1. Настройка дат
    msk_tz = pendulum.timezone("Europe/Moscow")
    run_date_msk = logical_date.in_timezone(msk_tz)
    target_date = run_date_msk.start_of("day").subtract(days=1)
    date_str = target_date.strftime("%Y-%m-%d")

    # Пути
    bronze_path = f"s3://{BUCKET_BRONZE}/hh/vacancies/date={date_str}/*.jsonl.gz"

    # Формируем путь партиции ВРУЧНУЮ (год/месяц/день)
    # Это позволяет избежать использования PartitionByKey
    partition_suffix = (
        f"year={target_date.year}/month={target_date.month}/day={target_date.day}"
    )
    silver_prefix = f"hh/vacancies/{partition_suffix}"

    # Локальный временный файл (Safe Mode)
    local_filename = f"part-{uuid.uuid4()}.parquet"
    local_path = f"/tmp/{local_filename}"

    logger.info(f"Processing Batch: {date_str}")
    logger.info(f"Source: {bronze_path}")

    # 2. Очистка S3 (Идемпотентность)
    s3_hook = S3Hook(aws_conn_id="aws_default")
    if s3_hook.check_for_bucket(BUCKET_SILVER):
        keys = s3_hook.list_keys(BUCKET_SILVER, prefix=silver_prefix)
        if keys:
            logger.info(f"Cleaning {len(keys)} old files in {silver_prefix}...")
            s3_hook.delete_objects(BUCKET_SILVER, keys)
    else:
        s3_hook.create_bucket(BUCKET_SILVER)

    # 3. Drift Check (Оставляем как было)
    check_drift_on_sample(bronze_path)

    # 4. Lazy Reading
    try:
        df_lazy = pl.scan_ndjson(
            bronze_path,
            schema=FULL_SCHEMA,
            storage_options=storage_options,
            ignore_errors=True,
        )
    except Exception as e:
        logger.error(f"Failed to read bronze: {e}")
        return

    # 5. Transformations
    df_transformed = (
        df_lazy.filter(pl.col("id").is_not_null())
        # ВАЖНО: Если OOM продолжится, unique() придется убрать или делать стриминг сложнее.
        # maintain_order=False экономит память
        .unique(subset="id", keep="any", maintain_order=False)
        .with_columns(
            [
                pl.col("published_at")
                .str.to_datetime("%Y-%m-%dT%H:%M:%S%z", strict=False)
                .dt.convert_time_zone("UTC")
                .alias("published_at_utc"),
                pl.col("created_at")
                .str.to_datetime("%Y-%m-%dT%H:%M:%S%z", strict=False)
                .dt.convert_time_zone("UTC")
                .alias("created_at_utc"),
                pl.col("published_at")
                .str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False, exact=False)
                .alias("published_at_wc"),
                pl.col("created_at")
                .str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False, exact=False)
                .alias("created_at_wc"),
            ]
        )
        .with_columns(
            [
                (
                    (
                        pl.col("published_at_wc")
                        - pl.col("published_at_utc").dt.replace_time_zone(None)
                    )
                    .dt.total_minutes()
                    .cast(pl.Int16)
                    .alias("published_at_offset")
                ),
                (
                    (
                        pl.col("created_at_wc")
                        - pl.col("created_at_utc").dt.replace_time_zone(None)
                    )
                    .dt.total_minutes()
                    .cast(pl.Int16)
                    .alias("created_at_offset")
                ),
            ]
        )
        .drop(["published_at", "created_at", "published_at_wc", "created_at_wc"])
    )

    # 6. DQ Metrics (Collect - отдельный проход)
    # Если падает по OOM здесь -> закомментируйте временно
    try:
        calculate_dq_metrics(df_transformed)
    except Exception as e:
        logger.warning(f"Skipping DQ due to error: {e}")

    # 7. Write to LOCAL Disk (Спасение от OOM)
    logger.info(f"Streaming transformation to local file: {local_path}...")

    # sink_parquet в локальный файл работает супер-стабильно
    # df_transformed.sink_parquet(local_path, compression="zstd", row_group_size=10_000)

    # 8. Upload to S3
    s3_key = f"{silver_prefix}/{local_filename}"
    logger.info(f"Uploading to S3: s3://{BUCKET_SILVER}/{s3_key}")

    df_transformed.sink_parquet(s3_key, compression="zstd", row_group_size=10_000)

    # s3_hook.load_file(
    #     filename=local_path, key=s3_key, bucket_name=BUCKET_SILVER, replace=True
    # )

    # # Cleanup
    # os.remove(local_path)
    logger.info("Done.")


@dag(
    dag_id="silver_hh_processor",
    description="Transforms Bronze JSONL to Silver Parquet (Batch Partitioned)",
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "hh", "etl", "polars"],
)
def silver_dag():
    process_bronze_to_silver()


silver_dag()
