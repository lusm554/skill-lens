"""
### Bronze Layer: HH.ru Vacancies Loader (Chunked Strategy)

Стек: Airflow SDK, Asyncio, S3.
Стратегия:
- Данные скачиваются батчами (по 10,000 вакансий).
- Каждый батч сразу сжимается и улетает в S3 (part_001.jsonl.gz, ...).
- RAM очищается после каждого батча.
- При перезапуске папка за конкретную дату в S3 предварительно очищается (полная перезаливка).

Расписание: Ежедневно в 00:00.
"""

import asyncio
import gzip
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import aiohttp
import pendulum
from aiolimiter import AsyncLimiter
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

# --- КОНФИГУРАЦИЯ ---
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

API_URL = "https://api.hh.ru/vacancies"
OAUTH_URL = "https://hh.ru/oauth/token"
USER_AGENT = "Skill Lens/Airflow-Worker (bronze-loader)"
AIRFLOW_VAR_TOKEN_KEY = "HH_AUTH_TOKEN_INFO"

# Настройки производительности
RATE_LIMIT = 10
MAX_CONCURRENT_DOWNLOADS = 10
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
INITIAL_STEP = timedelta(minutes=60)
MIN_STEP = timedelta(minutes=1)
MAX_ITEMS_PER_SEARCH = 2000
PER_PAGE = 100

# Настройки батчинга
BATCH_SIZE = 10_000  # Сколько вакансий держать в RAM перед сбросом на диск/S3

logger = logging.getLogger("airflow.task")


class TokenManager:
    """Управление токеном (хранение в Airflow Variables)."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._memory_token = None

    async def get_token(self, session: aiohttp.ClientSession) -> str:
        if self._memory_token:
            return self._memory_token

        token_info = Variable.get(
            AIRFLOW_VAR_TOKEN_KEY, deserialize_json=True, default_var=None
        )
        if token_info and self._is_valid(token_info):
            self._memory_token = token_info["access_token"]
            return self._memory_token

        return await self._refresh_token(session)

    def invalidate_token(self):
        self._memory_token = None
        Variable.delete(AIRFLOW_VAR_TOKEN_KEY)

    def _is_valid(self, token_info: dict) -> bool:
        return time.time() < (token_info.get("expires_at", 0) - 300)

    async def _refresh_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        logger.info("Requesting NEW Auth Token...")
        try:
            async with session.post(OAUTH_URL, data=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    access_token = data["access_token"]
                    expires_in = data.get("expires_in", 1209600)
                    Variable.set(
                        AIRFLOW_VAR_TOKEN_KEY,
                        json.dumps(
                            {
                                "access_token": access_token,
                                "expires_at": time.time() + expires_in,
                            }
                        ),
                    )
                    self._memory_token = access_token
                    return access_token
                else:
                    logger.error(f"Token error: {resp.status} - {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"Token exception: {e}")
            return None


class VacancyLoader:
    def __init__(self, client_id: str, client_secret: str):
        self.limiter = AsyncLimiter(RATE_LIMIT, time_period=1)
        self.session = None
        self.token_manager = TokenManager(client_id, client_secret)

    async def _request(self, params: dict) -> Tuple[Optional[dict], int]:
        url = API_URL
        delay = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            token = await self.token_manager.get_token(self.session)
            if not token:
                return None, 401

            headers = {
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {token}",
                "Accept": "*/*",
            }
            async with self.limiter:
                try:
                    async with self.session.get(
                        url, params=params, headers=headers
                    ) as resp:
                        status = resp.status
                        if status == 200:
                            return await resp.json(), 200
                        if status in [401, 403]:
                            logger.warning(f"Auth error {status}. Retrying...")
                            self.token_manager.invalidate_token()
                            await asyncio.sleep(1)
                            continue
                        if status in [429, 500, 502, 503, 504]:
                            logger.warning(f"Status {status}. Retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue
                        logger.error(f"Error {status} params={params}")
                        return None, status
                except Exception as e:
                    logger.error(f"Net error: {e}")
                    await asyncio.sleep(delay)
                    delay *= 2
        return None, 0

    async def get_found_count(
        self, start_dt: datetime, end_dt: datetime
    ) -> Tuple[int, int]:
        params = {
            "date_from": start_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "date_to": end_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "per_page": 0,
        }
        data, status = await self._request(params)
        return (data.get("found", 0) if data else 0), status

    async def fetch_page(
        self, start_dt: datetime, end_dt: datetime, page: int
    ) -> List[dict]:
        params = {
            "date_from": start_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "date_to": end_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "per_page": PER_PAGE,
            "page": page,
        }
        data, status = await self._request(params)
        return data.get("items", []) if data else []

    async def generate_intervals(
        self, start_dt: datetime, end_dt: datetime
    ) -> List[tuple]:
        intervals = []
        curr_start = start_dt
        curr_step = INITIAL_STEP

        if not await self.token_manager.get_token(self.session):
            return []
        logger.info(f"Slicing window: {start_dt} -> {end_dt}")

        while curr_start < end_dt:
            proposed_end = min(curr_start + curr_step, end_dt)
            found, status = await self.get_found_count(curr_start, proposed_end)
            if status not in [200, 404] and found == 0:
                break  # Error

            if found > MAX_ITEMS_PER_SEARCH:
                if curr_step <= MIN_STEP:
                    intervals.append((curr_start, proposed_end, found))
                    curr_start = proposed_end + timedelta(seconds=1)
                else:
                    curr_step /= 2
            else:
                if found > 0:
                    intervals.append((curr_start, proposed_end, found))
                curr_start = proposed_end + timedelta(seconds=1)
                if found < 500:
                    curr_step *= 2
                elif found > 1500:
                    curr_step *= 0.8
                if curr_step > timedelta(days=7):
                    curr_step = timedelta(days=7)
        return intervals

    async def process_interval(
        self, start: datetime, end: datetime, expected_found: int
    ) -> List[dict]:
        pages = math.ceil(expected_found / PER_PAGE)
        tasks = [self.fetch_page(start, end, p) for p in range(pages)]
        results = await asyncio.gather(*tasks)
        return [item for sublist in results for item in sublist]

    # --- НОВЫЙ МЕТОД RUN С БАТЧИНГОМ ---
    async def run(
        self,
        start_dt: datetime,
        end_dt: datetime,
        s3_hook: S3Hook,
        s3_bucket: str,
        s3_prefix: str,
    ):
        connector = aiohttp.TCPConnector(limit=100)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session
            intervals = await self.generate_intervals(start_dt, end_dt)
            if not intervals:
                logger.warning("No intervals found.")
                return

            buffer = []
            batch_counter = 1
            sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

            async def worker(idata):
                async with sem:
                    return await self.process_interval(idata[0], idata[1], idata[2])

            logger.info(
                f"Processing {len(intervals)} intervals with BATCH_SIZE={BATCH_SIZE}..."
            )

            tasks = [worker(i) for i in intervals]

            # Итерируемся по завершенным задачам
            for i, coro in enumerate(asyncio.as_completed(tasks)):
                items = await coro
                buffer.extend(items)

                # Если буфер переполнен -> сбрасываем в S3
                if len(buffer) >= BATCH_SIZE:
                    await self._flush_to_s3(
                        buffer, batch_counter, s3_hook, s3_bucket, s3_prefix
                    )
                    buffer.clear()  # Очищаем RAM
                    batch_counter += 1

                if i > 0 and i % 50 == 0:
                    logger.info(f"Parsed {i}/{len(intervals)} intervals...")

            # Сбрасываем остаток, если есть
            if buffer:
                await self._flush_to_s3(
                    buffer, batch_counter, s3_hook, s3_bucket, s3_prefix
                )

            logger.info("All batches processed and uploaded.")

    async def _flush_to_s3(
        self,
        vacancies: List[dict],
        part_num: int,
        hook: S3Hook,
        bucket: str,
        prefix: str,
    ):
        """Сжимает данные, пишет в tmp, грузит в S3, удаляет tmp."""
        filename = f"part_{part_num:03d}.jsonl.gz"
        local_path = f"/tmp/{filename}"
        s3_key = f"{prefix}/{filename}"

        logger.info(
            f"Flushing batch {part_num} ({len(vacancies)} items) to {s3_key}..."
        )

        # 1. Сжатие на диск
        with gzip.open(local_path, "wt", encoding="utf-8") as f:
            for v in vacancies:
                f.write(json.dumps(v, ensure_ascii=False) + "\n")

        # 2. Загрузка в S3 (синхронный вызов внутри async, блокирует поток ненадолго, но это ок для upload)
        # Для полной асинхронности можно использовать asyncio.to_thread, но S3Hook потокобезопасен
        hook.load_file(
            filename=local_path, key=s3_key, bucket_name=bucket, replace=True
        )

        # 3. Удаление
        os.remove(local_path)


# --- TASKFLOW ---


@task(task_id="fetch_and_upload_batches")
def fetch_and_upload_batches(logical_date=None):
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Env vars missing")

    # 1. Работа с датами (UTC -> MSK -> Вчерашние сутки)
    msk_tz = pendulum.timezone("Europe/Moscow")
    run_date_msk = logical_date.in_timezone(msk_tz)
    end_dt = run_date_msk.start_of("day")  # Сегодня 00:00 MSK
    start_dt = end_dt.subtract(days=1)  # Вчера 00:00 MSK

    date_str = start_dt.strftime("%Y-%m-%d")
    s3_bucket = "bronze"
    s3_prefix = f"hh/vacancies/date={date_str}"

    logger.info(f"Job for: {start_dt} -> {end_dt}")
    logger.info(f"Target S3: s3://{s3_bucket}/{s3_prefix}/")

    # 2. Идемпотентность: Очищаем префикс перед стартом
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # Создаем бакет если нет
    if not s3_hook.check_for_bucket(s3_bucket):
        s3_hook.create_bucket(s3_bucket)

    # Удаляем старые файлы (если был рестарт)
    keys_to_delete = s3_hook.list_keys(bucket_name=s3_bucket, prefix=s3_prefix)
    if keys_to_delete:
        logger.info(f"Cleaning up {len(keys_to_delete)} old files in {s3_prefix}...")
        s3_hook.delete_objects(bucket=s3_bucket, keys=keys_to_delete)

    # 3. Запуск
    loader = VacancyLoader(CLIENT_ID, CLIENT_SECRET)
    # Передаем хук внутрь, чтобы лоадер сам управлял батчами
    asyncio.run(loader.run(start_dt, end_dt, s3_hook, s3_bucket, s3_prefix))


@dag(
    dag_id="bronze_hh_loader_chunked",
    schedule="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "hh", "etl", "chunked"],
)
def bronze_hh_loader_dag():
    fetch_and_upload_batches()


bronze_hh_loader_dag()
