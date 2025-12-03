"""
### Bronze Layer: HH.ru Vacancies Loader (TaskFlow API / Airflow 3.x)

Этот DAG загружает сырые вакансии с API HH.ru.
Методология: ELT (Extract, Load).

Стек:
- Airflow SDK (TaskFlow API)
- Asyncio (aiohttp, aiolimiter)
- S3 (MinIO)

Расписание: Ежедневно в 00:00 (забирает данные за прошедшие сутки).
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
from aiolimiter import AsyncLimiter
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task  # Airflow 3.x SDK

# --- КОНФИГУРАЦИЯ ---
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

API_URL = "https://api.hh.ru/vacancies"
OAUTH_URL = "https://hh.ru/oauth/token"
USER_AGENT = "Skill Lens/Airflow-Worker (bronze-loader)"

# Настройки производительности
RATE_LIMIT = 10
MAX_CONCURRENT_DOWNLOADS = 10
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
INITIAL_STEP = timedelta(minutes=60)
MIN_STEP = timedelta(minutes=1)
MAX_ITEMS_PER_SEARCH = 2000
PER_PAGE = 100

logger = logging.getLogger("airflow.task")


# --- ВСПОМОГАТЕЛЬНЫЕ КЛАССЫ (БЕЗ ИЗМЕНЕНИЙ) ---


class TokenManager:
    """Управляет получением и обновлением OAuth токена HH.ru."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.expires_at = 0
        self._lock = asyncio.Lock()

    async def get_token(self, session: aiohttp.ClientSession) -> str:
        if self._is_valid():
            return self.access_token
        async with self._lock:
            if self._is_valid():
                return self.access_token
            await self._refresh_token(session)
            return self.access_token

    def _is_valid(self) -> bool:
        return self.access_token and time.time() < (self.expires_at - 60)

    async def _refresh_token(self, session: aiohttp.ClientSession):
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        logger.info("Refreshing Auth Token...")
        try:
            async with session.post(OAUTH_URL, data=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.access_token = data["access_token"]
                    expires_in = data.get("expires_in", 1209600)
                    self.expires_at = time.time() + expires_in
                    logger.info(f"Token refreshed. Expires in {expires_in}s")
                else:
                    text = await resp.text()
                    logger.error(f"Token refresh failed: {resp.status} - {text}")
                    self.access_token = None
        except Exception as e:
            logger.error(f"Token refresh exception: {e}")
            self.access_token = None


class VacancyLoader:
    """Основной класс для выгрузки вакансий."""

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
                logger.error("No valid token available.")
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

                        if status == 401:
                            logger.warning("Token expired (401). Resetting.")
                            self.token_manager.access_token = None
                            continue

                        if status in [429, 403, 500, 502, 503, 504]:
                            logger.warning(f"Status {status}. Retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue

                        logger.error(f"Unrecoverable status {status} for {params}")
                        return None, status
                except Exception as e:
                    logger.error(f"Network error: {e}. Retrying...")
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

        logger.info("Starting Greedy Walker slicing...")
        while curr_start < end_dt:
            proposed_end = min(curr_start + curr_step, end_dt)
            found, status = await self.get_found_count(curr_start, proposed_end)

            if status not in [200, 404] and found == 0:
                logger.error("Stopping slicing due to repeated API errors.")
                break

            if found > MAX_ITEMS_PER_SEARCH:
                if curr_step <= MIN_STEP:
                    intervals.append((curr_start, proposed_end, found))
                    curr_start = proposed_end + timedelta(seconds=1)
                else:
                    curr_step = curr_step / 2
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

        logger.info(f"Slicing complete. Generated {len(intervals)} intervals.")
        return intervals

    async def process_interval(
        self, start: datetime, end: datetime, expected_found: int
    ) -> List[dict]:
        pages = math.ceil(expected_found / PER_PAGE)
        tasks = [self.fetch_page(start, end, p) for p in range(pages)]
        results = await asyncio.gather(*tasks)
        return [item for sublist in results for item in sublist]

    async def run(self, start_dt: datetime, end_dt: datetime, output_path: str):
        connector = aiohttp.TCPConnector(limit=100)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session

            intervals = await self.generate_intervals(start_dt, end_dt)
            if not intervals:
                logger.warning("No intervals found or auth failed.")
                return

            all_vacancies = []
            sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

            async def worker(idata):
                async with sem:
                    return await self.process_interval(idata[0], idata[1], idata[2])

            logger.info(f"Downloading content for {len(intervals)} intervals...")
            tasks = [worker(i) for i in intervals]

            for i, coro in enumerate(asyncio.as_completed(tasks)):
                items = await coro
                all_vacancies.extend(items)
                if i % 10 == 0:
                    logger.info(f"Processed {i}/{len(intervals)} intervals...")

            logger.info(f"Total vacancies fetched: {len(all_vacancies)}")

            logger.info(f"Saving compressed data to {output_path}...")
            with gzip.open(output_path, "wt", encoding="utf-8") as f:
                for v in all_vacancies:
                    f.write(json.dumps(v, ensure_ascii=False) + "\n")

            logger.info("Save complete.")


# --- TASKFLOW DEFINITION ---


@task(task_id="fetch_and_upload_to_s3")
def fetch_and_upload_to_s3(
    data_interval_start: datetime = None, data_interval_end: datetime = None
):
    """
    TaskFlow задача:
    1. Запускает Async Loader.
    2. Загружает файл в S3.
    Airflow 3 автоматически инжектит data_interval_start/end.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("CLIENT_ID or CLIENT_SECRET not found in env!")

    logger.info(data_interval_start)

    # 1. Формируем пути
    date_str = data_interval_start.strftime("%Y-%m-%d")
    logger.info(date_str)
    local_filename = f"hh_vacancies_{date_str}.jsonl.gz"
    local_path = f"/tmp/{local_filename}"
    return

    # Путь в S3: bronze/hh/vacancies/date=YYYY-MM-DD/hh_vacancies.jsonl.gz
    s3_key = f"hh/vacancies/date={date_str}/{local_filename}"
    s3_bucket = "bronze"

    logger.info(
        f"Starting ETL for period: {data_interval_start} -> {data_interval_end}"
    )

    # 2. Extract (Скачивание на диск)
    loader = VacancyLoader(CLIENT_ID, CLIENT_SECRET)
    asyncio.run(loader.run(data_interval_start, data_interval_end, local_path))

    # Проверка, что файл создан
    if not os.path.exists(local_path):
        logger.warning("File was not created. No vacancies found?")
        return

    # 3. Load (Загрузка в S3)
    logger.info(f"Uploading to S3: s3://{s3_bucket}/{s3_key}")
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # Проверка бакета
    if not s3_hook.check_for_bucket(s3_bucket):
        s3_hook.create_bucket(s3_bucket)

    s3_hook.load_file(
        filename=local_path, key=s3_key, bucket_name=s3_bucket, replace=True
    )
    logger.info("Upload successful.")

    # 4. Cleanup
    os.remove(local_path)
    logger.info("Local temp file removed.")


@dag(
    dag_id="bronze_hh_loader",
    description="Loads raw HH.ru vacancies to S3 Bronze layer using TaskFlow API",
    # Cron-выражение: 0 минута, 0 час, каждый день.
    schedule="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "hh", "etl", "airflow3"],
)
def bronze_hh_loader_dag():
    # Вызов задачи
    fetch_and_upload_to_s3()


# Инициализация DAG
bronze_hh_loader_dag()
