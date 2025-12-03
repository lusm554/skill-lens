"""
### Bronze Layer: HH.ru Vacancies Loader

Стек: Airflow SDK, Asyncio, S3, Airflow Variables.
Логика:
1. Вычисляем окно "Вчерашние сутки по Москве" на основе logical_date.
2. Берем токен из Airflow Variables (или обновляем, если протух).
3. Скачиваем вакансии асинхронно.
4. Грузим в S3.
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
import pendulum  # Стандартная библиотека времени в Airflow
from aiolimiter import AsyncLimiter
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# from airflow.sdk import Variable
from airflow.sdk import dag, task

# --- КОНФИГУРАЦИЯ ---
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

API_URL = "https://api.hh.ru/vacancies"
OAUTH_URL = "https://hh.ru/oauth/token"
USER_AGENT = "Skill Lens/Airflow-Worker (bronze-loader)"
AIRFLOW_VAR_TOKEN_KEY = "HH_AUTH_TOKEN_INFO"  # Ключ переменной в Airflow

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


class TokenManager:
    """
    Управляет токеном, сохраняя его в Airflow Variables.
    Это позволяет использовать один токен между разными запусками DAG.
    """

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        # Локальный кеш в рамках одного запуска, чтобы не долбить БД Airflow
        self._memory_token = None

    def get_token_sync(self) -> Optional[str]:
        """Синхронное получение токена (для инициализации)."""
        token_info = Variable.get(
            AIRFLOW_VAR_TOKEN_KEY, deserialize_json=True, default_var=None
        )
        if token_info and self._is_valid(token_info):
            return token_info["access_token"]
        return None

    async def get_token(self, session: aiohttp.ClientSession) -> str:
        # 1. Сначала смотрим в память процесса
        if self._memory_token:
            return self._memory_token

        # 2. Смотрим в Airflow Variables (база данных)
        # Variable.get - синхронный вызов, но он быстрый
        token_info = Variable.get(
            AIRFLOW_VAR_TOKEN_KEY, deserialize_json=True, default_var=None
        )

        if token_info and self._is_valid(token_info):
            self._memory_token = token_info["access_token"]
            return self._memory_token

        # 3. Если нет или протух — обновляем через API
        return await self._refresh_token(session)

    def invalidate_token(self):
        """Сбрасывает токен при ошибке 401/403."""
        logger.warning("Invalidating token in Variables and memory...")
        self._memory_token = None
        # Удаляем из переменных Airflow, чтобы следующий запуск получил новый
        Variable.delete(AIRFLOW_VAR_TOKEN_KEY)

    def _is_valid(self, token_info: dict) -> bool:
        # Валиден, если до истечения больше 30 минут
        expires_at = token_info.get("expires_at", 0)
        return time.time() < (expires_at - (60 * 30))

    async def _refresh_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        logger.info("Requesting NEW Auth Token from HH.ru...")
        try:
            async with session.post(OAUTH_URL, data=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    access_token = data["access_token"]
                    expires_in = data.get("expires_in", 1209600)

                    # Сохраняем в Airflow Variables
                    token_info = {
                        "access_token": access_token,
                        "expires_at": time.time() + expires_in,
                    }
                    Variable.set(AIRFLOW_VAR_TOKEN_KEY, json.dumps(token_info))
                    logger.info(
                        f"Token saved to Airflow Variables. Expires in {expires_in}s"
                    )

                    self._memory_token = access_token
                    return access_token
                else:
                    text = await resp.text()
                    logger.error(f"Token request failed: {resp.status} - {text}")
                    return None
        except Exception as e:
            logger.error(f"Token refresh exception: {e}")
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
                logger.error("Failed to obtain token.")
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

                        # Если токен невалиден, сбрасываем его и пробуем снова (re-auth)
                        if status in [401, 403]:
                            logger.warning(
                                f"Auth error ({status}). Invalidating token and retrying..."
                            )
                            self.token_manager.invalidate_token()
                            await asyncio.sleep(1)  # Небольшая пауза перед повтором
                            continue

                        if status in [429, 500, 502, 503, 504]:
                            logger.warning(f"Status {status}. Retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue

                        logger.error(f"Unrecoverable status {status} for params")
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

        # Предварительная проверка авторизации
        if not await self.token_manager.get_token(self.session):
            return []

        logger.info(f"Slicing window: {start_dt} -> {end_dt}")

        while curr_start < end_dt:
            proposed_end = min(curr_start + curr_step, end_dt)
            found, status = await self.get_found_count(curr_start, proposed_end)

            if status not in [200, 404] and found == 0:
                logger.error("Critical API Error during slicing.")
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
                logger.warning("No intervals found.")
                return

            all_vacancies = []
            sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

            async def worker(idata):
                async with sem:
                    return await self.process_interval(idata[0], idata[1], idata[2])

            tasks = [worker(i) for i in intervals]

            logger.info(f"Downloading {len(intervals)} intervals...")
            for i, coro in enumerate(asyncio.as_completed(tasks)):
                items = await coro
                all_vacancies.extend(items)
                if i > 0 and i % 50 == 0:
                    logger.info(f"Progress: {i}/{len(intervals)}...")

            logger.info(f"Total downloaded: {len(all_vacancies)}")

            with gzip.open(output_path, "wt", encoding="utf-8") as f:
                for v in all_vacancies:
                    f.write(json.dumps(v, ensure_ascii=False) + "\n")


# --- TASKFLOW DEFINITION ---


@task(task_id="fetch_and_upload_to_s3")
def fetch_and_upload_to_s3(logical_date=None):
    """
    Основная задача.
    logical_date - это время запуска по расписанию (в UTC).
    Например, для запуска 3 декабря в 00:00, logical_date будет 3 декабря 00:00 UTC.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("CLIENT_ID or CLIENT_SECRET missing!")

    # 1. Работа с датами (Timezone Aware)
    # Конвертируем UTC logical_date в Moscow Time
    msk_tz = pendulum.timezone("Europe/Moscow")

    # Airflow передает pendulum.DateTime
    run_date_msk = logical_date.in_timezone(msk_tz)

    # Нам нужны ПОЛНЫЕ ПРОШЛЫЕ СУТКИ от момента запуска.
    # Если запуск 3 декабря 00:00 MSK -> мы хотим данные за 2 декабря (с 00:00 до 23:59:59).
    # Определяем "конец" окна как начало текущего дня запуска (00:00:00)
    end_dt = run_date_msk.start_of("day")
    # Определяем "начало" окна как минус 1 день
    start_dt = end_dt.subtract(days=1)

    logger.info(f"Logical Date (UTC): {logical_date}")
    logger.info(f"Target Window (MSK): {start_dt} -> {end_dt}")

    # 2. Формируем пути
    date_str = start_dt.strftime("%Y-%m-%d")  # Имя файла по дате начала данных
    local_filename = f"hh_vacancies_{date_str}.jsonl.gz"
    local_path = f"/tmp/{local_filename}"

    s3_key = f"hh/vacancies/date={date_str}/{local_filename}"
    s3_bucket = "bronze"

    # 3. Запуск Loader
    loader = VacancyLoader(CLIENT_ID, CLIENT_SECRET)
    asyncio.run(loader.run(start_dt, end_dt, local_path))

    # 4. Проверка и загрузка
    if not os.path.exists(local_path):
        logger.warning("No file created. Empty run?")
        return

    logger.info(f"Uploading to s3://{s3_bucket}/{s3_key}")
    s3_hook = S3Hook(aws_conn_id="aws_default")

    if not s3_hook.check_for_bucket(s3_bucket):
        s3_hook.create_bucket(s3_bucket)

    s3_hook.load_file(
        filename=local_path, key=s3_key, bucket_name=s3_bucket, replace=True
    )

    os.remove(local_path)
    logger.info("Done.")


@dag(
    dag_id="bronze_hh_loader",
    schedule="0 0 * * *",  # Каждый день в 00:00 UTC (или серверного времени)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "hh", "etl"],
)
def bronze_hh_loader_dag():
    fetch_and_upload_to_s3()


bronze_hh_loader_dag()
