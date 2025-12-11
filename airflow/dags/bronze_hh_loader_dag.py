"""
### Bronze Layer: HH.ru Vacancies Loader (Advanced Drilling Strategy)

DAG выгружает сырые вакансии за прошедшие сутки в S3 (Data Lake Bronze).

**Стратегия обхода ограничений (2000 вакансий на запрос):**
1. **Time Slicing:** Сначала пытаемся уменьшить временное окно (вплоть до 1 минуты).
2. **Employment Slicing:** Если в 1 минуте > 2000 вакансий, разбиваем запрос по `employment_form` (Полная, Частичная, Проект...).
3. **Work Format Slicing:** Если и этого мало, добавляем разбиение по `work_format` (Удаленка, Офис...).
4. **Hard Cap:** Если даже с максимальными фильтрами вакансий > 2000, скачиваем первые 2000 и идем дальше.

**Параметры:**
- Расписание: 00:00 ежедневно.
- Формат: JSONL.GZ
- S3 Путь: s3://bronze/hh/vacancies/date=YYYY-MM-DD/
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
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

# --- КОНФИГУРАЦИЯ ---
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

API_URL = "https://api.hh.ru/vacancies"
OAUTH_URL = "https://hh.ru/oauth/token"
USER_AGENT = "Skill Lens/Airflow-Worker (bronze-loader-v3)"
AIRFLOW_VAR_TOKEN_KEY = "HH_AUTH_TOKEN_INFO"
BRONZE_DATASET = Dataset("s3://bronze/hh/vacancies")

# Лимиты и настройки
RATE_LIMIT = 10  # Запросов в секунду
MAX_CONCURRENT_DOWNLOADS = 10  # Параллельность скачивания страниц
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
BATCH_SIZE = 10000  # Размер пачки для сброса на диск
MAX_API_DEPTH = 2000  # Жесткий лимит HH.ru (20 страниц * 100)
PER_PAGE = 100

# Стратегия нарезки
INITIAL_STEP = timedelta(minutes=60)
MIN_STEP = timedelta(minutes=1)  # Меньше минуты не дробим, переходим к параметрам

# Справочники для дробления (Drill Down)
# Источник: https://api.hh.ru/dictionaries
EMPLOYMENT_FORMS = [
    "FULL",  # Полная занятость
    "PART",  # Частичная занятость
    "PROJECT",  # Проектная работа
    "FLY_IN_FLY_OUT",  # Вахта
    "SIDE_JOB",  # Подработка
]

WORK_FORMATS = [
    "ON_SITE",  # На месте
    "REMOTE",  # Удаленно
    "HYBRID",  # Гибрид
    "FIELD_WORK",  # Разъездной
]

logger = logging.getLogger("airflow.task")


# --- МЕНЕДЖЕР ТОКЕНОВ ---
class TokenManager:
    """
    Отвечает за получение, кеширование и обновление токена через Airflow Variables.

    Attributes:
        client_id (str): Идентификатор клиента HH.ru.
        client_secret (str): Секретный ключ клиента HH.ru.
        _memory_token (Optional[str]): Закешированный токен в памяти процесса.
    """

    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self._memory_token: Optional[str] = None

    async def get_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        """
        Получает валидный токен доступа.

        Сначала проверяет кеш в памяти, затем Airflow Variables,
        и если токена нет или он истек — запрашивает новый у API.

        Args:
            session (aiohttp.ClientSession): Сессия для выполнения запросов.

        Returns:
            Optional[str]: Токен доступа или None в случае ошибки.
        """
        # 1. Проверяем память (самый быстрый путь)
        if self._memory_token:
            return self._memory_token

        # 2. Проверяем Airflow Variables (база данных)
        token_info = Variable.get(
            AIRFLOW_VAR_TOKEN_KEY, deserialize_json=True, default_var=None
        )
        if token_info and self._is_valid(token_info):
            self._memory_token = token_info["access_token"]
            return self._memory_token

        # 3. Обновляем через API
        return await self._refresh_token(session)

    def invalidate_token(self) -> None:
        """Сброс токена при ошибках 401/403."""
        logger.warning("[AUTH] Invalidating token...")
        self._memory_token = None
        # Удаляем переменную, чтобы другие воркеры тоже обновили токен
        Variable.delete(AIRFLOW_VAR_TOKEN_KEY)

    def _is_valid(self, token_info: dict) -> bool:
        """
        Проверяет валидность токена по времени истечения.

        Args:
            token_info (dict): Словарь с информацией о токене.

        Returns:
            bool: True, если токен валиден еще минимум 5 минут.
        """
        # Валиден, если до истечения осталось больше 5 минут
        return time.time() < (token_info.get("expires_at", 0) - 300)

    async def _refresh_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        """
        Запрашивает новый токен у API HH.ru.

        Args:
            session (aiohttp.ClientSession): Сессия для запроса.

        Returns:
            Optional[str]: Новый токен доступа или None.
        """
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        logger.info("[AUTH] Requesting NEW Token from HH...")
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
                    logger.info(f"[AUTH] Token refreshed. Expires in {expires_in}s")
                    return access_token
                else:
                    logger.error(f"[AUTH] Error: {resp.status} - {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"[AUTH] Exception: {e}")
            return None


# --- ЗАГРУЗЧИК ВАКАНСИЙ ---
class VacancyLoader:
    """
    Класс для загрузки вакансий с API HH.ru с использованием стратегии "Drill Down".

    Позволяет обходить ограничение API на выдачу макс. 2000 записей путем
    дробления запросов по времени и параметрам фильтрации.

    Attributes:
        limiter (AsyncLimiter): Ограничитель скорости запросов.
        session (Optional[aiohttp.ClientSession]): Сессия aiohttp.
        token_manager (TokenManager): Менеджер токенов.
    """

    def __init__(self, client_id: str, client_secret: str) -> None:
        self.limiter = AsyncLimiter(RATE_LIMIT, time_period=1)
        self.session: Optional[aiohttp.ClientSession] = None
        self.token_manager = TokenManager(client_id, client_secret)

    async def _request(self, params: dict) -> Tuple[Optional[dict], int]:
        """
        Базовый метод запроса с ретраями и обработкой ошибок.

        Автоматически обновляет токен при ошибках авторизации и соблюдает Rate Limit.

        Args:
            params (dict): Параметры GET-запроса.

        Returns:
            Tuple[Optional[dict], int]: JSON-ответ (или None) и HTTP-статус.
        """
        if self.session is None:
            logger.error("Session not initialized")
            return None, 500

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

                        # Авторизация слетела
                        if status in [401, 403]:
                            logger.warning(f"[API] Auth error {status}. Retrying...")
                            self.token_manager.invalidate_token()
                            await asyncio.sleep(1)
                            continue

                        # Лимиты или серверные ошибки
                        if status in [429, 500, 502, 503, 504]:
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue

                        # Bad Request (часто при запросе page > 19)
                        if status == 400:
                            return None, 400

                        logger.error(
                            f"[API] Unrecoverable error {status} params={params}"
                        )
                        return None, status
                except Exception as e:
                    logger.error(f"[API] Network error: {e}")
                    await asyncio.sleep(delay)
                    delay *= 2
        return None, 0

    async def get_found_count(
        self, start_dt: datetime, end_dt: datetime, extra_params: Optional[dict] = None
    ) -> int:
        """
        Получает только количество найденных вакансий (found).

        Используется для оценки плотности вакансий в интервале.

        Args:
            start_dt (datetime): Начало интервала.
            end_dt (datetime): Конец интервала.
            extra_params (Optional[dict]): Дополнительные фильтры (занятость, формат).

        Returns:
            int: Количество найденных вакансий.
        """
        params = {
            "date_from": start_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "date_to": end_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "per_page": 0,
        }
        if extra_params:
            params.update(extra_params)

        data, status = await self._request(params)
        if status == 200 and data:
            return data.get("found", 0)
        return 0

    async def generate_tasks(self, start_dt: datetime, end_dt: datetime) -> List[dict]:
        """
        Главный алгоритм (The Greedy Walker + Drilling).

        Сканирует временной диапазон и разбивает его на задачи (Task), чтобы
        в каждой задаче было <= 2000 вакансий. Если время дробить нельзя (1 минута),
        применяет фильтры по Employment и Work Format.

        Args:
            start_dt (datetime): Общее начало периода.
            end_dt (datetime): Общий конец периода.

        Returns:
            List[dict]: Список задач для скачивания.
            Task = {'start': dt, 'end': dt, 'params': dict, 'found': int}
        """
        tasks = []
        curr_start = start_dt
        curr_step = INITIAL_STEP

        if self.session is None:
            logger.error("Session not initialized")
            return []

        # Проверка токена перед стартом
        if not await self.token_manager.get_token(self.session):
            return []

        logger.info(f">>> Starting scan: {start_dt} -> {end_dt}")
        last_log_time = time.time()

        while curr_start < end_dt:
            # Логирование прогресса раз в 10 секунд
            if time.time() - last_log_time > 10:
                logger.info(
                    f"[SCANNER] Progress: {curr_start} / {end_dt}. Generated {len(tasks)} tasks so far."
                )
                last_log_time = time.time()

            proposed_end = min(curr_start + curr_step, end_dt)
            found = await self.get_found_count(curr_start, proposed_end)

            # --- Сценарий 1: Вакансий мало или норма (<= 2000) ---
            if 0 < found <= MAX_API_DEPTH:
                # Отличный интервал, берем
                tasks.append(
                    {
                        "start": curr_start,
                        "end": proposed_end,
                        "params": {},
                        "found": found,
                    }
                )
                # Сдвигаем время
                curr_start = proposed_end + timedelta(seconds=1)

                # Адаптация: если совсем мало вакансий, пробуем увеличить шаг
                if found < 500 and curr_step < timedelta(days=1):
                    curr_step *= 2

            # --- Сценарий 2: Много вакансий, но можно дробить время (> 1 мин) ---
            elif found > MAX_API_DEPTH and curr_step > MIN_STEP:
                # Уменьшаем шаг в 2 раза и пробуем снова (curr_start не сдвигаем)
                curr_step /= 2
                # logger.debug(f"[TIME SPLIT] Found {found} > 2000. Reducing step to {curr_step}")
                continue

            # --- Сценарий 3: Много вакансий, время дробить нельзя (1 мин). Дробим параметры! ---
            elif found > MAX_API_DEPTH and curr_step <= MIN_STEP:
                logger.info(
                    f"[DRILL DOWN] Interval {curr_start} - {proposed_end} has {found} items. Drilling by params..."
                )
                await self._drill_down_by_employment(curr_start, proposed_end, tasks)

                # После успешного дробления сдвигаем время
                curr_start = proposed_end + timedelta(seconds=1)
                # Сбрасываем шаг к начальному, так как сложный участок пройден
                curr_step = INITIAL_STEP

            # --- Сценарий 4: 0 вакансий ---
            else:
                curr_start = proposed_end + timedelta(seconds=1)
                if curr_step < timedelta(days=1):
                    curr_step *= 2

        logger.info(f">>> Scanning complete. Generated {len(tasks)} tasks.")
        return tasks

    async def _drill_down_by_employment(
        self, start: datetime, end: datetime, tasks: List[dict]
    ) -> None:
        """
        Разбиение по типу занятости (employment_form).

        Args:
            start (datetime): Начало интервала.
            end (datetime): Конец интервала.
            tasks (List[dict]): Список задач для наполнения.
        """
        total_drilled = 0

        for emp in EMPLOYMENT_FORMS:
            params = {"employment_form": emp}
            found = await self.get_found_count(start, end, params)

            if found == 0:
                continue

            if found <= MAX_API_DEPTH:
                # Ура, разбиение помогло
                tasks.append(
                    {"start": start, "end": end, "params": params, "found": found}
                )
                total_drilled += found
            else:
                # Внутри одного типа занятости все еще > 2000. Дробим глубже.
                logger.info(
                    f"  [DEEP DRILL] {emp} has {found} items. Drilling by work_format..."
                )
                await self._drill_down_by_work_format(start, end, params, tasks)

    async def _drill_down_by_work_format(
        self, start: datetime, end: datetime, base_params: dict, tasks: List[dict]
    ) -> None:
        """
        Разбиение по формату работы (work_format).

        Args:
            start (datetime): Начало интервала.
            end (datetime): Конец интервала.
            base_params (dict): Параметры из верхнего уровня (employment_form).
            tasks (List[dict]): Список задач для наполнения.
        """
        for work in WORK_FORMATS:
            params = base_params.copy()
            params["work_format"] = work
            found = await self.get_found_count(start, end, params)

            if found == 0:
                continue

            if found > MAX_API_DEPTH:
                # Крайний случай: 1 минута + Тип занятости + Формат работы > 2000.
                # Такое практически невозможно, но если случилось — берем CAP (первые 2000).
                logger.warning(
                    f"  [CAP LIMIT] Interval {start} with params {params} has {found} items. Capped at 2000."
                )
                found = MAX_API_DEPTH

            tasks.append({"start": start, "end": end, "params": params, "found": found})

    async def fetch_page(
        self, start_dt: datetime, end_dt: datetime, page: int, extra_params: dict
    ) -> List[dict]:
        """
        Скачивание одной страницы результатов.

        Args:
            start_dt (datetime): Начало интервала.
            end_dt (datetime): Конец интервала.
            page (int): Номер страницы (0-19).
            extra_params (dict): Доп. параметры фильтрации.

        Returns:
            List[dict]: Список вакансий (items) со страницы.
        """
        params = {
            "date_from": start_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "date_to": end_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "per_page": PER_PAGE,
            "page": page,
            "order_by": "publication_time",
        }
        if extra_params:
            params.update(extra_params)

        data, status = await self._request(params)

        # Если 400 — значит мы вышли за пределы пагинации (HH не отдает > 2000)
        if status == 400:
            return []

        return data.get("items", []) if data else []

    async def process_task(self, task_meta: dict) -> List[dict]:
        """
        Обработка одной задачи из очереди: скачивание всех страниц.

        Args:
            task_meta (dict): Метаданные задачи (start, end, params, found).

        Returns:
            List[dict]: Список всех вакансий для этой задачи.
        """
        # Рассчитываем кол-во страниц, но не больше 20 (лимит HH)
        effective_found = min(task_meta["found"], MAX_API_DEPTH)
        pages = math.ceil(effective_found / PER_PAGE)

        tasks = [
            self.fetch_page(
                task_meta["start"], task_meta["end"], p, task_meta["params"]
            )
            for p in range(pages)
        ]
        results = await asyncio.gather(*tasks)

        # Flatten list
        items = [item for sublist in results for item in sublist]
        return items

    # --- MAIN RUNNER ---
    async def run(
        self,
        start_dt: datetime,
        end_dt: datetime,
        s3_hook: S3Hook,
        s3_bucket: str,
        s3_prefix: str,
    ) -> None:
        """
        Основной метод запуска: генерация задач, скачивание и загрузка в S3.

        Args:
            start_dt (datetime): Начало периода.
            end_dt (datetime): Конец периода.
            s3_hook (S3Hook): Хук Airflow для работы с S3.
            s3_bucket (str): Имя бакета.
            s3_prefix (str): Префикс пути в бакете.
        """
        connector = aiohttp.TCPConnector(limit=100)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session

            # 1. Генерация задач (план выполнения)
            tasks_queue = await self.generate_tasks(start_dt, end_dt)
            if not tasks_queue:
                logger.warning("No tasks generated. Exiting.")
                return

            # 2. Исполнение (Скачивание)
            buffer = []
            batch_id = 1
            sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
            total_tasks = len(tasks_queue)

            async def worker(t_meta: dict) -> List[dict]:
                async with sem:
                    return await self.process_task(t_meta)

            logger.info(f"Downloading content via {total_tasks} tasks...")

            worker_futures = [worker(t) for t in tasks_queue]

            for i, coro in enumerate(asyncio.as_completed(worker_futures)):
                items = await coro
                buffer.extend(items)

                # Сброс батча на диск/S3
                if len(buffer) >= BATCH_SIZE:
                    await self._flush_batch(
                        buffer, batch_id, s3_hook, s3_bucket, s3_prefix
                    )
                    buffer.clear()
                    batch_id += 1

                if i > 0 and i % 50 == 0:
                    percent = (i / total_tasks) * 100
                    logger.info(
                        f"Processed {i}/{total_tasks} ({percent:.1f}%) download tasks..."
                    )

            # Остаток
            if buffer:
                await self._flush_batch(buffer, batch_id, s3_hook, s3_bucket, s3_prefix)

            logger.info("All batches uploaded successfully.")

    async def _flush_batch(
        self, data: List[dict], batch_id: int, hook: S3Hook, bucket: str, prefix: str
    ) -> None:
        """
        Сжатие и отправка в S3.

        Args:
            data (List[dict]): Список вакансий для сохранения.
            batch_id (int): Номер батча.
            hook (S3Hook): Хук S3.
            bucket (str): Бакет.
            prefix (str): Префикс пути.
        """
        fname = f"part_{batch_id:04d}.jsonl.gz"
        local_path = f"/tmp/{fname}"
        s3_key = f"{prefix}/{fname}"

        logger.info(f"[S3] Flushing batch {batch_id} ({len(data)} items) -> {s3_key}")

        with gzip.open(local_path, "wt", encoding="utf-8") as f:
            for v in data:
                f.write(json.dumps(v, ensure_ascii=False) + "\n")

        hook.load_file(local_path, key=s3_key, bucket_name=bucket, replace=True)
        os.remove(local_path)


# --- AIRFLOW TASK ---


@task(task_id="fetch_and_upload_batches", outlets=[BRONZE_DATASET])
def fetch_and_upload_batches(logical_date=None) -> None:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Environment variables CLIENT_ID or CLIENT_SECRET missing")

    # 1. Определяем период (Вчерашние сутки по MSK)
    msk_tz = pendulum.timezone("Europe/Moscow")

    if logical_date is None:
        run_date_msk = pendulum.now(msk_tz)
    else:
        run_date_msk = logical_date.in_timezone(msk_tz)

    # Конец = начало текущего дня (00:00:00 сегодня)
    end_dt = run_date_msk.start_of("day")
    # Начало = минус 1 день (00:00:00 вчера)
    start_dt = end_dt.subtract(days=1)

    date_str = start_dt.strftime("%Y-%m-%d")
    s3_bucket = "bronze"
    s3_prefix = f"hh/vacancies/date={date_str}"

    logger.info(f"Job Period (MSK): {start_dt} -> {end_dt}")
    logger.info(f"Target: s3://{s3_bucket}/{s3_prefix}")

    # 2. Идемпотентность (чистим папку перед загрузкой)
    s3 = S3Hook(aws_conn_id="aws_default")
    if not s3.check_for_bucket(s3_bucket):
        s3.create_bucket(s3_bucket)

    existing_keys = s3.list_keys(bucket_name=s3_bucket, prefix=s3_prefix)
    if existing_keys:
        logger.info(f"Cleaning {len(existing_keys)} old files...")
        s3.delete_objects(bucket=s3_bucket, keys=existing_keys)

    # 3. Запуск
    loader = VacancyLoader(CLIENT_ID, CLIENT_SECRET)
    asyncio.run(loader.run(start_dt, end_dt, s3, s3_bucket, s3_prefix))


@dag(
    dag_id="bronze_hh_loader_v3",
    description="Loads raw HH.ru vacancies (Bronze) with advanced param drilling",
    schedule="0 0 * * *",  # 00:00 UTC Daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "hh", "etl", "v3"],
)
def bronze_hh_loader_dag():
    fetch_and_upload_batches()


bronze_hh_loader_dag()
