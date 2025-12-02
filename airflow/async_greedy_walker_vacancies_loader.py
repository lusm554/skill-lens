import asyncio
import json
import math
import os
import time
from datetime import datetime, timedelta

import aiohttp
import humanize
from aiolimiter import AsyncLimiter
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

# --- ЗАГРУЗКА КОНФИГУРАЦИИ ---
load_dotenv(".hhru_env")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError(
        "Пожалуйста, заполните CLIENT_ID и CLIENT_SECRET в файле .hhru_env"
    )

# --- КОНСТАНТЫ ---
API_URL = "https://api.hh.ru/vacancies"
OAUTH_URL = "https://hh.ru/oauth/token"
USER_AGENT = "Skill Lens/3.0 (loveyousomuch554@gmail.com)"

PER_PAGE = 100
MAX_ITEMS_PER_SEARCH = 2000

# Скоростные настройки (для авторизованного клиента)
RATE_LIMIT = 10  # Запросов в секунду
MAX_CONCURRENT_DOWNLOADS = 10  # Одновременных "окон"
MAX_RETRIES = 5  # попыток при 429/403/5xx
INITIAL_BACKOFF = 1.0  # Секунд

INITIAL_STEP = timedelta(minutes=60)
MIN_STEP = timedelta(minutes=1)


def parse_dt(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S%z")


def to_str(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


class TokenManager:
    """
    Отвечает за получение, хранение и обновление Auth токена.
    Гарантирует, что мы не будем спамить запросами на /oauth/token.
    """

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.expires_at = 0
        self._lock = (
            asyncio.Lock()
        )  # Чтобы не обновлять токен параллельно из разных задач

    async def get_token(self, session):
        """Возвращает валидный токен. Если старый протух — обновляет."""
        if self._is_valid():
            return self.access_token

        async with self._lock:
            # Double check внутри блокировки (вдруг кто-то уже обновил пока мы ждали лок)
            if self._is_valid():
                return self.access_token

            await self._refresh_token(session)
            return self.access_token

    def _is_valid(self):
        # Считаем токен невалидным за 60 секунд до реального истечения (буфер)
        return self.access_token and time.time() < (self.expires_at - 60)

    async def _refresh_token(self, session):
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        print(">>> Обновление Auth токена...")
        try:
            async with session.post(OAUTH_URL, data=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.access_token = data["access_token"]
                    expires_in = data.get("expires_in", 1209600)  # Default 14 days
                    self.expires_at = time.time() + expires_in
                    print(f">>> Токен успешно получен. Действует {expires_in} сек.")
                else:
                    text = await resp.text()
                    print(
                        f"CRITICAL ERROR: Не удалось получить токен. Status: {resp.status}, Body: {text}"
                    )
                    # Сбрасываем токен, чтобы вызвать ошибку в основном цикле
                    self.access_token = None
        except Exception as e:
            print(f"EXCEPTION при получении токена: {e}")
            self.access_token = None


class VacancyLoader:
    def __init__(self):
        self.limiter = AsyncLimiter(RATE_LIMIT, time_period=1)
        self.session = None
        self.token_manager = TokenManager(CLIENT_ID, CLIENT_SECRET)

    async def _request(self, params):
        url = API_URL
        delay = INITIAL_BACKOFF

        for attempt in range(MAX_RETRIES):
            # 1. Получаем токен (из кеша или свежий)
            token = await self.token_manager.get_token(self.session)
            if not token:
                print("ABORT: Нет валидного токена.")
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

                        # Если токен протух или отозван (401), нужно сбросить его в менеджере
                        if status == 401:
                            print("WARN: Токен недействителен (401). Сброс токена.")
                            self.token_manager.access_token = None
                            # Попробуем снова в следующей итерации цикла (attempt)
                            continue

                        # Стандартные ошибки лимитов
                        if status in [429, 403]:
                            print(f"WARN: Status {status}. Retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue

                        # Ошибки сервера
                        if 500 <= status < 600:
                            print(
                                f"WARN: Server error {status}. Retrying in {delay}s..."
                            )
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue

                        print(
                            f"ERROR: Unrecoverable status {status} for params {params}"
                        )
                        return None, status

                except aiohttp.ClientError as e:
                    print(f"EXCEPTION: {e}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    delay *= 2

        return None, 0

    async def get_found_count(self, start_dt, end_dt):
        params = {
            "date_from": to_str(start_dt),
            "date_to": to_str(end_dt),
            "per_page": 0,
            "page": 0,
        }
        data, status = await self._request(params)
        if data is not None:
            return data.get("found", 0), status
        return None, status

    async def fetch_page(self, start_dt, end_dt, page):
        params = {
            "date_from": to_str(start_dt),
            "date_to": to_str(end_dt),
            "per_page": PER_PAGE,
            "page": page,
        }
        data, status = await self._request(params)
        if data:
            return data.get("items", [])
        return []

    async def generate_intervals(self, start_dt, end_dt):
        """
        Greedy Walker (Жадный шагоход) - нарезка интервалов.
        """
        intervals = []
        curr_start = start_dt
        curr_step = INITIAL_STEP
        total_found = 0

        print(">>> Начинаем авторизованную разведку интервалов...")
        # Предварительно получим токен, чтобы проверить, что всё ок
        if not await self.token_manager.get_token(self.session):
            print("CRITICAL: Не удалось пройти авторизацию на старте.")
            return []

        pbar = tqdm(
            total=int((end_dt - start_dt).total_seconds()), desc="Slicing", unit="sec"
        )
        last_update_time = curr_start

        while curr_start < end_dt:
            proposed_end = min(curr_start + curr_step, end_dt)
            found, status = await self.get_found_count(curr_start, proposed_end)

            # Обработка фатальных ошибок сети или авторизации
            if status not in [200, 404] and found is None:
                print("Stopping slicing due to repeated errors.")
                break

            # Защита от None
            if found is None:
                found = MAX_ITEMS_PER_SEARCH + 1

            if found > MAX_ITEMS_PER_SEARCH:
                # Слишком много, уменьшаем шаг
                if curr_step <= MIN_STEP:
                    # Шаг минимален, берем что есть
                    intervals.append((curr_start, proposed_end, found))
                    delta = (proposed_end - last_update_time).total_seconds()
                    pbar.update(int(delta))
                    last_update_time = proposed_end
                    curr_start = proposed_end + timedelta(seconds=1)
                    total_found += 2000
                else:
                    curr_step = curr_step / 2
            else:
                # Подходящий интервал
                if found > 0:
                    intervals.append((curr_start, proposed_end, found))
                    total_found += found

                delta = (proposed_end - last_update_time).total_seconds()
                pbar.update(int(delta))
                last_update_time = proposed_end
                curr_start = proposed_end + timedelta(seconds=1)

                # Адаптивный шаг
                if found < 500:
                    curr_step = curr_step * 2
                elif found > 1500:
                    curr_step = curr_step * 0.8

                if curr_step > timedelta(days=7):
                    curr_step = timedelta(days=7)

        pbar.close()
        print(
            f">>> Разведка завершена. Найден {len(intervals)} валидных интервалов и {total_found} вакансий."
        )
        return intervals

    async def process_interval(self, start, end, expected_found):
        pages_count = math.ceil(expected_found / PER_PAGE)
        if pages_count == 0:
            return []

        tasks = []
        for p in range(pages_count):
            tasks.append(self.fetch_page(start, end, p))

        results = await asyncio.gather(*tasks)
        vacancies = []
        for page_items in results:
            vacancies.extend(page_items)
        return vacancies

    async def run(self, input_start, input_end, outfile):
        start_dt = parse_dt(input_start)
        end_dt = parse_dt(input_end)

        delta = end_dt - start_dt
        print("Запрашиваемый период времени:", humanize.precisedelta(delta))

        # TCPConnector limit увеличен для скорости
        connector = aiohttp.TCPConnector(limit=100)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session

            # 1. Генерация интервалов
            intervals = await self.generate_intervals(start_dt, end_dt)
            if not intervals:
                print("Нет интервалов для скачивания (или ошибка авторизации).")
                return

            # 2. Скачивание
            all_vacancies = []
            sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

            async def worker(interval_data):
                start, end, found = interval_data
                async with sem:
                    return await self.process_interval(start, end, found)

            tasks = [worker(i) for i in intervals]

            print(f">>> Начинаем скачивание {len(intervals)} окон...")
            for coro in tqdm(
                asyncio.as_completed(tasks), total=len(tasks), desc="Downloading"
            ):
                items = await coro
                all_vacancies.extend(items)

            print(f"Всего получено: {len(all_vacancies)}")
            with open(outfile, "w", encoding="utf-8") as f:
                for v in all_vacancies:
                    f.write(json.dumps(v, ensure_ascii=False) + "\n")
            print(f"Сохранено в {outfile}")


if __name__ == "__main__":
    start_time = "2025-11-23T00:00:00+0300"
    end_time = "2025-11-26T00:00:00+0300"

    loader = VacancyLoader()
    asyncio.run(loader.run(start_time, end_time, "vacancies_auth.jsonl"))
