import asyncio
import json
import math
import os
from datetime import datetime, timedelta

import aiohttp
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm

# --- КОНФИГУРАЦИЯ ---
API_URL = "https://api.hh.ru/vacancies"
USER_AGENT = "Skill Lens/2.0 greedy_walker_approach (loveyousomuch554@gmail.com)"
AUTH_TOKEN = os.getenv("AUTH_TOKEN")

# Ограничения HH.ru
PER_PAGE = 100
MAX_ITEMS_PER_SEARCH = 2000

# Настройки скорости и повторов
RATE_LIMIT = 10  # макс запросов в секунду
MAX_CONCURRENT_DOWNLOADS = 10  # сколько окон скачивать одновременно
MAX_RETRIES = 5  # попыток при 429/403/5xx
INITIAL_BACKOFF = 2.0  # начальное ожидание (сек) при ошибке

# Настройки "Шагохода" (Slicer)
INITIAL_STEP = timedelta(minutes=60)  # Начальный шаг проверки (1 час)
MIN_STEP = timedelta(minutes=1)  # Минимальный шаг (защита от зацикливания)


def parse_dt(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S%z")


def to_str(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


class VacancyLoader:
    def __init__(self):
        self.limiter = AsyncLimiter(RATE_LIMIT, time_period=1)
        self.session = None

    async def _request(self, params):
        """
        Выполняет запрос с Rate Limit и экспоненциальным Backoff.
        """
        url = API_URL
        headers = {
            "HH-User-Agent": USER_AGENT,
            "Authorization": f"Bearer {AUTH_TOKEN}",
        }
        delay = INITIAL_BACKOFF

        for attempt in range(MAX_RETRIES):
            async with self.limiter:
                try:
                    async with self.session.get(
                        url, params=params, headers=headers
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json(), 200

                        # Обработка ошибок
                        status = resp.status

                        # Если 429 (Too Many Requests) или 403 (Forbidden/Captcha)
                        if status in [429, 403]:
                            print(f"WARN: Status {status}. Retrying in {delay}s...")
                            await asyncio.sleep(delay)
                            delay *= 2  # Увеличиваем ожидание
                            continue

                        # Если 5xx (Server Error), тоже можно повторить
                        if 500 <= status < 600:
                            print(
                                f"WARN: Server Error {status}. Retrying in {delay}s..."
                            )
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue

                        # Остальные ошибки (400, 404 и т.д.) - критические
                        print(
                            f"ERROR: Unrecoverable status {status} for params {params}"
                        )
                        return None, status

                except aiohttp.ClientError as e:
                    print(f"EXCEPTION: {e}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    delay *= 2

        print(f"FAIL: Max retries exceeded for params {params}")
        return None, 0

    async def get_found_count(self, start_dt, end_dt):
        """Узнает количество вакансий в интервале (metadata only)."""
        params = {
            "date_from": to_str(start_dt),
            "date_to": to_str(end_dt),
            "per_page": 0,  # Нам нужно только поле found
            "page": 0,
        }
        data, status = await self._request(params)
        if data is not None:
            return data.get("found", 0), status
        return None, status

    async def fetch_page(self, start_dt, end_dt, page):
        """Скачивает одну страницу вакансий."""
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
        GREEDY WALKER: Идет по оси времени и нарезает безопасные интервалы.
        """
        intervals = []
        curr_start = start_dt
        curr_step = INITIAL_STEP

        print(">>> Начинаем разведку интервалов (Slicing phase)...")
        pbar = tqdm(
            total=int((end_dt - start_dt).total_seconds()),
            desc="Slicing Time",
            unit="sec",
        )
        last_update_time = curr_start
        total_found = 0

        while curr_start < end_dt:
            # Определяем пробный конец интервала
            proposed_end = min(curr_start + curr_step, end_dt)

            # Запрашиваем кол-во (легкий запрос)
            found, status = await self.get_found_count(curr_start, proposed_end)

            if status != 200:
                print("CRITICAL: Не удалось проверить интервал. Прерывание.")
                break

            # ЛОГИКА ШАГОХОДА
            if found > MAX_ITEMS_PER_SEARCH:
                # Слишком много вакансий (>2000). Уменьшаем шаг и пробуем этот же start снова.
                if curr_step <= MIN_STEP:
                    print(
                        f"WARNING: Интервал {curr_step} уже минимален, но found={found}. Берем что есть (потеряем данные >2000)."
                    )
                    # Так как из интервала возьмем только первые 2000
                    total_found += 2000
                    intervals.append((curr_start, proposed_end, found))
                    # Сдвигаем время
                    delta = (proposed_end - last_update_time).total_seconds()
                    pbar.update(int(delta))
                    last_update_time = proposed_end

                    curr_start = proposed_end + timedelta(seconds=1)
                else:
                    curr_step = curr_step / 2
                    # Не сдвигаем curr_start, цикл повторится с меньшим шагом
            else:
                # Кол-во вакансий допустимое (<= 2000).
                # Если 0 - тоже сохраним (или можно пропустить, но лучше проверить),
                # но шаг увеличим сильнее.

                if found > 0:
                    total_found += found
                    intervals.append((curr_start, proposed_end, found))

                # Обновляем прогресс бар
                delta = (proposed_end - last_update_time).total_seconds()
                pbar.update(int(delta))
                last_update_time = proposed_end

                # Сдвигаем время ВПЕРЕД (без нахлеста + 1 сек)
                curr_start = proposed_end + timedelta(seconds=1)

                # Адаптация шага для следующего раза
                if found < 500:
                    # Если вакансий мало, можно смело шагать шире
                    curr_step = curr_step * 2
                elif found > 1500:
                    # Если близко к пределу, лучше чуть уменьшить или оставить как есть
                    curr_step = curr_step * 0.8

                # Защита от слишком большого шага (не больше 30 дней)
                if curr_step > timedelta(days=30):
                    curr_step = timedelta(days=30)

        pbar.close()
        print(
            f">>> Разведка завершена. Найден {len(intervals)} валидных интервалов и {total_found} вакансий."
        )
        return intervals

    async def process_interval(self, start, end, expected_found):
        """Скачивает все страницы для одного интервала."""
        # Рассчитываем кол-во страниц
        pages_count = math.ceil(expected_found / PER_PAGE)
        if pages_count == 0:
            return []

        tasks = []
        for p in range(pages_count):
            tasks.append(self.fetch_page(start, end, p))

        # Скачиваем страницы параллельно внутри окна
        results = await asyncio.gather(*tasks)

        # Собираем в плоский список
        vacancies = []
        for page_items in results:
            vacancies.extend(page_items)

        return vacancies

    async def run(self, input_start, input_end, outfile):
        start_dt = parse_dt(input_start)
        end_dt = parse_dt(input_end)

        async with aiohttp.ClientSession() as session:
            self.session = session

            # 1. Генерируем интервалы (последовательно)
            intervals = await self.generate_intervals(start_dt, end_dt)

            # 2. Скачиваем данные (параллельно по окнам)
            all_vacancies = []

            # Семафор ограничивает количество окон, обрабатываемых одновременно
            # (не путать с RateLimiter - семафор нужен, чтобы не забить память тысячами задач)
            sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

            async def worker(interval_data):
                start, end, found = interval_data
                async with sem:
                    # Скачиваем
                    items = await self.process_interval(start, end, found)
                    return items

            # Создаем задачи
            tasks = [worker(i) for i in intervals]

            print(f">>> Начинаем скачивание {len(intervals)} окон...")

            # Запускаем с прогресс-баром
            for coro in tqdm(
                asyncio.as_completed(tasks), total=len(tasks), desc="Downloading"
            ):
                items = await coro
                all_vacancies.extend(items)

            print(f">>> Скачивание завершено. Всего вакансий: {len(all_vacancies)}")

            # 3. Сохранение
            with open(outfile, "w", encoding="utf-8") as f:
                for v in all_vacancies:
                    f.write(json.dumps(v, ensure_ascii=False) + "\n")
            print(f"Saved to {outfile}")


if __name__ == "__main__":
    # Пример использования
    start_time = "2025-11-25T00:00:00+0300"
    end_time = "2025-11-26T00:00:00+0300"

    loader = VacancyLoader()

    # В Windows asyncio.run может требовать особого event loop policy,
    # но для стандартного скрипта обычно ок.
    asyncio.run(loader.run(start_time, end_time, "vacancies_clean.jsonl"))
