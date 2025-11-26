import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
from tqdm import tqdm

API_URL = "https://api.hh.ru/vacancies"
MIN_INTERVAL = timedelta(minutes=5)
MAX_INTERVAL = timedelta(days=30)
PER_PAGE = 100
MAX_CONCURRENT_REQUESTS = 7  # Лимит асинхронных запросов

USER_AGENT = "Skill Lens/1.0 (loveyousomuch554@gmail.com)"

def parse_datetime(dt):
    return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")

def to_str(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")

def split_interval(start, end):
    mid = start + (end - start) / 2
    return (start, mid), (mid, end)

async def get_found(session, date_from, date_to):
    params = {
        "date_from": to_str(date_from),
        "date_to": to_str(date_to),
        "page": 0,
        "per_page": 0
    }
    headers = {
        "HH-User-Agent": USER_AGENT
    }
    async with session.get(API_URL, params=params, headers=headers) as r:
        print('get_found', r.status, r.reason)
        data = await r.json()
        return int(data.get("found", 0))

async def fetch_page(session, date_from, date_to, page):
    params = {
        "date_from": to_str(date_from),
        "date_to": to_str(date_to),
        "per_page": PER_PAGE,
        "page": page
    }
    headers = {
        "HH-User-Agent": USER_AGENT
    }
    async with session.get(API_URL, params=params, headers=headers) as r:
        print('fetch_page', r.status, r.reason)
        data = await r.json()
        return data.get("items", [])

async def fetch_vacancies_async(session, date_from, date_to, semaphore, log):
    params = {
        "date_from": to_str(date_from),
        "date_to": to_str(date_to),
        "per_page": PER_PAGE,
        "page": 0
    }
    headers = {
        "HH-User-Agent": USER_AGENT
    }
    # Получаем total_pages и found
    async with session.get(API_URL, params=params, headers=headers) as r:
        print('fetch_vacancies_async', r.status, r.reason)
        main_data = await r.json()
        total_pages = main_data.get("pages", 1)
        found = main_data.get("found", 0)
        log.append(f"fetch_vacancies_async: {to_str(date_from)} - {to_str(date_to)}: found={found}, pages={total_pages}")
        tasks = []
        # Лимитируем асинхронные запросы с помощью semaphore
        for page in range(total_pages):
            task = fetch_page_limited(session, date_from, date_to, page, semaphore)
            tasks.append(task)
        results = await tqdm_asyncio_gather(tasks)
        # results — список списков items
        flattened = [item for sublist in results for item in sublist]
        log.append(f"fetched {len(flattened)} items for window {to_str(date_from)} - {to_str(date_to)}")
    return flattened

async def fetch_page_limited(session, date_from, date_to, page, semaphore):
    async with semaphore:
        return await fetch_page(session, date_from, date_to, page)

async def tqdm_asyncio_gather(tasks):
    results = []
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        result = await coro
        results.append(result)
    return results

async def recursive_fetch_async(session, date_from, date_to, semaphore, results_raw, log, overlap_minutes=5):
    # Добавляем overlap для компенсации округления API (например, +5 минут)
    interval = date_to - date_from
    if interval < MIN_INTERVAL:
        log.append(f"interval < MIN_INTERVAL: {to_str(date_from)} — {to_str(date_to)} skipped")
        return

    found = await get_found(session, date_from, date_to)
    log.append(f"recursive_fetch_async: {to_str(date_from)} — {to_str(date_to)}: found={found}")
    if found > 2000:
        # Делим интервал на две части с overlap
        overlap = timedelta(minutes=overlap_minutes)
        mid = date_from + (date_to - date_from) / 2
        left_end = mid + overlap  # расширяем правый край левого окна
        right_start = mid - overlap  # сужаем левый край правого окна
        await recursive_fetch_async(session, date_from, left_end, semaphore, results_raw, log, overlap_minutes)
        await recursive_fetch_async(session, right_start, date_to, semaphore, results_raw, log, overlap_minutes)
    elif found > 0:
        res = await fetch_vacancies_async(session, date_from, date_to, semaphore, log)
        results_raw.extend(res)

async def main_async(input_start, input_end, outfile="vacancies_raw.jsonl", max_concurrent=7):
    start = parse_datetime(input_start)
    end = parse_datetime(input_end)
    results_raw = []
    log = []

    # Стартовый интервал
    initial_span = end - start
    if initial_span > MAX_INTERVAL / 2:
        interval = MAX_INTERVAL
    else:
        interval = MAX_INTERVAL / 2

    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        curr_start = start
        while curr_start < end:
            curr_end = min(curr_start + interval, end)
            await recursive_fetch_async(session, curr_start, curr_end, semaphore, results_raw, log)
            curr_start = curr_end

    # Сохраняем результаты по одному vacancy на строку
    with open(outfile, "w", encoding="utf-8") as f:
        for item in results_raw:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"Saved {len(results_raw)} raw items to {outfile}")

    # Сохраняем логи
    with open("vacancies_loader.log", "w", encoding="utf-8") as f:
        for entry in log:
            f.write(entry + "\n")
    print("Saved logs to vacancies_loader.log")

# Пример запуска
if __name__ == "__main__":
    # Пример: сутки с 2025-11-25T00:00:00+0300 по 2025-11-26T00:00:00+0300
    input_start = "2025-11-25T09:00:00+0300"
    input_end = "2025-11-25T10:00:00+0300"

    # input_start = "2025-11-25T18:00:00+0300"
    # input_end = "2025-11-25T21:00:00+0300"
    asyncio.run(main_async(input_start, input_end, outfile="vacancies_raw.jsonl", max_concurrent=7))
