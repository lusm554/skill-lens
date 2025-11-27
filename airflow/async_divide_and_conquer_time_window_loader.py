import asyncio
import json
from datetime import datetime, timedelta

import aiohttp
from aiolimiter import AsyncLimiter
from tqdm import tqdm

API_URL = "https://api.hh.ru/vacancies"
MIN_INTERVAL = timedelta(minutes=5)
MAX_INTERVAL = timedelta(days=30)
PER_PAGE = 100

USER_AGENT = "Skill Lens/1.0 (loveyousomuch554@gmail.com)"

RATE_LIMIT = 5  # запросов в секунду (регулируй сам!)
MAX_CONCURRENT_REQUESTS = 5  # максимум одновременных запросов

MAX_RETRIES = 5  # попыток ретрая
BACKOFF_COEFF = 2  # коэффициент роста задержки
INITIAL_BACKOFF = 2  # секунд (начальный delay)


def parse_datetime(dt):
    return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")


def to_str(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


def split_interval(start, end):
    mid = start + (end - start) / 2
    return (start, mid), (mid, end)


async def rate_limited_get(
    session, url, params, headers, limiter, log, tries=MAX_RETRIES
):
    delay = INITIAL_BACKOFF
    for attempt in range(tries):
        async with limiter:
            try:
                async with session.get(url, params=params, headers=headers) as r:
                    status = r.status
                    if status == 200:
                        return await r.json(), status
                    else:
                        log.append(
                            f"ERROR: status={status}, params={params}, url={url}, attempt={attempt + 1}, delay={delay}s"
                        )
                        if status in [429, 403]:
                            await asyncio.sleep(delay)
                            delay *= BACKOFF_COEFF
                        else:
                            break
            except Exception as ex:
                log.append(
                    f"EXCEPTION: {ex}, params={params}, url={url}, attempt={attempt + 1}, delay={delay}s"
                )
                await asyncio.sleep(delay)
                delay *= BACKOFF_COEFF
    return None, status if "status" in locals() else -1


async def get_found(session, date_from, date_to, limiter, log):
    params = {
        "date_from": to_str(date_from),
        "date_to": to_str(date_to),
        "page": 0,
        "per_page": 0,
    }
    headers = {"HH-User-Agent": USER_AGENT}
    data, status = await rate_limited_get(
        session, API_URL, params, headers, limiter, log
    )
    if data:
        return int(data.get("found", 0)), status
    return 0, status


async def fetch_page(session, date_from, date_to, page, limiter, log):
    params = {
        "date_from": to_str(date_from),
        "date_to": to_str(date_to),
        "per_page": PER_PAGE,
        "page": page,
    }
    headers = {"HH-User-Agent": USER_AGENT}
    data, status = await rate_limited_get(
        session, API_URL, params, headers, limiter, log
    )
    if data:
        return data.get("items", [])
    return []


async def fetch_vacancies_async(session, date_from, date_to, limiter, log):
    # Получаем pages и found с rate limiter
    params = {
        "date_from": to_str(date_from),
        "date_to": to_str(date_to),
        "per_page": PER_PAGE,
        "page": 0,
    }
    headers = {"HH-User-Agent": USER_AGENT}
    data, status = await rate_limited_get(
        session, API_URL, params, headers, limiter, log
    )
    if not data:
        return [], status
    total_pages = data.get("pages", 1)
    found = data.get("found", 0)
    log.append(
        f"fetch_vacancies_async: {to_str(date_from)}-{to_str(date_to)} found={found}, pages={total_pages}"
    )
    tasks = [
        fetch_page_limited(session, date_from, date_to, page, limiter, log)
        for page in range(total_pages)
    ]
    results = await tqdm_asyncio_gather(tasks)
    flattened = [item for sublist in results for item in sublist]
    log.append(
        f"fetched {len(flattened)} items for window {to_str(date_from)}-{to_str(date_to)}"
    )
    return flattened, status


async def fetch_page_limited(session, date_from, date_to, page, limiter, log):
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with sem:
        return await fetch_page(session, date_from, date_to, page, limiter, log)


async def tqdm_asyncio_gather(tasks):
    results = []
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        result = await coro
        results.append(result)
    return results


async def recursive_fetch_async(
    session,
    date_from,
    date_to,
    limiter,
    results_raw,
    log,
    failed_windows,
    overlap_minutes=5,
):
    interval = date_to - date_from
    if interval < MIN_INTERVAL:
        log.append(
            f"interval < MIN_INTERVAL: {to_str(date_from)}-{to_str(date_to)} skipped"
        )
        return

    found, status = await get_found(session, date_from, date_to, limiter, log)
    log.append(
        f"recursive_fetch_async: {to_str(date_from)}-{to_str(date_to)}: found={found} status={status}"
    )
    print(
        f"recursive_fetch_async: {to_str(date_from)}-{to_str(date_to)}: found={found} status={status}"
    )

    # ОБРАБОТКА ЛЮБЫХ ОШИБОК!
    if status != 200 or status is None:
        log.append(
            f"FAILED WINDOW: {to_str(date_from)}-{to_str(date_to)} status={status}"
        )
        failed_windows.append(
            {
                "date_from": to_str(date_from),
                "date_to": to_str(date_to),
                "status": status or -1,
            }
        )
        return  # Не дробить дальше при ошибке!

    # Дальше — только при found корректном!
    if found > 2000:
        interval = date_to - date_from
        half = interval / 2
        raw_overlap = timedelta(minutes=overlap_minutes)
        # ensure overlap does not cancel out the split; otherwise we recurse forever
        effective_overlap = min(raw_overlap, half / 2)
        mid = date_from + half
        left_end = mid + effective_overlap
        right_start = mid - effective_overlap
        # safety: guarantee progress even if overlap is bigger than remaining span
        if left_end >= date_to:
            left_end = mid
        if right_start <= date_from:
            right_start = mid
        await recursive_fetch_async(
            session,
            date_from,
            left_end,
            limiter,
            results_raw,
            log,
            failed_windows,
            overlap_minutes,
        )
        await recursive_fetch_async(
            session,
            right_start,
            date_to,
            limiter,
            results_raw,
            log,
            failed_windows,
            overlap_minutes,
        )
    elif found > 0:
        res, status_vac = await fetch_vacancies_async(
            session, date_from, date_to, limiter, log
        )
        print(
            "fetch_vacancies_async",
            status_vac,
            f"{to_str(date_from)}-{to_str(date_to)}",
        )
        if status_vac != 200:
            log.append(
                f"FAILED WINDOW fetch: {to_str(date_from)}-{to_str(date_to)} status={status_vac}"
            )
            failed_windows.append(
                {
                    "date_from": to_str(date_from),
                    "date_to": to_str(date_to),
                    "status": status_vac,
                }
            )
            return  # ОБЯЗАТЕЛЬНО выйти!
        else:
            results_raw.extend(res)


async def main_async(
    input_start,
    input_end,
    outfile="vacancies_raw.jsonl",
    failed_windows_file="failed_windows.jsonl",
    max_concurrent=5,
):
    start = parse_datetime(input_start)
    end = parse_datetime(input_end)
    results_raw = []
    log = []
    failed_windows = []

    initial_span = end - start
    if initial_span > MAX_INTERVAL / 2:
        interval = MAX_INTERVAL
    else:
        interval = MAX_INTERVAL / 2

    limiter = AsyncLimiter(RATE_LIMIT, time_period=1)
    async with aiohttp.ClientSession() as session:
        curr_start = start
        while curr_start < end:
            curr_end = min(curr_start + interval, end)
            await recursive_fetch_async(
                session, curr_start, curr_end, limiter, results_raw, log, failed_windows
            )
            curr_start = curr_end

    with open(outfile, "w", encoding="utf-8") as f:
        for item in results_raw:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"Saved {len(results_raw)} raw items to {outfile}")

    with open("vacancies_loader.log", "w", encoding="utf-8") as f:
        for entry in log:
            f.write(entry + "\n")
    print("Saved logs to vacancies_loader.log")

    with open(failed_windows_file, "w", encoding="utf-8") as f:
        for fail in failed_windows:
            f.write(json.dumps(fail, ensure_ascii=False) + "\n")
    print(f"Saved {len(failed_windows)} failed windows to {failed_windows_file}")


# Пример запуска
if __name__ == "__main__":
    input_start = "2025-11-25T17:00:00+0300"
    input_end = "2025-11-25T21:00:00+0300"
    asyncio.run(
        main_async(
            input_start,
            input_end,
            outfile="vacancies_raw.jsonl",
            failed_windows_file="failed_windows.jsonl",
            max_concurrent=MAX_CONCURRENT_REQUESTS,
        )
    )
