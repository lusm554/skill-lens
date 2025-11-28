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
    print(f"{interval=}")
    if interval < MIN_INTERVAL:
        log.append(
            f"interval < MIN_INTERVAL: {to_str(date_from)}-{to_str(date_to)} skipped"
        )
        print(f"interval < MIN_INTERVAL: {to_str(date_from)}-{to_str(date_to)} skipped")
        return

    found, status = await get_found(session, date_from, date_to, limiter, log)
    log.append(
        f"recursive_fetch_async: {to_str(date_from)}-{to_str(date_to)}: found={found} status={status}"
    )
    print(
        f"recursive_fetch_async: {to_str(date_from)}-{to_str(date_to)}: found={found} status={status}"
    )


async def main_async(
    input_start,
    input_end,
    outfile="vacancies_raw.jsonl",
    failed_windows_file="failed_windows.jsonl",
    max_concurrent=5,
):
    print(f"main_async({input_start=}, {input_end=})")
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
    print(f"{initial_span=}, {interval=}")

    limiter = AsyncLimiter(RATE_LIMIT, time_period=1)
    async with aiohttp.ClientSession() as session:
        curr_start = start
        while curr_start < end:
            curr_end = min(curr_start + interval, end)
            print(f"{to_str(curr_start)=}, {to_str(curr_end)=}")
            await recursive_fetch_async(
                session, curr_start, curr_end, limiter, results_raw, log, failed_windows
            )
            curr_start = curr_end


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
