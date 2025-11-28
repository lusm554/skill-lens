import asyncio
import json
from datetime import datetime, timedelta

import aiohttp
from aiolimiter import AsyncLimiter
from tqdm import tqdm

MAX_CONCURRENT_REQUESTS = 5  # максимум одновременных запросов


def parse_datetime(dt):
    return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")


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
