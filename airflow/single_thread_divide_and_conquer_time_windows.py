import requests
import sys
from tqdm import tqdm
import time
from datetime import datetime, timedelta

print(f"{sys.version=}") # sys.version='3.14.0 (main, Oct  7 2025, 09:34:52) [Clang 17.0.0 (clang-1700.3.19.1)]'

MIN_INTERVAL = timedelta(minutes=5)
MAX_INTERVAL = timedelta(days=30)

def get(url: str, params: dict = {}, headers: dict = {}):
    headers = {**headers, "HH-User-Agent": "Skill Lens/1.0 (loveyousomuch554@gmail.com)"}
    params = {**params}
    res = requests.get(url=url, params=params, headers=headers)
    res.raise_for_status()
    return res.json()

def parse_datetime(dt):
    return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")

def get_vacancy_count(date_from, date_to, extra_params=None):
    query_params = {
        "date_from": date_from,
        "date_to": date_to,
        "per_page": 0,
        "page": 0
    }
    if extra_params:
        query_params.update(extra_params)
    data = get("https://api.hh.ru/vacancies", params=query_params)
    found = data.get("found", 0)
    return found

def fetch_vacancies(date_from, date_to, extra_params=None):
    # Здесь логика для перебора page, per_page - реального забора всех вакансий
    print(f"call fetch_vacancies({date_from=}, {date_to=}, {extra_params=})")
    per_page = 100
    query_params = {
        "date_from": date_from,
        "date_to": date_to,
        "per_page": per_page,
        "page": 0,
    }
    vacancies = get("https://api.hh.ru/vacancies", params=query_params)
    total_pages_cnt = vacancies.get("pages")
    total_vacancies = vacancies['items']
    for page_number in tqdm(range(0, total_pages_cnt)):
        query_params.update({"page": page_number})
        vacancies = get("https://api.hh.ru/vacancies", params=query_params)
        total_vacancies.extend(vacancies.get("items", []))
        time.sleep(.5)
    print(f"total fetched {len(total_vacancies)} vacancies, unique {len(set(i.get('id') for i in total_vacancies))}")
    return total_vacancies

def split_interval(start, end):
    mid = start + (end - start) / 2
    return (start, mid), (mid, end)

def recursive_fetch(start, end):
    # Ограничения входа
    if end - start < MIN_INTERVAL:
        return
    count = get_vacancy_count(
        start.strftime("%Y-%m-%dT%H:%M:%S%z"),
        end.strftime("%Y-%m-%dT%H:%M:%S%z")
    )
    _start = start.strftime("%Y-%m-%dT%H:%M:%S%z")
    _end = end.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(f"recursive_fetch({_start=}, {_end=}) {count=}")
    if count > 2000:
        # Делим по времени
        (s1, e1), (s2, e2) = split_interval(start, end)
        recursive_fetch(s1, e1)
        recursive_fetch(s2, e2)
    elif count > 0:
        fetch_vacancies(
            start.strftime("%Y-%m-%dT%H:%M:%S%z"),
            end.strftime("%Y-%m-%dT%H:%M:%S%z")
        )

def main(input_start, input_end):
    start = parse_datetime(input_start)
    end = parse_datetime(input_end)
    initial_span = end - start
    if initial_span > MAX_INTERVAL / 2:
        interval = MAX_INTERVAL
    else:
        interval = MAX_INTERVAL / 2
    print(f"{start=}")
    print(f"{end=}")
    print(f"{interval=}")
    print()
    # Перебираем по кускам
    curr_start = start
    while curr_start < end:
        curr_end = min(curr_start + interval, end)
        recursive_fetch(curr_start, curr_end)
        curr_start = curr_end


# %%time Wall time: 22.5 s
main("2025-11-23T20:00:00+0300", "2025-11-24T00:00:00+0300")

# logs:
# start=datetime.datetime(2025, 11, 23, 20, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=10800)))
# end=datetime.datetime(2025, 11, 24, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=10800)))
# interval=datetime.timedelta(days=15)

# recursive_fetch(_start='2025-11-23T20:00:00+0300', _end='2025-11-24T00:00:00+0300') count=2055
# recursive_fetch(_start='2025-11-23T20:00:00+0300', _end='2025-11-23T22:00:00+0300') count=1423
# call fetch_vacancies(date_from='2025-11-23T20:00:00+0300', date_to='2025-11-23T22:00:00+0300', extra_params=None)
# 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 15/15 [00:14<00:00,  1.03it/s]
# total fetched 1523 vacancies, unique 1423
# recursive_fetch(_start='2025-11-23T22:00:00+0300', _end='2025-11-24T00:00:00+0300') count=632
# call fetch_vacancies(date_from='2025-11-23T22:00:00+0300', date_to='2025-11-24T00:00:00+0300', extra_params=None)
# 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 7/7 [00:06<00:00,  1.07it/s]
# total fetched 732 vacancies, unique 632
# CPU times: user 1.06 s, sys: 166 ms, total: 1.23 s
# Wall time: 22.5 s
