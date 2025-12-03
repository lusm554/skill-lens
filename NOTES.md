# Заметки процесса разработки

## Организация и обработка данных

[2025 Dec 2 18:14] Способ огранизации данных S3 Data Lake - Medallion Architecture. Bronze - ingestion слой, забираем сырые данные из hh.ru api как есть. Silver - очистка, дедупликация, структурирование, форматирование данных. Gold - агрегированные, чистые, оптимизированные данные под бизнес (приложения, отчетность, аналитика, дешы, ml модели и тд). Данные в формате parquet с партиционированием по дате.


[2025 Dec 2 18:42] Реализация Medallion Architecture в Data Lakehouse (гибрид Data Lake и Data Warehouse). Слои Bronze/Silver/Gold + по необходимости слой Serving - реплицированные данные из Gold в СУБД под специфику потребителя данных (например под web/phone приложение подходит СУБД Postgres).


[2025 Dec 2 18:55] Потоки на Airflow. Итоговый выбор Medallion Architecture в Data Lake, тк Data LakeHouse поверх S3 добавляет Delta Lake/Apache Iceberg/Apache Hudi для свойств БД. Для небольшого приложения LakeHouse перебор, хоть и интересно.


[2025 Dec 2 19:02] Посоветовались с Gemini:

**Схема пайплайна:**

1.  **Bronze (Loader DAG):**
    *   **Tool:** Python (`aiohttp`).
    *   **Action:** API -> S3 (`.jsonl.gz`).
    *   *Здесь не нужны ни Polars, ни DuckDB, просто потоковая запись в файл.*

2.  **Silver (Processor DAG):**
    *   **Tool: Polars.**
    *   **Почему:** Polars великолепен в обработке вложенных структур (nested JSON). У него очень удобный API для `struct` и `list` колонок, чтобы "расплющить" (unnest) сложные ответы HH.ru в плоскую таблицу. DuckDB с JSON работает чуть более многословно.
    *   **Action:** Читает JSONL Bronze -> Чистит, нормализует, Дедуплицирует (в рамках батча) -> Пишет Parquet Silver (партиционированный по дате публикации вакансии, а не дате скачивания!).

3.  **Gold (Analytical DAG / Views):**
    *   **Tool: DuckDB.**
    *   **Почему:** Gold слой — это агрегаты, джойны и SQL-логика (например, "Топ-10 навыков для Python разработчиков"). DuckDB — это SQL-движок. Писать аналитические витрины на SQL (через dbt или просто SQL в DuckDB) часто понятнее, чем на DataFrame API. Плюс, DuckDB умеет читать Parquet файлы Silver слоя как одну виртуальную таблицу без копирования данных.
    *   **Action:** SQL-запросы поверх Silver Parquet -> Сохранение небольших Parquet/CSV файлов для отчетов или бэкенда.


## DevOps, CI/CD

[2025 Dec 3 13:40] Единый docker compose для airflow и minio, тк далее добавятся http сервер, ml сервис и тд.
