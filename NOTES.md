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


[2025 Dec 3 21:51] Реализован Loader ETL поток. Слабые места: 1) Дробления по времени не достаточно для пиковых периодов, например, когда за 30 секу публикуется больше 2тыс вакансий. 2) Поток грузит все данные сначала в RAM, затем в дисковое пространство контейнера, только потом в S3, это узкое горлышко, при пиковых нагрузках поток упадет, да и ресурсы можно потреблять разумнее при потоковой/batch'евой загрузке в s3. Также надо расширить лимит hardware для airflow-worker контейнера чтобы высвободить весь потенциал асинхронного скрипта выгрузки hhru.

[2025 Dec 3 22:33] Доавлена батч обработка вакансий - выгружаем в RAM N-ое кол-во вакансий, сжимаем gzip и сохраняем в s3. Механизм пропуска уже скачанных интервалов не реализован, тк 1) система динамическая, hhru api может вернуть другие результаты на один запрос 2) State recovery требует системы состояний, которая усложнит код в разы, а профита сильного не даст ИМХО.

[2025 Dec 10 10:30] Для silver слоя лучше использовать OBT (One Big Table) c частичным flate'ом часто используемых полей. Это лучше подходит для Data Lake + parquet.

[2025 Dec 10 14:10] Переписал silver dag с Polars на DuckDB. Polars работает отлично, но метод дедупликации lazy_df.unique собирает хеш таблицу в RAM, без возможности offload'a на диск. RAM не хватает. Зато DuckDB имеет возможность offload'a на диск и гибкие настройки потребления RAM. На DuckDB silver DAG выполняется 7 секунд для 300тыс jsonl строк с nested структурой. Это имба.

[2025 Dec 11 08:00] ETL потоки bronze и silver слоев готовы. На ежедневной основе собираются все доступные вакансии hh.ru T-1 и сохраняются в parquet zstd с партиционированием year-month-day. Нюансы/доработки: 1) Сейчас партиционирование по logical date airflow, нужно перевести на UTC hh.ru publish_date, тк hh.ru api бывает возвращает вакансии вне рамках запрашиваемой даты 2) Потоки запускаются независимо друг от друга, они как бы "не знают", что один зависит от другого, нужно добавить зависимость между потоками на уровне airflow 3) Расширить Data Quality Check на silver слое с проверкой появилась/пропала колонка, data drift, data null rate.

[2025 Dec 11 14:00] Добавил тригер чтобы silver запускался от bronze с такой же logical date. Тригер работает через TriggerDagRunOperator. Пробовал Assets, он не предназначен, чтобы передавать logical date между DAG'ами. Также добавил max_active_runs=1() в silver bronze, чтобы не выходить за лимиты RAM, Disk, hh.ru API.

[2025 Dec 17 06:30] Реализованы слои Bronze и Silver. Доступны вакансии hh.ru T-1 в parquet. Бывает, вакансии дублируются по id между днями, тк вакансия с одним id может публиковаться несколько раз. Возможно, это стоит добавить в bronze слой как признак, что вакансия публиковалась N раз.

[2025 Dec 17 12:00] Схема данных Silver слоя:
| Колонка | Тип данных (из схемы) | Описание | Примечание |
| :--- | :--- | :--- | :--- |
| **id** | `VARCHAR` | Идентификатор вакансии | Required |
| **name** | `VARCHAR` | Название | Required |
| **schedule** | `STRUCT(id VARCHAR, "name" VARCHAR)` | График работы | **Deprecated**. Объект или null |
| **working_time_modes** | `STRUCT(id VARCHAR, "name" VARCHAR)[]` | Список режимов времени работы | **Deprecated**. Массив объектов или null |
| **working_time_intervals** | `STRUCT(id VARCHAR, "name" VARCHAR)[]` | Список с временными интервалами работы | **Deprecated**. Массив объектов или null |
| **working_days** | `STRUCT(id VARCHAR, "name" VARCHAR)[]` | Список рабочих дней | **Deprecated**. Массив объектов или null |
| **working_hours** | `STRUCT(id VARCHAR, "name" VARCHAR)[]` | Список вариантов рабочих часов в день | Массив объектов или null |
| **work_schedule_by_days** | `STRUCT(id VARCHAR, "name" VARCHAR)[]` | Список графиков работы | Массив объектов или null |
| **fly_in_fly_out_duration** | `STRUCT(id VARCHAR, "name" VARCHAR)[]` | Варианты длительности вахты | Массив объектов или null |
| **is_adv_vacancy** | `BOOLEAN` | — | Нет в описании |
| **internship** | `BOOLEAN` | Стажировка | |
| **accept_temporary** | `BOOLEAN` | Временное трудоустройство | Указание, что вакансия доступна с временным трудоустройством |
| **accept_incomplete_resumes** | `BOOLEAN` | Неполное резюме | Разрешен ли отклик на вакансию неполным резюме. Required |
| **premium** | `BOOLEAN` | Премиум-вакансия | Является ли данная вакансия премиум-вакансией |
| **has_test** | `BOOLEAN` | Наличие теста | Информация о наличии прикрепленного тестового задания. Required |
| **show_contacts** | `BOOLEAN` | Доступны ли контакты | |
| **response_letter_required** | `BOOLEAN` | Сопроводительное письмо | Обязательно ли заполнять сообщение при отклике. Required |
| **show_logo_in_search** | `BOOLEAN` | Отображать ли лого | Отображать ли лого для вакансии в поисковой выдаче |
| **archived** | `BOOLEAN` | В архиве | Находится ли данная вакансия в архиве |
| **night_shifts** | `BOOLEAN` | Ночные смены | |
| **url** | `VARCHAR` | URL вакансии | Required |
| **alternate_url** | `VARCHAR` | Ссылка на представление вакансии на сайте | Required |
| **apply_alternate_url** | `VARCHAR` | Ссылка на отклик на вакансию на сайте | Required |
| **response_url** | `VARCHAR` | URL отклика | Для прямых вакансий (type.id=direct) |
| **adv_response_url** | `VARCHAR` | — | Нет в описании |
| **immediate_redirect_url** | `VARCHAR` | — | Нет в описании |
| **salary** | `STRUCT("from" BIGINT, "to" BIGINT, currency VARCHAR, gross BOOLEAN)` | Зарплата | **Deprecated**. Required |
| **salary_range** | `STRUCT(...)` | Зарплата | Required. Расширенная структура с mode и frequency |
| **employer** | `STRUCT(...)` | Информация о компании работодателя | Required. Содержит ссылки на логотипы, аккредитацию и др. |
| **department** | `STRUCT(id VARCHAR, "name" VARCHAR)` | Департамент | Required. Может быть null-объектом |
| **type** | `STRUCT(id VARCHAR, "name" VARCHAR)` | Тип вакансии | Required |
| **employment** | `STRUCT(id VARCHAR, "name" VARCHAR)` | Тип занятости | **Deprecated** |
| **employment_form** | `STRUCT(id VARCHAR, "name" VARCHAR)` | Тип занятости | |
| **experience** | `STRUCT(id VARCHAR, "name" VARCHAR)` | Опыт работы | |
| **area** | `STRUCT(id VARCHAR, "name" VARCHAR, url VARCHAR)` | Регион | Required |
| **address** | `STRUCT(...)` | Адрес | В схеме включает в себя массив `metro_stations` (станции метро) |
| **work_format** | `STRUCT(id VARCHAR, "name" VARCHAR)[]` | Список форматов работы | Массив объектов |
| **relations** | `VARCHAR[]` | Связи соискателя с вакансией | Required. Enum: favorited, got_response и др. |
| **professional_roles** | `STRUCT(id VARCHAR, "name" VARCHAR)[]` | Список профессиональных ролей | Required. Массив объектов |
| **contacts** | `STRUCT(...)` | Контактная информация | Включает телефоны, email. Объект или null |
| **adv_context** | `VARCHAR` | — | Нет в описании |
| **sort_point_distance** | `DOUBLE` | Расстояние (в метрах) | Между центром сортировки и адресом вакансии |
| **snippet** | `STRUCT(requirement VARCHAR, responsibility VARCHAR)` | Дополнительные текстовые отрывки | Required. Требования и обязанности |
| **branding** | `STRUCT("type" VARCHAR, tariff VARCHAR)` | — | Нет в описании |
| **insider_interview** | `STRUCT(id VARCHAR, url VARCHAR)` | Интервью о жизни в компании | Объект или null |
| **video_vacancy** | `STRUCT(...)` | Видео вакансия | Содержит ссылки на видео, обложки и превью |
| **brand_snippet** | `STRUCT(...)` | — | Нет в описании (брендинговые элементы: лого, фон) |
| **published_at_utc** | `TIMESTAMP` | Дата и время публикации | В описании поле названо `published_at` (Required) |
| **created_at_utc** | `TIMESTAMP` | Дата и время создания | В описании поле названо `created_at` |
| **published_at_offset** | `SMALLINT` | — | Техническое поле (смещение часового пояса), нет в описании |
| **created_at_offset** | `SMALLINT` | — | Техническое поле (смещение часового пояса), нет в описании |
| **day** | `BIGINT` | — | Техническое поле партицирования, нет в описании |
| **month** | `BIGINT` | — | Техническое поле партицирования, нет в описании |
| **year** | `BIGINT` | — | Техническое поле партицирования, нет в описании |

## DevOps, CI/CD

[2025 Dec 3 13:40] Единый docker compose для airflow и minio, тк далее добавятся http сервер, ml сервис и тд.
