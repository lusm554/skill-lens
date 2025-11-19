# Готовые датасеты вакансий (СНГ/Россия)

Обработка и анализ готовых исторических датасетов вакансий для проекта SkillLens.

## Датасеты отранжированы по релевантности

| **Ранг** | **Название** | **Источник** | **Период данных** | **Объем** | **Платформы** | **Формат** | **Ссылка** | **Примечание** |
|----------|-------------|-------------|------------------|----------|--------------|-----------|----------|---------------|
| 🥇 **1** | **Vacancies from popular agregators** | Mendeley Data | Июнь 2022 - Ноябрь 2023 | **575,957 вакансий** | hh.ru, trudvsem.ru, superjob.ru, rabota.ru | CSV (1.45 GB) | [Mendeley](https://data.mendeley.com/datasets/gkfx465zwk/1) | ✅ Самый большой датасет с историчностью 1.5 года |
| 🥈 **2** | **Raw Jobs Data from HeadHunter Russia** | Kaggle | Не указан (вероятно 2022-2023) | **560,000+ вакансий** | hh.ru | CSV (1.56 GB) | [Kaggle](https://www.kaggle.com/datasets/etietopabraham/jobs-raw-data) | ✅ Большой объем, хорошая документация |
| 🥉 **3** | **Resume-Job Matching Dataset** | Kaggle | Май 2021 - Октябрь 2024 | **~80,000+ откликов** (вакансии + резюме) | hh.ru | CSV | [Kaggle](https://www.kaggle.com/datasets/darysha/hse-hackathon) | ✅ Уникальный: связка резюме-вакансии с результатами откликов |
| **4** | **Data analyst vacancies on HeadHunter for 2023-2024** | Kaggle | Август 2023 - Август 2024 | **839 MB** (~100K+ вакансий) | hh.ru | CSV | [Kaggle](https://www.kaggle.com/datasets/lludop/data-nalyst-vacancies-on-headhunter-for-2023-2024) | ✅ Специализация Data Analyst, свежие данные, хорошее описание |
| **5** | **IT Vacancies from HeadHunter Website** | Kaggle | Сентябрь - Октябрь 2023 | **Не указан** (~10-20K) | hh.ru API | CSV | [Kaggle](https://www.kaggle.com/datasets/ilyazawilsiv/it-vacancies-from-headhunter-website) | ✅ IT-специализация, все регионы России |
| **6** | **IT vacancy data** | Figshare | Не указан | **83.5 MB** (~50-100K вакансий) | hh.ru API | CSV/JSON | [Figshare](https://figshare.com/articles/dataset/it_vacancy_data/19005092) | ✅ IT-специализация, хорошая структура |
| **7** | **Vacancies Scrapped from HH.ru** | Kaggle | Июль - Август 2021 | **26.49 MB** (7 файлов: Java, Kotlin, Python, Data, DS) | hh.ru | CSV | [Kaggle](https://www.kaggle.com/datasets/pavfedotov/heaadhunter-vacancies) | ⚠️ Специализированные вакансии по языкам программирования |
| **8** | **HH.ru IT vacancies (Moscow + SPb)** | Kaggle | 25.10.2021 - 02.12.2021 | **47,330 IT вакансий** | hh.ru | CSV | [Kaggle](https://www.kaggle.com/datasets/vyacheslavpanteleev1/hhru-it-vacancies-from-20211025-to-20211202) | ⚠️ IT-специализация, только Москва + Питер |
| **9** | **Job Posting Data in Russia** | Kaggle | Не указан (обновлен год назад) | **Не указан** | Множественные источники | CSV | [Kaggle](https://www.kaggle.com/datasets/techsalerator/job-posting-data-in-russia) | ⚠️ Comprehensive dataset, мало информации о размере |
| **10** | **Open Vacancies Dataset (Moscow)** | Kaggle | Май 2024 | **Не указан** (ориентировочно 5-10K) | hh.ru API | CSV | [Kaggle](https://www.kaggle.com/datasets/tanelid/open-vacancies-datasetmoscow-may-2024) | ⚠️ Только Москва, свежие данные |
| **11** | **DS vacancies from hh.ru** | Kaggle | Обновлен 3 месяца назад | **422 KB** (~несколько сотен) | hh.ru | CSV | [Kaggle](https://www.kaggle.com/datasets/vagascience/ds-vacancies-from-hh-ru) | ⚠️ Data Science специализация, небольшой объем |
| **12** | **HeadHunter vacancies for data search** | Kaggle | Не указан | **Не указан** | hh.ru | CSV | [Kaggle](https://www.kaggle.com/datasets/antonbelyaevd/headhunter-vacancies-for-data-search) | ⚠️ Мало информации о датасете |
| **13** | **Yandex Jobs** | Kaggle | Не указан | **600+ IT вакансий** | Telegram канал @ya_jobs | CSV | [Kaggle](https://www.kaggle.com/datasets/kirili4ik/yandex-jobs) | ⚠️ Узкая специализация (только Яндекс) |

---

## Дополнительные источники

### Официальные API для дополнения данных:

| **Платформа** | **API доступен** | **Документация** | **Ограничения** |
|--------------|-----------------|-----------------|----------------|
| HeadHunter (hh.ru) | ✅ Да | [GitHub API Docs](https://github.com/hhru/api) | Rate limits |
| SuperJob | ✅ Да | [SuperJob API](https://api.superjob.ru/) | Rate limits |
| Zarplata.ru | ✅ Да | [OpenAPI Docs](https://api.zarplata.ru/openapi/redoc) | Rate limits |
| TrudVsem.ru | ✅ Да | Федеральная служба занятости | Ограниченный доступ |
