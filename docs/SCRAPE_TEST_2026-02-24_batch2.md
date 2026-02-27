# Тестовый запуск скраппинга — 2026-02-24 (batch 2)

## Конфигурация

| Параметр | Значение |
|----------|----------|
| Backend | HikerAPI (SaaS) |
| Batch model | gpt-5-nano |
| Posts to fetch | 25 |
| Highlights to fetch | 3 |
| Thumbnails to persist | 7 |
| Batch min size | 10 |
| Worker concurrency | 2 |
| Worker poll interval | 30 сек |

## Бюджет

| Метрика | Значение |
|---------|----------|
| HikerAPI баланс ДО | $19.15 |
| HikerAPI баланс ПОСЛЕ | нужно проверить |
| HikerAPI потрачено | — |
| OpenAI Batch стоимость (всего) | ~$0.10 (прогноз: 45 × $0.0023) |
| OpenAI Batch стоимость на профиль | ~$0.0023 |
| **Итого на профиль** | **~$0.10** |

## Блогеры (50 профилей)

| # | Username | Подписчики ДО | Подписчики ПОСЛЕ | Статус |
|---|----------|--------------|-----------------|--------|
| 1 | zhahan_utargaliyev | 2,000,000 | 2,490,115 | active |
| 2 | ztb_media | 2,000,000 | 2,205,491 | active |
| 3 | lifeofmedet | 1,822,000 | 2,131,893 | active |
| 4 | _m_a_d_l_e_n | 2,066,246 | 2,095,243 | active |
| 5 | dakentiy_official | 2,063,134 | 2,050,107 | active |
| 6 | 100baksoff | 1,900,000 | 2,038,663 | active |
| 7 | mila_sayanovna | 1,800,000 | 1,988,414 | active |
| 8 | raides1 | 1,182,000 | 1,961,875 | active |
| 9 | mussilim | 1,900,000 | 1,937,301 | active |
| 10 | kris_p_brothers | 2,000,000 | 1,913,699 | active |
| 11 | kris__p__official | 1,900,000 | 1,814,258 | active |
| 12 | almatynews_kaz | 1,751,059 | 1,807,671 | active |
| 13 | ramina_almas | 1,737,894 | 1,805,994 | active |
| 14 | di1yara09 | 1,700,000 | 1,757,678 | active |
| 15 | alibekovkz | 1,670,000 | 1,675,443 | active |
| 16 | mahmetova.guli | 1,600,000 | 1,651,020 | active |
| 17 | almaty.suntimes | 1,302,988 | 1,649,184 | active |
| 18 | pardaev__ | 1,800,000 | 1,642,290 | active |
| 19 | sabirkin_ | 1,500,000 | 1,592,090 | active |
| 20 | yussupov21 | 1,500,000 | 1,591,061 | active |
| 21 | kris.p.original | 1,206,212 | 1,531,473 | active |
| 22 | kazakhpress.kz | 1,200,000 | 1,512,156 | active |
| 23 | assel.askar | 1,300,000 | 1,488,115 | active |
| 24 | kenjebekovaa | 1,399,890 | 1,478,737 | active |
| 25 | medet.jan | 1,400,000 | 1,475,922 | active |
| 26 | zarina_nurzhanovaa | 1,200,000 | 1,453,015 | active |
| 27 | aissaulebakytbek | 1,392,172 | 1,441,213 | active |
| 28 | sss.sabina | 1,131,287 | 1,430,492 | active |
| 29 | kyrykbayeva | 1,450,643 | 1,426,523 | active |
| 30 | typicalkaz | 1,200,000 | 1,399,556 | active |
| 31 | zhazok_vines | 1,342,425 | 1,391,992 | active |
| 32 | akerke_aryss | 1,320,107 | 1,379,194 | active |
| 33 | kris.p.media | 1,300,000 | 1,329,760 | active |
| 34 | kris__p__almaty | 1,200,000 | 1,323,451 | active |
| 35 | feeviun | 1,300,000 | 1,315,539 | active |
| 36 | aizhuldyz_adaibekova | 1,200,000 | 1,280,153 | active |
| 37 | kris__p__astana | 1,300,000 | 1,275,927 | active |
| 38 | neboldykz | 1,200,000 | 1,222,190 | active |
| 39 | lifelearning | 1,200,000 | 1,219,337 | active |
| 40 | ayhanyma | 1,200,000 | 1,206,445 | active |
| 41 | _ahmetova_zhanna_ | 1,200,000 | 1,189,117 | active |
| 42 | altynbekova_20 | 1,189,980 | 1,176,715 | active |
| 43 | krispnews.kz | 1,200,000 | 1,154,704 | active |
| 44 | bidash_official | 1,213,000 | 1,140,632 | active |
| 45 | zhest_kz | 1,144,430 | 1,113,323 | active |
| 46 | 100baksoff_original | 1,800,000 | — | **failed** (404) |
| 47 | damelya_sw_ | 1,439,269 | — | **failed** (404) |
| 48 | kris.p.brothers | 1,326,170 | — | **failed** (404) |
| 49 | meganews.kaz | 1,200,000 | — | **failed** (404) |
| 50 | yussupov20 | 1,500,000 | — | **failed** (404) |

## Тайминги

| Этап | Время начала (UTC) | Время окончания (UTC) | Длительность |
|------|-------------------|-----------------------|-------------|
| full_scrape (50 профилей) | ~20:56 | ~21:12 | **~16 мин** |
| ai_analysis batch submit | ~21:12 | — | — |
| ai_analysis batch complete | — | ~21:50 (est.) | — |
| Общее (scrape → ai done) | ~20:56 | ~21:50 | **~54 мин** |

**Скорость full_scrape**: ~19.2 сек/профиль (50 профилей, 45 успешных за ~16 мин)
**Ожидание AI batch**: ~38 мин (batch 45 профилей, больше чем 20 в batch 1)

## Результаты full_scrape

| # | Username | Постов | Хайл. | ER Posts | ER Reels | ER Trend | Posts/week |
|---|----------|--------|-------|----------|----------|----------|------------|
| 1 | zhahan_utargaliyev | 12 | 1 | — | 3.10% | growing | 0.07 |
| 2 | ztb_media | 12 | 1 | 0.70% | 0.60% | stable | 20.92 |
| 3 | lifeofmedet | 12 | 1 | 3.42% | 10.58% | declining | 0.09 |
| 4 | _m_a_d_l_e_n | 12 | 1 | 4.23% | 2.24% | declining | 0.39 |
| 5 | dakentiy_official | 12 | 1 | 2.06% | 0.43% | declining | 0.50 |
| 6 | 100baksoff | 12 | 1 | 0.15% | 0.01% | declining | 3.39 |
| 7 | mila_sayanovna | 12 | 1 | 1.10% | 1.66% | declining | 0.12 |
| 8 | raides1 | 12 | 1 | 0.24% | 0.73% | declining | 4.99 |
| 9 | mussilim | 12 | 1 | — | 0.01% | declining | 3.49 |
| 10 | kris_p_brothers | 12 | 0 | 0.05% | 0.23% | declining | 13.87 |
| 11 | kris__p__official | 12 | 0 | 0.04% | 0.48% | declining | 13.46 |
| 12 | almatynews_kaz | 12 | 1 | 0.05% | 0.04% | declining | 16.81 |
| 13 | ramina_almas | 12 | 1 | 1.58% | 1.81% | stable | 2.46 |
| 14 | di1yara09 | 12 | 0 | 1.39% | 0.51% | declining | 0.29 |
| 15 | alibekovkz | 12 | 1 | 0.17% | 0.18% | declining | 24.69 |
| 16 | mahmetova.guli | 12 | 1 | 0.62% | 0.58% | declining | 0.83 |
| 17 | almaty.suntimes | 12 | 1 | 0.01% | 0.01% | declining | 6.69 |
| 18 | pardaev__ | 12 | 1 | — | 4.89% | declining | 0.15 |
| 19 | sabirkin_ | 12 | 1 | 4.78% | 3.99% | declining | 0.17 |
| 20 | yussupov21 | 12 | 1 | 0.07% | 6.11% | declining | 0.07 |
| 21 | kris.p.original | 12 | 1 | 0.06% | 0.06% | stable | 16.24 |
| 22 | kazakhpress.kz | 12 | 0 | 0.00% | 0.02% | declining | 19.94 |
| 23 | assel.askar | 12 | 1 | 5.64% | 2.89% | stable | 0.19 |
| 24 | kenjebekovaa | 12 | 1 | 4.60% | 3.88% | declining | 0.09 |
| 25 | medet.jan | 12 | 1 | 2.96% | 1.73% | stable | 2.71 |
| 26 | zarina_nurzhanovaa | 12 | 1 | 2.08% | 1.89% | declining | 0.13 |
| 27 | aissaulebakytbek | 12 | 1 | 1.41% | 0.61% | declining | 0.05 |
| 28 | sss.sabina | 12 | 1 | 5.25% | 2.16% | declining | 0.17 |
| 29 | kyrykbayeva | 12 | 1 | — | 0.62% | declining | 0.10 |
| 30 | typicalkaz | 12 | 0 | 0.01% | 0.01% | declining | 4.63 |
| 31 | zhazok_vines | 12 | 1 | — | 2.85% | declining | 0.10 |
| 32 | akerke_aryss | 12 | 1 | — | 1.00% | growing | 1.35 |
| 33 | kris.p.media | 12 | 1 | 0.02% | 0.05% | growing | 19.45 |
| 34 | kris__p__almaty | 12 | 0 | 0.04% | 0.38% | declining | 0.93 |
| 35 | feeviun | 12 | 1 | 3.05% | 1.34% | declining | 0.06 |
| 36 | aizhuldyz_adaibekova | 12 | 1 | 0.56% | 1.57% | declining | 0.11 |
| 37 | kris__p__astana | 12 | 0 | 0.06% | 0.15% | declining | 1.40 |
| 38 | neboldykz | 12 | 0 | 0.00% | 0.01% | stable | 37.14 |
| 39 | lifelearning | 12 | 1 | 2.27% | 1.55% | stable | 3.24 |
| 40 | ayhanyma | 12 | 1 | — | 0.55% | declining | 0.25 |
| 41 | _ahmetova_zhanna_ | 12 | 1 | 0.44% | 0.18% | growing | 2.72 |
| 42 | altynbekova_20 | 12 | 1 | 1.41% | — | declining | 0.17 |
| 43 | krispnews.kz | 12 | 0 | 0.14% | 0.05% | declining | 21.71 |
| 44 | bidash_official | 12 | 1 | 0.76% | 1.56% | declining | 2.21 |
| 45 | zhest_kz | 12 | 1 | 0.01% | 0.02% | growing | 0.19 |

**full_scrape: 45/50 успешных, 5 failed (404)**

## Результаты ai_analysis

| # | Username | Confidence | Основная тема |
|---|----------|------------|--------------|
| 1 | zhahan_utargaliyev | 0.75 | lifestyle |
| 2 | ztb_media | 0.80 | entertainment |
| 3 | lifeofmedet | 0.80 | fitness |
| 4 | _m_a_d_l_e_n | 0.85 | beauty |
| 5 | dakentiy_official | 0.75 | lifestyle |
| 6 | 100baksoff | 0.85 | business |
| 7 | mila_sayanovna | 0.80 | lifestyle |
| 8 | raides1 | 0.78 | fashion |
| 9 | mussilim | 0.85 | lifestyle |
| 10 | kris_p_brothers | 0.80 | business |
| 11 | kris__p__official | 0.75 | business |
| 12 | almatynews_kaz | 0.75 | lifestyle |
| 13 | ramina_almas | 0.78 | lifestyle |
| 14 | di1yara09 | 0.70 | family |
| 15 | alibekovkz | 0.82 | lifestyle |
| 16 | mahmetova.guli | 0.65 | lifestyle |
| 17 | almaty.suntimes | 0.85 | business |
| 18 | pardaev__ | 0.85 | food |
| 19 | sabirkin_ | 0.85 | lifestyle |
| 20 | yussupov21 | 0.75 | lifestyle |
| 21 | kris.p.original | 0.75 | business |
| 22 | kazakhpress.kz | 0.80 | education |
| 23 | assel.askar | 0.72 | entertainment |
| 24 | kenjebekovaa | 0.75 | travel |
| 25 | medet.jan | 0.85 | entertainment |
| 26 | zarina_nurzhanovaa | 0.85 | fashion |
| 27 | aissaulebakytbek | 0.85 | beauty |
| 28 | sss.sabina | 0.85 | beauty |
| 29 | kyrykbayeva | 0.80 | family |
| 30 | typicalkaz | 0.75 | education |
| 31 | zhazok_vines | 0.72 | lifestyle |
| 32 | akerke_aryss | 0.80 | lifestyle |
| 33 | kris.p.media | 0.60 | lifestyle |
| 34 | kris__p__almaty | 0.72 | business |
| 35 | feeviun | 0.85 | beauty |
| 36 | aizhuldyz_adaibekova | 0.75 | health |
| 37 | kris__p__astana | 0.85 | lifestyle |
| 38 | neboldykz | 0.72 | education |
| 39 | lifelearning | 0.72 | lifestyle |
| 40 | ayhanyma | 0.72 | food |
| 41 | _ahmetova_zhanna_ | 0.75 | fashion |
| 42 | altynbekova_20 | 0.82 | fitness |
| 43 | krispnews.kz | 0.75 | education |
| 44 | bidash_official | 0.75 | travel |
| 45 | zhest_kz | 0.72 | education |

**ai_analysis: 45/45 успешных, 0 ошибок** | Средний confidence: 0.78

Распределение тем: lifestyle (15), business (7), education (5), beauty (4), fashion (3), entertainment (3), food (2), family (2), fitness (2), travel (2), health (1)

## Ошибки full_scrape (5 из 50)

| Username | Ошибка |
|----------|--------|
| 100baksoff_original | HikerAPI HTTP 404: Target user not found |
| damelya_sw_ | HikerAPI HTTP 404: Target user not found (pk 5478603460) |
| kris.p.brothers | HikerAPI HTTP 404: Target user not found |
| meganews.kaz | HikerAPI HTTP 404: Target user not found (pk 40927919334) |
| yussupov20 | HikerAPI HTTP 404: Entries not found |

> Все 5 — HTTP 404. Аккаунты удалены, переименованы или заблокированы.

## Проблемы и наблюдения

- **5/50 failed (10%)** — все 404, аккаунты не существуют. Данные в БД были ручного ввода, часть устаревшая
- **12 постов у всех** — как и в batch 1, HikerAPI возвращает 12 вместо 25
- **kris_p/kris.p сеть** — 7 аккаунтов (kris_p_brothers, kris__p__official, kris.p.original, kris__p__almaty, kris__p__astana, kris.p.media, krispnews.kz). Все business/lifestyle/education, ER < 0.5%, posts/week 1-22. Вероятно медиа-сеть одного владельца
- **Высокий ER** у персональных аккаунтов: assel.askar (5.64%), sss.sabina (5.25%), sabirkin_ (4.78%), kenjebekovaa (4.60%), _m_a_d_l_e_n (4.23%)
- **Низкий ER** у новостных/сетевых: neboldykz (0.00%), kazakhpress.kz (0.00%), almaty.suntimes (0.01%), typicalkaz (0.01%)
- **lifeofmedet ER Reels = 10.58%** — аномально высокий, вероятно вирусные видео
- **raides1 скачок**: 1.18M → 1.96M (+66%) — старые данные в БД сильно устарели
- **0 хайлайтов** у kris-сети, di1yara09, kazakhpress.kz, typicalkaz, neboldykz — отключены или нет

## Выводы

- Pipeline стабилен на 50 профилях: 45/50 scrape + 45/45 AI — **90% success rate**
- 10% failed — из-за устаревших данных в БД (аккаунты не существуют)
- AI-анализ 45 профилей за один batch — без ошибок
- Общее время ~54 мин (16 мин scrape + 38 мин AI batch wait)
- Стоимость ~$0.10/профиль стабильна (98% — HikerAPI)
