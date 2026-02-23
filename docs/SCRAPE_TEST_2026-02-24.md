# Тестовый запуск скраппинга — 2026-02-24

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
| HikerAPI баланс ДО | $21.11 |
| HikerAPI баланс ПОСЛЕ | нужно проверить |
| HikerAPI стоимость на профиль | нужно проверить |
| OpenAI Batch стоимость (всего) | **$0.045** |
| OpenAI Batch стоимость на профиль | **$0.0023** |

## Блогеры (20 профилей, все pending на старте)

| # | Username | Подписчики ДО | Подписчики ПОСЛЕ | Имя | Примечание |
|---|----------|--------------|-----------------|-----|------------|
| 1 | sekavines | 7,185,908 | 7,186,271 | Toktarbek Sergazy | сброшен |
| 2 | aminaxo26 | 6,455,219 | 6,454,171 | Amina Nurtaza | сброшен |
| 3 | nnnurdaylet | 5,210,362 | 5,209,584 | Нурдаулет (ПРОСТОЙ КАЗАХ) | сброшен |
| 4 | dragon_knm | 3,757,242 | 6,213,948 | Unknown → family blogger | followers обновились! |
| 5 | sultankyzy | 3,400,000 | 3,562,011 | Ляйля Султанкызы | |
| 6 | saltanat_bakayeva | 3,100,000 | 3,181,709 | Салтанат Бакаева | |
| 7 | dana_yesseyeva | 3,040,000 | 3,030,557 | Дана Есеева | |
| 8 | djurinskaya | 3,000,812 | 3,016,658 | Жания Джуринская | |
| 9 | territima | 2,875,000 | 2,832,104 | Теритима (В Астане) | |
| 10 | zhuldyzabdukarimovaofficial | 2,800,000 | 2,933,689 | Жулдыз Абдукаримова | |
| 11 | aii_officiiall | 2,400,000 | 2,578,659 | Ай | |
| 12 | 100baksoff_almaty | 2,300,000 | 2,315,004 | 100baksoff_almaty | |
| 13 | tengrinewskz | 2,300,000 | 2,475,703 | Tengri News | |
| 14 | kagiristwins | 2,249,294 | 2,181,594 | Томирис и Наргиз Канатовы | |
| 15 | yuniypovar | 2,206,328 | 2,212,123 | Дулат Тантаев (Юный Повар) | сброшен |
| 16 | sadraddinn | 2,200,000 | 2,355,603 | Sadraddin | |
| 17 | good_zhan | 2,200,000 | 2,298,918 | good_zhan | |
| 18 | abilovva_m | 2,200,000 | 2,248,627 | Мерей Абилова | |
| 19 | ztb_kz | 2,100,000 | 2,302,805 | ZTB | |
| 20 | molyajan_ | 2,100,000 | — | Молдир | не попал в выборку (21-й) |

> molyajan_ не попал в топ-20 по обновлённым данным — его место занял dragon_knm (6.2M вместо 3.7M).

## Тайминги

| Этап | Время начала (UTC) | Время окончания (UTC) | Длительность |
|------|-------------------|-----------------------|-------------|
| full_scrape (20 профилей) | 19:40:58 | 19:46:35 | **~5 мин 37 сек** |
| ai_analysis batch submit | ~19:47 | — | — |
| ai_analysis batch complete | — | 20:08:34 | — |
| Общее (scrape → ai done) | 19:40:58 | 20:08:34 | **~27 мин 36 сек** |

**Скорость full_scrape**: ~16.9 сек/профиль (20 профилей за 5.6 мин)
**Ожидание AI batch**: ~21 мин (submit → complete, OpenAI Batch API + 15-мин poll interval)

## Результаты full_scrape

| # | Username | Статус | Постов | Хайлайтов | ER Posts | ER Reels | ER Trend | Posts/week |
|---|----------|--------|--------|-----------|----------|----------|----------|------------|
| 1 | sekavines | active | 12 | 1 | — | 0.57% | declining | 1.95 |
| 2 | aminaxo26 | active | 12 | 1 | 0.77% | 1.93% | stable | 0.08 |
| 3 | nnnurdaylet | active | 12 | 1 | 13.27% | 3.07% | declining | 0.04 |
| 4 | dragon_knm | active | 12 | 1 | 1.02% | 7.68% | declining | 0.15 |
| 5 | sultankyzy | active | 12 | 1 | 0.38% | 0.42% | growing | 0.08 |
| 6 | saltanat_bakayeva | active | 12 | 0 | 0.07% | 0.03% | declining | 0.72 |
| 7 | dana_yesseyeva | active | 12 | 1 | 1.37% | 0.15% | declining | 0.06 |
| 8 | djurinskaya | active | 12 | 1 | 0.28% | 0.02% | growing | 7.63 |
| 9 | territima | active | 12 | 1 | 0.16% | 0.75% | stable | 2.27 |
| 10 | zhuldyzabdukarimovaofficial | active | 12 | 1 | 0.89% | 0.43% | growing | 4.66 |
| 11 | aii_officiiall | active | 12 | 1 | 7.76% | 1.29% | declining | 0.41 |
| 12 | 100baksoff_almaty | active | 12 | 1 | 0.14% | 0.01% | declining | 3.86 |
| 13 | tengrinewskz | active | 12 | 1 | 0.26% | 0.41% | declining | 29.12 |
| 14 | kagiristwins | active | 12 | 1 | 0.04% | 0.24% | stable | 2.81 |
| 15 | yuniypovar | active | 12 | 1 | — | 5.52% | declining | 0.12 |
| 16 | sadraddinn | active | 12 | 0 | — | 1.16% | declining | 0.50 |
| 17 | good_zhan | active | 12 | 1 | — | 0.88% | growing | 0.12 |
| 18 | abilovva_m | active | 12 | 1 | 6.66% | 4.51% | declining | 0.08 |
| 19 | ztb_kz | active | 12 | 1 | 0.49% | 0.64% | stable | 21.09 |

**Ошибок: 0/20**

## Результаты ai_analysis

| # | Username | Batch ID | Confidence | Основная тема |
|---|----------|----------|------------|--------------|
| 1 | sekavines | batch_699caeb8... | 0.80 | lifestyle |
| 2 | aminaxo26 | batch_699caeb8... | 0.85 | lifestyle |
| 3 | nnnurdaylet | batch_699caeb8... | 0.75 | lifestyle |
| 4 | dragon_knm | batch_699caeb8... | 0.80 | family |
| 5 | sultankyzy | batch_699caeb8... | 0.75 | lifestyle |
| 6 | saltanat_bakayeva | batch_699caeb8... | 0.80 | entertainment |
| 7 | dana_yesseyeva | batch_699caeb8... | 0.80 | lifestyle |
| 8 | djurinskaya | batch_699caeb8... | 0.80 | lifestyle |
| 9 | territima | batch_699caeb8... | 0.88 | lifestyle |
| 10 | zhuldyzabdukarimovaofficial | batch_699caeb8... | 0.75 | lifestyle |
| 11 | aii_officiiall | batch_699caeb8... | 0.65 | lifestyle |
| 12 | 100baksoff_almaty | batch_699caeb8... | 0.80 | business |
| 13 | tengrinewskz | batch_699caeb8... | 0.80 | business |
| 14 | kagiristwins | batch_699caeb8... | 0.80 | beauty |
| 15 | yuniypovar | batch_699caeb8... | 0.85 | food |
| 16 | sadraddinn | batch_699caeb8... | 0.90 | music |
| 17 | good_zhan | batch_699caeb8... | 0.75 | fashion |
| 18 | abilovva_m | batch_699caeb8... | 0.80 | beauty |
| 19 | ztb_kz | batch_699caeb8... | 0.75 | business |

**Средний confidence: 0.79** | Один batch на все 20 профилей | Ошибок: 0

Распределение тем: lifestyle (9), business (3), beauty (2), family (1), entertainment (1), food (1), music (1), fashion (1)

## OpenAI Batch API — детальная аналитика

**Batch ID**: `batch_699caeb8fff481908cfb1a49b467a646`
**Модель**: `gpt-5-nano-2025-08-07` (reasoning model)
**Статус**: 20/20 completed, 0 ошибок, 0 refusals

### Токены

| Метрика | Значение |
|---------|----------|
| Prompt tokens (всего) | 177,848 |
| — Cached (51%) | 90,624 |
| — Uncached (49%) | 87,224 |
| Completion tokens (всего) | 213,149 |
| — Reasoning (90%) | 190,912 |
| — Visible output (10%) | 22,237 |
| **Total tokens** | **390,997** |

### Стоимость (gpt-5-nano Batch API, 50% скидка)

| Категория | Токены | Цена за 1M | Стоимость |
|-----------|--------|-----------|-----------|
| Uncached input | 87,224 | $0.025 | $0.0022 |
| Cached input | 90,624 | $0.0025 | $0.0002 |
| Output (reasoning + visible) | 213,149 | $0.20 | $0.0426 |
| **Итого** | **390,997** | — | **$0.045** |
| **На профиль** | ~19,550 | — | **$0.0023** |

### Статистика по запросам

| Метрика | Min | Max | Avg |
|---------|-----|-----|-----|
| Prompt tokens | 7,471 | 10,129 | 8,892 |
| Completion tokens | 8,028 | 14,496 | 10,657 |
| Total tokens | 16,664 | 23,265 | 19,550 |

### Прогноз стоимости

| Масштаб | Стоимость OpenAI |
|---------|-----------------|
| 100 профилей | ~$0.23 |
| 500 профилей | ~$1.13 |
| 1,000 профилей | ~$2.25 |
| 5,000 профилей | ~$11.26 |

> **Наблюдения:**
> - 90% completion tokens — reasoning (внутренние рассуждения модели), только 10% — видимый JSON-output
> - 51% prompt tokens — cached (OpenAI переиспользует общий system prompt между запросами в batch)
> - Output-тяжёлый pipeline: 95% стоимости — за output tokens (reasoning дорогой)
> - Стоимость крайне низкая: $0.0023/профиль — можно анализировать тысячи блогеров за копейки

## Проблемы и наблюдения

- **language, tone, quality_score = NULL**: Поля в `ai_insights.content` не заполняются — возможно изменилась схема AIInsights или промпт не запрашивает эти поля
- **ER Posts = NULL** у 4 блогеров (sekavines, yuniypovar, sadraddinn, good_zhan) — вероятно только Reels-контент, нет фото-постов для расчёта
- **12 постов у всех** — вместо ожидаемых 25 (posts_to_fetch=25). Возможно HikerAPI возвращает меньше или фильтрация по дате
- **dragon_knm followers скачок**: 3.7M → 6.2M — старое значение в БД было устаревшим (ручной ввод), теперь актуальное
- **0 хайлайтов** у saltanat_bakayeva и sadraddinn — либо нет хайлайтов, либо приватные
- **nnnurdaylet ER Posts = 13.27%** — аномально высокий, возможно мало постов (всего видео-контент)
- **Один batch на 20** — batch_min_size=10, но все 20 попали в один batch (накопились за время скраппинга)

## Выводы

- full_scrape через HikerAPI стабильно работает: 20/20 без ошибок, ~17 сек/профиль
- AI Batch pipeline работает end-to-end: submit → poll → parse → upsert
- Основное ожидание — poll_batches interval (15 мин), сам OpenAI batch готов быстрее
- Нужно разобраться с NULL полями в ai_insights (language, tone, quality_score)
- Нужно проверить почему всего 12 постов вместо 25
