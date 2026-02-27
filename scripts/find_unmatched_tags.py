"""Скрипт для поиска "изобретённых" тегов, которых нет в справочнике taxonomy.py.

Подключается к Supabase, берёт все блоги с ai_insights,
сравнивает их теги с эталонным списком TAGS и выводит таблицу
незнакомых тегов с частотностью.
"""

import sys
import unicodedata
from collections import Counter
from pathlib import Path

# Добавляем корень проекта в sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.ai.taxonomy import TAGS  # noqa: E402
from src.config import load_settings  # noqa: E402
from supabase import create_client  # noqa: E402


def normalize(tag: str) -> str:
    """Нормализация тега для сравнения: lowercase, strip, ё→е, убрать лишние пробелы."""
    t = tag.lower().strip()
    t = t.replace("ё", "е")
    # Убрать двойные пробелы
    while "  " in t:
        t = t.replace("  ", " ")
    # NFC нормализация Unicode
    t = unicodedata.normalize("NFC", t)
    return t


def build_reference_set() -> set[str]:
    """Собрать множество нормализованных эталонных тегов из TAGS."""
    ref = set()
    for group_tags in TAGS.values():
        for tag in group_tags:
            ref.add(normalize(tag))
    return ref


def main() -> None:
    settings = load_settings()
    client = create_client(
        settings.supabase_url,
        settings.supabase_service_key.get_secret_value(),
    )

    reference = build_reference_set()
    print(f"Эталонных тегов (нормализованных): {len(reference)}")

    # Получаем блоги с ai_insights != null
    # Supabase REST API возвращает max 1000 за раз, пагинируем
    all_rows = []
    page_size = 1000
    offset = 0

    while True:
        resp = (
            client.table("blogs")
            .select("id, ai_insights")
            .not_.is_("ai_insights", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = resp.data
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size

    print(f"Блогов с ai_insights: {len(all_rows)}")

    # Собираем статистику
    unmatched_counter: Counter[str] = Counter()
    total_tags = 0
    matched_count = 0
    unmatched_count = 0
    blogs_with_tags = 0

    for row in all_rows:
        insights = row.get("ai_insights")
        if not insights or not isinstance(insights, dict):
            continue

        tags = insights.get("tags")
        if not tags or not isinstance(tags, list):
            continue

        blogs_with_tags += 1

        for tag in tags:
            if not isinstance(tag, str):
                continue
            total_tags += 1
            norm = normalize(tag)
            if norm in reference:
                matched_count += 1
            else:
                unmatched_count += 1
                # Сохраняем оригинальное написание первого встреченного варианта
                unmatched_counter[norm] += 1

    print(f"Блогов с тегами: {blogs_with_tags}")
    print(f"Всего тегов (вхождений): {total_tags}")
    print(f"Совпавших: {matched_count} ({matched_count/total_tags*100:.1f}%)" if total_tags else "Совпавших: 0")
    print(f"Несовпавших: {unmatched_count} ({unmatched_count/total_tags*100:.1f}%)" if total_tags else "Несовпавших: 0")
    print(f"Уникальных несовпавших тегов: {len(unmatched_counter)}")
    print()

    # Собираем оригинальные написания для каждого нормализованного тега
    orig_map: dict[str, str] = {}
    for row in all_rows:
        insights = row.get("ai_insights")
        if not insights or not isinstance(insights, dict):
            continue
        tags = insights.get("tags")
        if not tags or not isinstance(tags, list):
            continue
        for tag in tags:
            if not isinstance(tag, str):
                continue
            norm = normalize(tag)
            if norm in unmatched_counter and norm not in orig_map:
                orig_map[norm] = tag.strip()

    # Сортировка по частоте (убывание)
    sorted_tags = unmatched_counter.most_common()

    # Вывод таблицы
    print(f"{'#':<5} {'Тег':<60} {'Кол-во':>8}")
    print("-" * 75)
    for i, (norm_tag, count) in enumerate(sorted_tags, 1):
        original = orig_map.get(norm_tag, norm_tag)
        print(f"{i:<5} {original:<60} {count:>8}")


if __name__ == "__main__":
    main()
