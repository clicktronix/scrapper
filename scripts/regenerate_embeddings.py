"""Перегенерация embedding-векторов для всех проанализированных блогов.

Использует текущий build_embedding_text() — включает quality fields
(engagement_quality, brand_safety_score, content_quality, lifestyle_level,
collaboration_risk), которые улучшают семантический поиск.

Запуск:
    uv run python -m scripts.regenerate_embeddings [--dry-run] [--limit N]
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI
from pydantic import ValidationError
from supabase import create_client

from src.ai.embedding import build_embedding_text, generate_embedding
from src.ai.schemas import AIInsights

# Загружаем .env напрямую (без Settings, которому нужен SCRAPER_API_KEY)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

_EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")


async def main(dry_run: bool = False, limit: int | None = None) -> None:
    """Перегенерировать embeddings для всех блогов с ai_insights."""
    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_SERVICE_KEY"]
    openai_key = os.environ["OPENAI_API_KEY"]

    db = create_client(supabase_url, supabase_key)
    openai_client = AsyncOpenAI(api_key=openai_key)

    # Запрос всех блогов с ai_insights
    query = (
        db.table("blogs")
        .select("id, username, ai_insights")
        .not_.is_("ai_insights", "null")
        .neq("scrape_status", "ai_refused")
        .order("created_at")
    )
    if limit:
        query = query.limit(limit)

    result = query.execute()
    blogs = result.data

    logger.info(f"Найдено {len(blogs)} блогов с ai_insights" + (f" (limit={limit})" if limit else ""))

    if not blogs:
        logger.info("Нечего перегенерировать")
        return

    regenerated = 0
    skipped = 0
    failed = 0

    for i, blog in enumerate(blogs, 1):
        blog_id = blog["id"]
        username = blog.get("username", "?")

        try:
            insights = AIInsights.model_validate(blog["ai_insights"])
        except ValidationError as e:
            logger.error(f"[{i}/{len(blogs)}] @{username} ({blog_id}): невалидный ai_insights: {e}")
            failed += 1
            continue

        text = build_embedding_text(insights)
        if text is None:
            logger.warning(f"[{i}/{len(blogs)}] @{username}: пустой embedding_text, пропускаем")
            skipped += 1
            continue

        if dry_run:
            logger.info(f"[{i}/{len(blogs)}] @{username}: embedding_text ({len(text)} chars) — dry run")
            regenerated += 1
            continue

        vector = await generate_embedding(openai_client, text, model=_EMBEDDING_MODEL)
        if vector is None:
            logger.error(f"[{i}/{len(blogs)}] @{username}: API вернул None")
            failed += 1
            await asyncio.sleep(1.0)  # Backoff при ошибке (возможно rate limit)
            continue

        # NB: синхронный Supabase SDK — скрипт однопоточный, блокировка event loop допустима
        db.table("blogs").update({"embedding": vector}).eq("id", blog_id).execute()
        regenerated += 1
        await asyncio.sleep(0.05)  # Защита от rate limit OpenAI

        if i % 20 == 0:
            logger.info(f"Прогресс: {i}/{len(blogs)} ({regenerated} обновлено)")

    mode = "DRY RUN" if dry_run else "DONE"
    logger.info(
        f"[{mode}] Результат: {regenerated} обновлено, "
        f"{skipped} пропущено, {failed} ошибок (из {len(blogs)})"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Перегенерация embedding-векторов")
    parser.add_argument("--dry-run", action="store_true", help="Только проверить текст, без вызова API")
    parser.add_argument("--limit", type=int, default=None, help="Максимум блогов для обработки")
    args = parser.parse_args()

    asyncio.run(main(dry_run=args.dry_run, limit=args.limit))
