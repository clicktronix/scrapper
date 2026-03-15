"""Supabase-реализация BlogRepository."""

from datetime import UTC, datetime, timedelta
from typing import Any

from loguru import logger
from supabase import AsyncClient


class SupabaseBlogRepository:
    """Supabase-реализация операций с блогами."""

    def __init__(self, db: AsyncClient) -> None:
        self._db = db

    async def is_fresh(self, blog_id: str, min_days: int) -> bool:
        """Проверить, скрапился ли блог менее min_days дней назад."""
        threshold = (datetime.now(UTC) - timedelta(days=min_days)).isoformat()
        result = await self._db.table("blogs") \
            .select("scraped_at") \
            .eq("id", blog_id) \
            .gt("scraped_at", threshold) \
            .limit(1) \
            .execute()
        return bool(result.data)

    async def upsert(self, blog_id: str, data: dict[str, Any]) -> None:
        """Обновить данные блога из скрапинга."""
        await self._db.table("blogs").update(data).eq("id", blog_id).execute()

    async def upsert_posts(
        self, blog_id: str, posts: list[dict[str, Any]]
    ) -> None:
        """Upsert постов блогера. ON CONFLICT (blog_id, platform_id) DO UPDATE."""
        if not posts:
            return
        rows = [{**post, "blog_id": blog_id} for post in posts]
        await self._db.table("blog_posts").upsert(
            rows, on_conflict="blog_id,platform_id"
        ).execute()

    async def upsert_highlights(
        self, blog_id: str, highlights: list[dict[str, Any]]
    ) -> None:
        """Upsert хайлайтов блогера."""
        if not highlights:
            return
        rows = [{**h, "blog_id": blog_id} for h in highlights]
        await self._db.table("blog_highlights").upsert(
            rows, on_conflict="blog_id,platform_id"
        ).execute()

    async def cleanup_orphan_person(self, person_id: str) -> None:
        """Удалить person без привязанных блогов (best-effort cleanup)."""
        try:
            blogs = await self._db.table("blogs").select("id").eq("person_id", person_id).limit(1).execute()
            if not blogs.data:
                await self._db.table("persons").delete().eq("id", person_id).execute()
        except Exception as e:
            logger.warning(f"[cleanup_orphan] Ошибка очистки person {person_id}: {e}")
