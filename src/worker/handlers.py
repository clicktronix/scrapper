"""Обработчики задач воркера — re-export модуль для обратной совместимости.

Реализации в scrape_handler.py, ai_handler.py, discover_handler.py.

Внешние зависимости импортируются ДО подмодулей, чтобы:
1. Тесты могли патчить через ``src.worker.handlers.mark_task_failed`` и т.д.
2. Подмодули получали те же ссылки через ``import src.worker.handlers as _h``
   (разрешается без цикла, т.к. к моменту импорта подмодулей атрибуты уже связаны).

NB: символы с ``_`` (напр. ``_dedup_brands``, ``_normalize_username``) — внутренние,
экспортируются только для патчинга в тестах.
"""

from loguru import logger  # noqa: F401

from src.ai.batch_api import poll_batch, submit_batch  # noqa: F401
from src.ai.embedding import build_embedding_text, generate_embedding  # noqa: F401
from src.ai.taxonomy_matching import (  # noqa: F401
    load_categories,
    load_cities,
    load_tags,
    match_categories,
    match_city,
    match_tags,
)
from src.database import (  # noqa: F401
    cleanup_orphan_person,
    create_task_if_not_exists,
    is_blog_fresh,
    mark_task_done,
    mark_task_failed,
    mark_task_running,
    sanitize_error,
    upsert_blog,
    upsert_highlights,
    upsert_posts,
)
from src.image_storage import persist_profile_images  # noqa: F401
from src.worker.ai_handler import (  # noqa: F401
    _CONFIDENCE_TO_FLOAT,
    _ENRICHMENT_RETRY_ATTEMPTS,
    _ENRICHMENT_RETRY_DELAY_SECONDS,
    BatchContext,
    _dedup_brands,
    _extract_blog_fields,
    _load_profiles_for_batch,
    _process_blog_result,
    _retry_enrichment,
    handle_ai_analysis,
    handle_batch_results,
)
from src.worker.blog_data import build_blog_data_from_user  # noqa: F401
from src.worker.discover_handler import handle_discover  # noqa: F401
from src.worker.scrape_handler import (  # noqa: F401
    _normalize_username,
    _parse_top_comments,
    handle_full_scrape,
)
