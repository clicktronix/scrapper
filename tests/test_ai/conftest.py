"""Фикстуры для тестов AI-модуля."""
import pytest

from src.ai.taxonomy_matching import invalidate_taxonomy_cache


@pytest.fixture(autouse=True)
def _clear_taxonomy_cache():
    """Сбрасывать кэш таксономии перед и после каждого теста."""
    invalidate_taxonomy_cache()
    yield
    invalidate_taxonomy_cache()
