"""Импорт блогеров из xlsx файла через API pre_filter."""
import argparse
import asyncio
import os
from typing import Any

import httpx
import pandas as pd
from loguru import logger

from src.database import sanitize_error


def extract_usernames(df: pd.DataFrame) -> list[str]:
    """Извлечь уникальные username-ы из DataFrame."""
    result: list[str] = []
    seen: set[str] = set()
    for raw in df["username"]:
        if not isinstance(raw, str) or not raw.strip():
            continue
        name = raw.strip().lower()
        if name not in seen:
            result.append(name)
            seen.add(name)
    return result


async def send_batch(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    usernames: list[str],
) -> dict[str, Any]:
    """Отправить батч username-ов на API."""
    resp = await client.post(
        f"{base_url}/api/tasks/pre_filter",
        json={"usernames": usernames},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


async def run(
    file_path: str,
    base_url: str,
    api_key: str,
    batch_size: int,
    delay: float,
) -> None:
    """Читает xlsx → шлёт батчами на API."""
    logger.info(f"Читаем {file_path}...")
    df = pd.read_excel(file_path)
    usernames = extract_usernames(df)
    logger.info(f"Найдено {len(usernames)} уникальных username-ов")

    total_created = 0
    total_skipped = 0
    total_errors = 0
    batches = [usernames[i:i + batch_size] for i in range(0, len(usernames), batch_size)]

    async with httpx.AsyncClient() as client:
        for i, batch in enumerate(batches, 1):
            try:
                data = await send_batch(client, base_url, api_key, batch)
                total_created += data.get("created", 0)
                total_skipped += data.get("skipped", 0)
                total_errors += data.get("errors", 0)
                logger.info(
                    f"Батч {i}/{len(batches)}: "
                    f"+{data.get('created', 0)} создано, "
                    f"~{data.get('skipped', 0)} пропущено, "
                    f"!{data.get('errors', 0)} ошибок"
                )
            except httpx.HTTPStatusError as e:
                safe_response = sanitize_error(e.response.text)
                logger.error(f"Батч {i}/{len(batches)} ошибка: {e.response.status_code} {safe_response}")
                total_errors += len(batch)
            except Exception as e:
                logger.error(f"Батч {i}/{len(batches)} ошибка: {sanitize_error(str(e))}")
                total_errors += len(batch)

            if delay > 0 and i < len(batches):
                await asyncio.sleep(delay)

    logger.info(
        f"Импорт завершён: "
        f"создано={total_created}, пропущено={total_skipped}, ошибок={total_errors}"
    )


def _resolve_api_key(cli_api_key: str | None) -> str:
    """Получить API key из аргумента или окружения."""
    if cli_api_key and cli_api_key.strip():
        return cli_api_key.strip()

    env_api_key = os.getenv("SCRAPER_API_KEY", "").strip()
    if env_api_key:
        return env_api_key

    raise ValueError("SCRAPER_API_KEY is required (use --api-key or env SCRAPER_API_KEY)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Импорт блогеров из xlsx")
    parser.add_argument("file", help="Путь к xlsx файлу")
    parser.add_argument("--base-url", default="http://localhost:8001", help="URL скрапера")
    parser.add_argument("--api-key", help="SCRAPER_API_KEY (prefer env SCRAPER_API_KEY)")
    parser.add_argument("--batch-size", type=int, default=100, help="Размер батча")
    parser.add_argument("--delay", type=float, default=0.1, help="Пауза между батчами (сек)")
    args = parser.parse_args()

    api_key = _resolve_api_key(args.api_key)
    asyncio.run(run(args.file, args.base_url, api_key, args.batch_size, args.delay))


if __name__ == "__main__":
    main()
