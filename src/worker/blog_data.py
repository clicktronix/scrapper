"""Хелпер для построения blog_insert_data из сырого HikerAPI user dict."""

from typing import Any


def build_blog_data_from_user(
    user: dict[str, Any],
    *,
    person_id: str,
    username: str,
    source: str = "xlsx_import",
    scrape_status: str = "pending",
) -> dict[str, Any]:
    """Построить словарь для insert в таблицу blogs из сырого ответа HikerAPI.

    Извлекает все доступные поля профиля: метрики, контактные данные,
    тип аккаунта, bio links. Используется в pre_filter_handler и discover_handler.
    """
    data: dict[str, Any] = {
        "person_id": person_id,
        "platform": "instagram",
        "username": username,
        "platform_id": str(user.get("pk", "")),
        "followers_count": user.get("follower_count"),
        "following_count": user.get("following_count"),
        "media_count": user.get("media_count"),
        "bio": user.get("biography") or "",
        "is_verified": user.get("is_verified", False),
        "is_business": user.get("is_business", False),
        "source": source,
        "scrape_status": scrape_status,
    }
    # Опциональные поля — добавляем только если заданы
    business_category = user.get("business_category_name") or user.get("category_name")
    if business_category:
        data["business_category"] = business_category
    if user.get("account_type") is not None:
        data["account_type"] = user["account_type"]
    if user.get("public_email"):
        data["public_email"] = user["public_email"]
    if user.get("contact_phone_number"):
        data["contact_phone_number"] = user["contact_phone_number"]
    if user.get("public_phone_country_code"):
        data["public_phone_country_code"] = user["public_phone_country_code"]
    if user.get("city_name"):
        data["city_name"] = user["city_name"]
    if user.get("address_street"):
        data["address_street"] = user["address_street"]
    if user.get("external_url"):
        data["external_url"] = user["external_url"]
    # Bio links
    raw_bio_links = user.get("bio_links") or []
    if raw_bio_links:
        bio_links = []
        for link in raw_bio_links:
            if isinstance(link, dict) and link.get("url"):
                bio_links.append({
                    "url": str(link["url"]),
                    "title": link.get("title") or None,
                    "link_type": link.get("link_type") or None,
                })
        if bio_links:
            data["bio_links"] = bio_links
    return data
