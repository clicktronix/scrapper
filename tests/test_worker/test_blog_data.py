"""Тесты хелпера build_blog_data_from_user."""

from src.worker.blog_data import build_blog_data_from_user


class TestBuildBlogDataFromUser:
    """build_blog_data_from_user — построение blog_insert_data из сырого HikerAPI ответа."""

    def test_basic_fields(self) -> None:
        """Базовые обязательные поля всегда присутствуют."""
        user = {"pk": 12345, "follower_count": 50000, "biography": "Hello"}
        result = build_blog_data_from_user(user, person_id="p-1", username="test_user")

        assert result["person_id"] == "p-1"
        assert result["platform"] == "instagram"
        assert result["username"] == "test_user"
        assert result["platform_id"] == "12345"
        assert result["followers_count"] == 50000
        assert result["bio"] == "Hello"
        assert result["source"] == "xlsx_import"
        assert result["scrape_status"] == "pending"

    def test_all_optional_fields(self) -> None:
        """Все опциональные поля из HikerAPI корректно маппятся."""
        user = {
            "pk": 99,
            "follower_count": 100000,
            "following_count": 500,
            "media_count": 300,
            "biography": "Bio text",
            "is_verified": True,
            "is_business": True,
            "business_category_name": "Beauty",
            "account_type": 2,
            "public_email": "test@example.com",
            "contact_phone_number": "+71234567890",
            "public_phone_country_code": "RU",
            "city_name": "Moscow",
            "address_street": "Main St 1",
            "external_url": "https://example.com",
            "bio_links": [
                {"url": "https://link1.com", "title": "Site", "link_type": "external"},
                {"url": "https://link2.com"},
            ],
        }
        result = build_blog_data_from_user(user, person_id="p-2", username="full_user")

        assert result["following_count"] == 500
        assert result["media_count"] == 300
        assert result["is_verified"] is True
        assert result["is_business"] is True
        assert result["business_category"] == "Beauty"
        assert result["account_type"] == 2
        assert result["public_email"] == "test@example.com"
        assert result["contact_phone_number"] == "+71234567890"
        assert result["public_phone_country_code"] == "RU"
        assert result["city_name"] == "Moscow"
        assert result["address_street"] == "Main St 1"
        assert result["external_url"] == "https://example.com"
        assert len(result["bio_links"]) == 2
        assert result["bio_links"][0]["title"] == "Site"
        assert result["bio_links"][1]["title"] is None

    def test_missing_optional_fields_not_included(self) -> None:
        """Пустые опциональные поля не добавляются в результат."""
        user = {"pk": 1, "follower_count": 1000}
        result = build_blog_data_from_user(user, person_id="p-3", username="minimal")

        assert "business_category" not in result
        assert "account_type" not in result
        assert "public_email" not in result
        assert "city_name" not in result
        assert "external_url" not in result
        assert "bio_links" not in result

    def test_custom_source_and_status(self) -> None:
        """source и scrape_status можно переопределить."""
        user = {"pk": 1, "follower_count": 1000}
        result = build_blog_data_from_user(
            user, person_id="p-4", username="custom",
            source="hashtag_search", scrape_status="scraping",
        )

        assert result["source"] == "hashtag_search"
        assert result["scrape_status"] == "scraping"

    def test_category_name_fallback(self) -> None:
        """Если business_category_name нет, берём category_name."""
        user = {"pk": 1, "follower_count": 1000, "category_name": "Fashion"}
        result = build_blog_data_from_user(user, person_id="p-5", username="cat_user")

        assert result["business_category"] == "Fashion"

    def test_invalid_bio_links_filtered(self) -> None:
        """Bio links без url отфильтровываются."""
        user = {
            "pk": 1, "follower_count": 1000,
            "bio_links": [
                {"title": "No URL"},
                {"url": "https://valid.com", "title": "OK"},
                "not_a_dict",
            ],
        }
        result = build_blog_data_from_user(user, person_id="p-6", username="links_user")

        assert len(result["bio_links"]) == 1
        assert result["bio_links"][0]["url"] == "https://valid.com"
