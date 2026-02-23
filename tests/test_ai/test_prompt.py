"""Тесты сборки промпта для AI-анализа."""
from datetime import UTC, datetime


class TestBuildAnalysisPrompt:
    """Тесты build_analysis_prompt."""

    def _make_profile(self):
        from src.models.blog import ScrapedHighlight, ScrapedPost, ScrapedProfile

        return ScrapedProfile(
            platform_id="12345",
            username="testblogger",
            biography="Мама двоих детей",
            profile_pic_url="https://example.com/avatar.jpg",
            follower_count=50000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    caption_text="Пост #мама @friend",
                    hashtags=["#мама"],
                    mentions=["@friend"],
                    like_count=1500,
                    comment_count=50,
                    thumbnail_url="https://example.com/post1.jpg",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    caption_text="Рилс #дети",
                    play_count=10000,
                    thumbnail_url="https://example.com/reel1.jpg",
                    taken_at=datetime(2026, 1, 20, tzinfo=UTC),
                ),
            ],
            highlights=[
                ScrapedHighlight(
                    platform_id="h1",
                    title="Дети",
                    media_count=10,
                    story_mentions=["@school"],
                ),
            ],
        )

    def test_returns_messages_list(self) -> None:
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()
        messages = build_analysis_prompt(profile)

        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"

    def test_system_prompt_contains_instructions(self) -> None:
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()
        messages = build_analysis_prompt(profile)

        system_text = messages[0]["content"]
        assert "инфлюенс-маркетинга" in system_text
        assert "page_type" in system_text
        assert "summary" in system_text

    def test_user_prompt_contains_username(self) -> None:
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()
        messages = build_analysis_prompt(profile)

        # user message — multimodal content list
        content = messages[1]["content"]
        text_parts = [p for p in content if p["type"] == "text"]
        assert any("testblogger" in p["text"] for p in text_parts)

    def test_includes_images(self) -> None:
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()
        messages = build_analysis_prompt(profile)

        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]
        # 1 avatar + 2 medias = 3
        assert len(image_parts) == 3
        # Все с detail: "low"
        assert all(p["image_url"]["detail"] == "low" for p in image_parts)

    def test_includes_highlights(self) -> None:
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()
        messages = build_analysis_prompt(profile)

        content = messages[1]["content"]
        text_parts = [p for p in content if p["type"] == "text"]
        full_text = " ".join(p["text"] for p in text_parts)
        assert "Дети" in full_text

    def test_empty_profile_no_crash(self) -> None:
        """Пустой профиль (без медиа/хайлайтов) не падает."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedProfile

        profile = ScrapedProfile(
            platform_id="empty",
            username="emptyuser",
        )
        messages = build_analysis_prompt(profile)

        assert len(messages) == 2
        content = messages[1]["content"]
        # Только текстовая часть, без изображений
        assert len(content) == 1
        assert content[0]["type"] == "text"
        assert "emptyuser" in content[0]["text"]

    def test_max_images_limit(self) -> None:
        """Не больше 10 изображений (avatar + medias)."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        # 1 avatar + 12 posts + 5 reels = 18 medias, но должно быть ≤ 10
        medias = [
            ScrapedPost(
                platform_id=f"p{i}",
                media_type=1,
                thumbnail_url=f"https://example.com/p{i}.jpg",
                taken_at=datetime(2026, 1, i + 1, tzinfo=UTC),
            )
            for i in range(12)
        ] + [
            ScrapedPost(
                platform_id=f"r{i}",
                media_type=2,
                product_type="clips",
                thumbnail_url=f"https://example.com/r{i}.jpg",
                taken_at=datetime(2026, 1, i + 1, tzinfo=UTC),
            )
            for i in range(5)
        ]

        profile = ScrapedProfile(
            platform_id="12345",
            username="manyimages",
            profile_pic_url="https://example.com/avatar.jpg",
            follower_count=10000,
            medias=medias,
        )
        messages = build_analysis_prompt(profile)

        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]
        assert len(image_parts) == 10

    def test_caption_truncated_to_500(self) -> None:
        """Длинный caption обрезается до 500 символов."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        long_caption = "А" * 1000
        profile = ScrapedProfile(
            platform_id="12345",
            username="longcap",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    caption_text=long_caption,
                    like_count=100,
                    comment_count=10,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        content = messages[1]["content"]
        text = content[0]["text"]
        # В тексте должно быть 500 "А", а не 1000
        assert "А" * 500 in text
        assert "А" * 501 not in text

    def test_sponsor_brands_included(self) -> None:
        """Спонсорские бренды попадают в текст промпта."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="sponsored",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    has_sponsor_tag=True,
                    sponsor_brands=["cocacola", "adidas"],
                    like_count=100,
                    comment_count=10,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        text = messages[1]["content"][0]["text"]
        assert "cocacola" in text
        assert "adidas" in text

    def test_highlight_mentions_and_links_aggregated(self) -> None:
        """Упоминания и ссылки из хайлайтов агрегируются."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedHighlight, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="hltest",
            highlights=[
                ScrapedHighlight(
                    platform_id="h1",
                    title="Отзывы",
                    story_mentions=["@brand1"],
                    story_links=["https://link1.com"],
                ),
                ScrapedHighlight(
                    platform_id="h2",
                    title="Реклама",
                    story_mentions=["@brand2", "@brand1"],
                    story_links=["https://link2.com"],
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        text = messages[1]["content"][0]["text"]
        # Дедупликация: @brand1 появляется в обоих хайлайтах
        assert "@brand1" in text
        assert "@brand2" in text
        assert "https://link1.com" in text
        assert "https://link2.com" in text

    def test_er_calculation_in_text(self) -> None:
        """ER% рассчитывается и показывается для постов."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="ertest",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    like_count=500,
                    comment_count=100,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        text = messages[1]["content"][0]["text"]
        # ER = (500+100)/10000*100 = 6.0%
        assert "ER=6.0%" in text

    def test_zero_followers_no_er_in_text(self) -> None:
        """При 0 подписчиков ER не показывается (деление на 0)."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="zerof",
            follower_count=0,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    like_count=100,
                    comment_count=10,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        text = messages[1]["content"][0]["text"]
        assert "ER=" not in text

    def test_no_avatar_less_images(self) -> None:
        """Без аватара — на 1 изображение меньше."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="noavatar",
            profile_pic_url=None,
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url="https://example.com/post1.jpg",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]
        assert len(image_parts) == 1

    def test_posts_without_thumbnail_skipped(self) -> None:
        """Посты без thumbnail_url не добавляют изображение."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="nothumb",
            profile_pic_url=None,
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url=None,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]
        assert len(image_parts) == 0

    def test_top_hashtags_aggregated(self) -> None:
        """Топ хештеги из всех медиа агрегируются."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="hashtest",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    hashtags=["#мама", "#дети", "#мама"],
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    hashtags=["#мама", "#рилс"],
                    taken_at=datetime(2026, 1, 16, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        text = messages[1]["content"][0]["text"]
        assert "#мама" in text
        assert "#дети" in text
        assert "#рилс" in text

    def test_reel_play_count_in_text(self) -> None:
        """play_count рилса показывается в тексте."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="reeltest",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    play_count=50000,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        text = messages[1]["content"][0]["text"]
        assert "plays=50000" in text

    def test_reel_no_play_count(self) -> None:
        """Рилс без play_count — plays не показывается."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="noplay",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    play_count=None,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)

        text = messages[1]["content"][0]["text"]
        assert "plays=" not in text


class TestBuildAnalysisPromptNewFields:
    """Тесты новых полей в промпте."""

    def test_account_type_in_prompt(self) -> None:
        """account_type отображается в промпте."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="actype",
            account_type=2,
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "Account type: business" in text

    def test_public_email_in_prompt(self) -> None:
        """public_email отображается в промпте."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="emailtest",
            public_email="test@example.com",
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "Public email: test@example.com" in text

    def test_contact_phone_with_country_code(self) -> None:
        """Телефон с кодом страны."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="phonetest",
            contact_phone_number="7001234567",
            public_phone_country_code="7",
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "Contact phone: +7 7001234567" in text

    def test_city_and_address_in_prompt(self) -> None:
        """Город и адрес отображаются в промпте."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="citytest",
            city_name="Алматы",
            address_street="Абая 1",
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "City: Алматы" in text
        assert "Address: Абая 1" in text

    def test_bio_links_new_format_with_title(self) -> None:
        """bio_links в новом формате с title."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="bltest",
            bio_links=[
                {"url": "https://t.me/ch", "title": "Telegram", "link_type": None},
                {"url": "https://wa.me/77", "title": None, "link_type": None},
            ],
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "https://t.me/ch (Telegram)" in text
        assert "https://wa.me/77" in text

    def test_usertags_in_post(self) -> None:
        """usertags отображаются в посте."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="tagtest",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    like_count=100,
                    comment_count=10,
                    usertags=["@user1", "@user2"],
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "tagged=" in text
        assert "@user1" in text

    def test_comments_disabled_in_post(self) -> None:
        """comments_disabled=True отображается в посте."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="distest",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    like_count=100,
                    comment_count=0,
                    comments_disabled=True,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "comments_disabled=True" in text

    def test_carousel_slides_in_post(self) -> None:
        """carousel_media_count отображается как slides=N."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="cartest",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=8,
                    like_count=100,
                    comment_count=10,
                    carousel_media_count=5,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "slides=5" in text

    def test_video_duration_in_reel(self) -> None:
        """video_duration отображается в рилсе."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="durtest",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    video_duration=30.5,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "duration=30.5s" in text

    def test_reel_title_in_text(self) -> None:
        """title рилса отображается в тексте."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="titletest",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    title="My Reel",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert 'title="My Reel"' in text

    def test_highlight_sponsors_and_hashtags(self) -> None:
        """Спонсоры и хештеги хайлайтов агрегируются."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedHighlight, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="hlnew",
            highlights=[
                ScrapedHighlight(
                    platform_id="h1",
                    title="Ads",
                    story_sponsor_tags=["brand_x"],
                    has_paid_partnership=True,
                    story_hashtags=["beauty", "fashion"],
                ),
                ScrapedHighlight(
                    platform_id="h2",
                    title="More",
                    story_sponsor_tags=["brand_y", "brand_x"],
                    story_hashtags=["fashion", "style"],
                ),
            ],
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "Highlight sponsors:" in text
        assert "brand_x" in text
        assert "brand_y" in text
        assert "Highlight hashtags:" in text
        assert "beauty" in text
        assert "fashion" in text
        assert "style" in text
        assert "Has paid partnerships in highlights: True" in text


class TestBuildAnalysisPromptImageMap:
    """Тесты image_map параметра в build_analysis_prompt."""

    def _make_profile(self):
        from src.models.blog import ScrapedPost, ScrapedProfile

        return ScrapedProfile(
            platform_id="12345",
            username="imgmap",
            profile_pic_url="https://example.com/avatar.jpg",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="p1",
                    media_type=1,
                    thumbnail_url="https://example.com/post1.jpg",
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    thumbnail_url="https://example.com/reel1.jpg",
                    taken_at=datetime(2026, 1, 20, tzinfo=UTC),
                ),
            ],
        )

    def test_image_map_replaces_urls(self) -> None:
        """image_map заменяет remote URL на data URI."""
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()
        image_map = {
            "https://example.com/avatar.jpg": "data:image/jpeg;base64,avatar_data",
            "https://example.com/post1.jpg": "data:image/jpeg;base64,post1_data",
            "https://example.com/reel1.jpg": "data:image/jpeg;base64,reel1_data",
        }

        messages = build_analysis_prompt(profile, image_map=image_map)
        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]

        assert len(image_parts) == 3
        urls = [p["image_url"]["url"] for p in image_parts]
        assert urls[0] == "data:image/jpeg;base64,avatar_data"
        assert urls[1] == "data:image/jpeg;base64,post1_data"
        assert urls[2] == "data:image/jpeg;base64,reel1_data"
        # detail остаётся "low"
        assert all(p["image_url"]["detail"] == "low" for p in image_parts)

    def test_image_map_missing_url_skipped(self) -> None:
        """URL не в image_map → изображение пропускается."""
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()
        image_map = {
            "https://example.com/avatar.jpg": "data:image/jpeg;base64,avatar_data",
            # post1 и reel1 отсутствуют — не удалось скачать
        }

        messages = build_analysis_prompt(profile, image_map=image_map)
        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]

        assert len(image_parts) == 1
        assert image_parts[0]["image_url"]["url"] == "data:image/jpeg;base64,avatar_data"

    def test_image_map_empty_dict(self) -> None:
        """Пустой image_map → все изображения пропускаются."""
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()
        image_map: dict[str, str] = {}

        messages = build_analysis_prompt(profile, image_map=image_map)
        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]

        assert len(image_parts) == 0

    def test_image_map_none_backward_compatible(self) -> None:
        """image_map=None → поведение как раньше (remote URL)."""
        from src.ai.prompt import build_analysis_prompt

        profile = self._make_profile()

        messages = build_analysis_prompt(profile, image_map=None)
        content = messages[1]["content"]
        image_parts = [p for p in content if p["type"] == "image_url"]

        assert len(image_parts) == 3
        urls = [p["image_url"]["url"] for p in image_parts]
        assert urls[0] == "https://example.com/avatar.jpg"
        assert urls[1] == "https://example.com/post1.jpg"
        assert urls[2] == "https://example.com/reel1.jpg"


class TestPromptIncludesTaxonomy:
    """Тесты: промпт содержит справочник категорий и тегов."""

    def test_system_prompt_contains_categories(self) -> None:
        """System prompt содержит список категорий."""
        from src.ai.prompt import SYSTEM_PROMPT
        assert "beauty" in SYSTEM_PROMPT
        assert "Красота" in SYSTEM_PROMPT
        assert "entertainment" in SYSTEM_PROMPT
        assert "Развлечения" in SYSTEM_PROMPT

    def test_system_prompt_contains_tags(self) -> None:
        """System prompt содержит список тегов."""
        from src.ai.prompt import SYSTEM_PROMPT
        assert "видео-контент" in SYSTEM_PROMPT
        assert "reels" in SYSTEM_PROMPT
        assert "brand safe" in SYSTEM_PROMPT

    def test_system_prompt_has_new_field_instructions(self) -> None:
        """System prompt содержит инструкции для новых полей."""
        from src.ai.prompt import SYSTEM_PROMPT
        assert "short_label" in SYSTEM_PROMPT
        assert "short_summary" in SYSTEM_PROMPT
        assert "tags" in SYSTEM_PROMPT
        assert "has_manager" in SYSTEM_PROMPT
        assert "country" in SYSTEM_PROMPT
        assert "ambassador_brands" in SYSTEM_PROMPT

    def test_system_prompt_instructs_code_selection(self) -> None:
        """Primary topic должен быть кодом категории, не свободным текстом."""
        from src.ai.prompt import SYSTEM_PROMPT
        # Промпт должен инструктировать AI выбирать код
        lower = SYSTEM_PROMPT.lower()
        assert "код" in lower or "code" in lower or "из списка" in lower


class TestBuildAnalysisPromptPlayCountZero:
    """BUG-13: play_count=0 — валидное значение, должно отображаться."""

    def test_reel_play_count_zero_shown(self) -> None:
        """play_count=0 не должен скрываться (0 is falsy, но это реальное значение)."""
        from src.ai.prompt import build_analysis_prompt
        from src.models.blog import ScrapedPost, ScrapedProfile

        profile = ScrapedProfile(
            platform_id="12345",
            username="zeroplay",
            follower_count=10000,
            medias=[
                ScrapedPost(
                    platform_id="r1",
                    media_type=2,
                    product_type="clips",
                    play_count=0,
                    like_count=5,
                    comment_count=1,
                    taken_at=datetime(2026, 1, 15, tzinfo=UTC),
                ),
            ],
        )
        messages = build_analysis_prompt(profile)
        text = messages[1]["content"][0]["text"]
        assert "plays=0" in text
