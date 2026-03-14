"""Сборка промптов для AI-анализа профилей блогеров."""
from collections import Counter
from typing import Any

from src.ai.images import MAX_IMAGES
from src.ai.taxonomy import get_categories_for_prompt, get_tags_for_prompt
from src.models.blog import ScrapedProfile

_BASE_PROMPT = """\
Ты — аналитик инфлюенс-маркетинга для Instagram-профилей СНГ \
(Казахстан, Россия, Узбекистан). Заполни JSON-профиль строго по схеме.

ГЛАВНЫЕ ПРАВИЛА:
1) Все текстовые поля на русском языке. Literal-поля заполняй на английском, как в схеме.
2) Если данных недостаточно: scalar -> null, списки -> [].
3) Не выдумывай факты, бренды, теги, категории и локации.
4) reasoning заполняй ПЕРВЫМ: 3-5 предложений.
5) summary: 2-3 абзаца о блогере, аудитории и пользе для рекламодателя.

ОПРЕДЕЛЕНИЯ КЛЮЧЕВЫХ ПОЛЕЙ:
- short_label: 2-3 слова на русском. Типовые значения: "лайфстайл-блогер", "бьюти-блогер", \
"фуд-блогер", "мама-блогер", "фитнес-блогер", "тревел-блогер", "юмор-блогер", \
"новостной паблик", "бизнес-страница", "авто-блогер", "фэшн-блогер", "семейный блогер", \
"эксперт-блогер", "развлекательный паблик". Придумай аналогичный, если ни один не подходит.
- ambassador_brands: только долгосрочные амбассадорства, не разовая реклама.

ТАКСОНОМИЯ:
- primary_categories: до 3 кодов (английский), первый = основной.
- secondary_topics: до 5 подкатегорий (русские названия); ДОЛЖНЫ относиться к выбранным primary_categories.
- tags: 7-40 штук, СТРОГО из справочника ниже, на русском. НЕ переводи, НЕ выдумывай.

АУДИТОРИЯ (проценты, сумма каждой группы = 100, ЗАПОЛНЯЙ ОБЯЗАТЕЛЬНО):
- gender: male_pct + female_pct + other_pct = 100.
- age: pct_13_17 + pct_18_24 + pct_25_34 + pct_35_44 + pct_45_plus = 100.
  Пример beauty 25 лет: 10/40/35/10/5. Пример мама 35 лет: 0/10/35/40/15.
- geo: kz_pct + ru_pct + uz_pct + other_geo_pct = 100.
  КЗ-профиль типично: 60-80/15-30/0-5/5-10. РФ-профиль: 5-15/70-90/0-5/5-10.

ПОВЕДЕНЧЕСКИЕ ПРАВИЛА:
- engagement_quality: ставь "mixed" ЕСЛИ КОММЕНТАРИИ НЕДОСТУПНЫ или их мало для вывода.
- confidence: используй "Объём данных" из профиля. Большинство профилей НЕ должны получать 4 — \
оценивай строго: 4 только при 10+ постах с развёрнутым текстом, подробном био И хайлайтах.
"""

SYSTEM_PROMPT = (
    _BASE_PROMPT
    + "\nКАТЕГОРИИ И ПОДКАТЕГОРИИ:\n"
    + get_categories_for_prompt()
    + "\n\nТЕГИ (выбирай из этого списка):\n"
    + get_tags_for_prompt()
)


def _build_profile_section(profile: ScrapedProfile) -> list[str]:
    """Секция профиля: username, bio, метрики, бизнес-информация."""
    text_parts: list[str] = []

    text_parts.append(f"Username: @{profile.username}")
    text_parts.append(f"Bio: {profile.biography}")
    if profile.external_url:
        text_parts.append(f"External URL: {profile.external_url}")
    if profile.bio_links:
        bio_links_str = ", ".join(
            f"{bl.url}" + (f" ({bl.title})" if bl.title else "")
            for bl in profile.bio_links
        )
        text_parts.append(f"Bio links: {bio_links_str}")
    text_parts.append(f"Followers: {profile.follower_count}")
    text_parts.append(f"Following: {profile.following_count}")
    text_parts.append(f"Total posts: {profile.media_count}")
    text_parts.append(f"Is verified: {profile.is_verified}")
    text_parts.append(f"Is business: {profile.is_business}")
    if profile.business_category:
        text_parts.append(f"Business category: {profile.business_category}")
    if profile.account_type is not None:
        account_type_map = {1: "personal", 2: "business", 3: "creator"}
        text_parts.append(
            f"Account type: {account_type_map.get(profile.account_type, str(profile.account_type))}"
        )
    if profile.public_email:
        text_parts.append(f"Public email: {profile.public_email}")
    if profile.contact_phone_number:
        phone = profile.contact_phone_number
        if profile.public_phone_country_code:
            phone = f"+{profile.public_phone_country_code} {phone}"
        text_parts.append(f"Contact phone: {phone}")
    if profile.city_name:
        text_parts.append(f"City: {profile.city_name}")
    if profile.address_street:
        text_parts.append(f"Address: {profile.address_street}")

    # Вычисленные метрики
    if profile.avg_er is not None:
        text_parts.append(f"Avg ER: {profile.avg_er:.2f}%")
    if profile.avg_er_reels is not None:
        text_parts.append(f"Avg ER reels: {profile.avg_er_reels:.2f}%")
    if profile.er_trend:
        text_parts.append(f"ER trend: {profile.er_trend}")
    if profile.posts_per_week is not None:
        text_parts.append(f"Posts per week: {profile.posts_per_week:.1f}")

    # Data quality hint — объём доступных данных для AI
    posts_with_text = sum(1 for m in profile.medias if m.caption_text and len(m.caption_text) > 20)
    posts_with_comments = sum(1 for m in profile.medias if m.top_comments)
    hint_parts = [f"{len(profile.medias)} постов"]
    if posts_with_text:
        hint_parts.append(f"{posts_with_text} с текстом")
    if profile.biography:
        hint_parts.append("био заполнено")
    if profile.highlights:
        hint_parts.append(f"{len(profile.highlights)} хайлайтов")
    if posts_with_comments:
        hint_parts.append(f"{posts_with_comments} с комментариями")
    text_parts.append(f"Объём данных: {', '.join(hint_parts)}.")

    return text_parts


def _build_highlights_section(profile: ScrapedProfile) -> list[str]:
    """Секция хайлайтов: заголовки, упоминания, ссылки, локации."""
    if not profile.highlights:
        return []

    text_parts: list[str] = []

    titles = [h.title for h in profile.highlights]
    text_parts.append(f"\nHighlight titles: {titles}")
    all_hl_mentions: set[str] = set()
    all_hl_links: set[str] = set()
    all_hl_locations: set[str] = set()
    for h in profile.highlights:
        all_hl_mentions.update(h.story_mentions)
        all_hl_links.update(h.story_links)
        all_hl_locations.update(h.story_locations)
    if all_hl_mentions:
        text_parts.append(f"Highlight mentions: {sorted(all_hl_mentions)}")
    if all_hl_links:
        text_parts.append(f"Highlight links: {sorted(all_hl_links)}")
    if all_hl_locations:
        text_parts.append(f"Highlight locations: {sorted(all_hl_locations)}")
    all_hl_sponsors: set[str] = set()
    all_hl_hashtags: set[str] = set()
    any_paid_partnership = False
    for h in profile.highlights:
        all_hl_sponsors.update(h.story_sponsor_tags)
        all_hl_hashtags.update(h.story_hashtags)
        if h.has_paid_partnership:
            any_paid_partnership = True
    if all_hl_sponsors:
        text_parts.append(f"Highlight sponsors: {sorted(all_hl_sponsors)}")
    if all_hl_hashtags:
        text_parts.append(f"Highlight hashtags: {sorted(all_hl_hashtags)}")
    if any_paid_partnership:
        text_parts.append("Has paid partnerships in highlights: True")

    return text_parts


def _build_media_section(profile: ScrapedProfile) -> list[str]:
    """Секция медиа: посты, рилсы, хештеги, упоминания, комментарии."""
    text_parts: list[str] = []

    # Посты
    if profile.medias:
        text_parts.append("\n--- Posts ---")
        for i, post in enumerate(profile.medias, 1):
            date_str = post.taken_at.strftime("%Y-%m-%d")
            stats = f"likes={post.like_count}, comments={post.comment_count}"
            er = ""
            if profile.follower_count > 0:
                er_val = (post.like_count + post.comment_count) / profile.follower_count * 100
                er = f", ER={er_val:.1f}%"
            sponsor = ""
            if post.has_sponsor_tag:
                sponsor = f", SPONSORED by {post.sponsor_brands}"
            location = ""
            if post.location_name:
                location = f", location={post.location_name}"
                if post.location_city:
                    location += f" ({post.location_city})"
            tagged = ""
            if post.usertags:
                tagged = f", tagged={post.usertags}"
            disabled = ""
            if post.comments_disabled:
                disabled = ", comments_disabled=True"
            slides = ""
            if post.carousel_media_count:
                slides = f", slides={post.carousel_media_count}"
            plays = ""
            if post.play_count is not None:
                plays = f", plays={post.play_count}"
            duration = ""
            if post.video_duration is not None:
                duration = f", duration={post.video_duration}s"
            title = ""
            if post.title:
                title = f', title="{post.title}"'
            alt = ""
            if post.accessibility_caption:
                alt = f', alt="{post.accessibility_caption[:200]}"'
            meta = f"{stats}{er}{plays}{duration}{title}{alt}{sponsor}{location}{tagged}{disabled}{slides}"
            text_parts.append(
                f"Post {i} ({date_str}, {meta}): {post.caption_text[:500]}"
            )
            if post.top_comments:
                comments_str = "; ".join(
                    f"@{c.username}: {c.text[:100]}"
                    for c in post.top_comments[:10]
                )
                text_parts.append(f"  Comments: [{comments_str}]")

    # Топ хештеги
    all_hashtags: list[str] = []
    for post in profile.medias:
        all_hashtags.extend(post.hashtags)
    if all_hashtags:
        top_hashtags = [tag for tag, _ in Counter(all_hashtags).most_common(20)]
        text_parts.append(f"\nTop hashtags: {top_hashtags}")

    # Топ упоминания
    all_mentions: list[str] = []
    for post in profile.medias:
        all_mentions.extend(post.mentions)
    if all_mentions:
        top_mentions = [m for m, _ in Counter(all_mentions).most_common(10)]
        text_parts.append(f"Top mentions: {top_mentions}")

    # Бренды-спонсоры
    all_brands: set[str] = set()
    for post in profile.medias:
        all_brands.update(post.sponsor_brands)
    if all_brands:
        text_parts.append(f"Sponsor brands: {sorted(all_brands)}")

    # Локации из постов
    all_locations: set[str] = set()
    for post in profile.medias:
        if post.location_name:
            loc = post.location_name
            if post.location_city:
                loc += f" ({post.location_city})"
            all_locations.add(loc)
    if all_locations:
        text_parts.append(f"Post locations: {sorted(all_locations)}")

    return text_parts


def build_analysis_prompt(
    profile: ScrapedProfile,
    image_map: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """
    Собрать multimodal-запрос для OpenAI.
    Возвращает list[message] для chat completions.

    image_map — словарь {url: data_uri} для замены remote URL на base64.
    Если None — используются оригинальные remote URL (обратная совместимость).
    Если передан, но url нет в словаре — изображение пропускается.
    """
    # Текстовая часть
    text_parts: list[str] = []
    text_parts.extend(_build_profile_section(profile))
    text_parts.extend(_build_highlights_section(profile))
    text_parts.extend(_build_media_section(profile))

    # Собираем multimodal content
    content: list[dict[str, Any]] = [
        {"type": "text", "text": "\n".join(text_parts)}
    ]

    # Изображения — все low detail для экономии токенов
    max_images = MAX_IMAGES
    image_count = 0

    def _add_image(url: str) -> bool:
        """Добавить изображение (low detail) в content. Возвращает True при успехе."""
        nonlocal image_count
        if image_count >= max_images:
            return False
        if image_map is not None:
            # Режим base64: используем data URI из словаря
            resolved = image_map.get(url)
            if resolved is None:
                return False  # скачивание не удалось — пропускаем
            image_url = resolved
        else:
            # Обратная совместимость: remote URL
            image_url = url
        content.append({
            "type": "image_url",
            "image_url": {"url": image_url, "detail": "low"},
        })
        image_count += 1
        return True

    if profile.profile_pic_url:
        _add_image(profile.profile_pic_url)

    for post in profile.medias:
        if post.thumbnail_url:
            _add_image(post.thumbnail_url)

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": content},
    ]
