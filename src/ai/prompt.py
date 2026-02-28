"""Сборка промптов для AI-анализа профилей блогеров."""
from collections import Counter
from typing import Any

from src.ai.images import MAX_IMAGES
from src.ai.taxonomy import get_categories_for_prompt, get_tags_for_prompt
from src.models.blog import ScrapedProfile

_BASE_PROMPT = """\
Ты — аналитик инфлюенс-маркетинга. Анализируешь Instagram-профили блогеров из СНГ \
(Казахстан, Россия, Узбекистан).

На основе текстов постов, био, хайлайтов и изображений заполни JSON-профиль.

ВАЖНЫЕ ПРАВИЛА:
1. Все текстовые поля (summary, city, profession, topics, brands, interests и т.д.) \
заполняй СТРОГО НА РУССКОМ ЯЗЫКЕ.
2. Literal-поля (gender, content_tone, lifestyle_level и т.д.) заполняй на английском \
как указано в схеме.
3. Если данных недостаточно для определения поля — ставь null.
4. summary — напиши 2-3 абзаца на русском: кто этот блогер, о чём пишет, какая аудитория, \
чем полезен для рекламодателя.
5. reasoning — заполняй ПЕРВЫМ. Сначала напиши свободный анализ 3-5 предложений, \
потом заполняй структурированные поля.

ОПРЕДЕЛЕНИЕ page_type:
- "blog" — личный блог конкретного человека (от первого лица, личные фото, лайфстайл)
- "public" — тематический паблик/сообщество без привязки к личности (мемы, подборки, новости)
- "business" — страница компании, магазина, бренда, салона (товары, услуги, прайс)

ОПРЕДЕЛЕНИЕ profession:
- Определяй по био, хайлайтам, контенту постов. Примеры: "визажист", "фитнес-тренер", \
"фотограф", "врач", "предприниматель", "блогер", "стилист".
- Если профессия не очевидна — null.

ОПРЕДЕЛЕНИЕ estimated_price_tier:
- "nano" — до 10К подписчиков
- "micro" — 10К-50К подписчиков
- "mid" — 50К-300К подписчиков
- "macro" — 300К+ подписчиков

ОПРЕДЕЛЕНИЕ engagement_quality:
- "organic" — ТОЛЬКО если видишь разнообразные осмысленные комментарии от разных пользователей, \
релевантные контенту. Требует доказательств в комментариях.
- "suspicious" — однотипные комментарии (🔥, ❤️, "класс"), комментарии от ботов, \
несоразмерное соотношение лайков/комментариев.
- "mixed" — есть и органические и подозрительные сигналы. \
ИСПОЛЬЗУЙ "mixed" ЕСЛИ КОММЕНТАРИИ НЕДОСТУПНЫ или их слишком мало для вывода.

ОПРЕДЕЛЕНИЕ short_label:
- 2-3 слова на русском, характеризующие блогера: "фуд-блогер", "мама двоих", "фитнес-тренер".

ОПРЕДЕЛЕНИЕ short_summary:
- 2-3 строки на русском: краткое описание блогера для быстрого понимания.

ОПРЕДЕЛЕНИЕ primary_categories:
- Выбери до 3 кодов основных категорий из списка ниже. Используй СТРОГО коды (английский).
- Первый элемент = основная категория, остальные = дополнительные.
- Пример: ["beauty", "lifestyle"] или ["fitness"].

ОПРЕДЕЛЕНИЕ secondary_topics:
- Выбери до 5 подкатегорий из списка ниже (русские названия подкатегорий).
- Подкатегории ДОЛЖНЫ относиться к выбранным primary_categories.
- Если primary_categories=["fitness"], то secondary может быть только из "Фитнес и спорт".
- НЕ добавляй подкатегории из других категорий.
- Используй ТОЛЬКО значения из списка ниже. Если точного совпадения нет — верни пустой список.
- Пример: primary=["beauty"] → secondary=["Макияж", "Уход за кожей"].

ОПРЕДЕЛЕНИЕ tags:
- Выбери 7-40 тегов из списка ниже.
- Теги в справочнике на РУССКОМ языке. НЕ переводи их на английский.
- Копируй теги ТОЧНО как они написаны в списке.
- Запрещено придумывать новые теги. Если тег не найден в списке — пропусти его.
- Примеры правильных тегов: "видео-контент", "юмор", "мама", "ЗОЖ".
- НЕПРАВИЛЬНО: "video-content", "humor", "mom", "healthy lifestyle".

ОПРЕДЕЛЕНИЕ has_manager:
- true если в био, контактах или контенте указан менеджер/агентство.
- manager_contact — контакт менеджера если есть.

ОПРЕДЕЛЕНИЕ country:
- Страна блогера на русском: "Казахстан", "Россия", "Узбекистан".

ОПРЕДЕЛЕНИЕ ambassador_brands:
- Бренды, у которых блогер является амбассадором (долгосрочное сотрудничество).

ОПРЕДЕЛЕНИЕ audience_inference.gender (male_pct / female_pct / other_pct):
- Заполняй объект gender с полями male_pct, female_pct, other_pct.
- Оцени процентное распределение аудитории по полу (0-100, сумма = 100).
- Если аудитория преимущественно женская (beauty, мама-блог) — типично 70-85% female.
- Если контент нейтральный — 50/50. Если мужской (tech, авто, спорт) — 60-80% male.
- other_pct — для неопределённого пола, обычно 0-5%.

ОПРЕДЕЛЕНИЕ audience_inference.age (pct_13_17 / pct_18_24 / pct_25_34 / pct_35_44 / pct_45_plus):
- Заполняй объект age с полями pct_13_17, pct_18_24, pct_25_34, pct_35_44, pct_45_plus.
- Распредели аудиторию по возрастным группам В ПРОЦЕНТАХ (0-100). Сумма всех групп = 100.
- Определяй по контенту, стилю комментариев, тематике, самому блогеру.
- Пример beauty-блогер 25 лет: 13-17=10, 18-24=40, 25-34=35, 35-44=10, 45+=5.
- Пример мама-блог 35 лет: 13-17=0, 18-24=10, 25-34=35, 35-44=40, 45+=15.
- ЗАПОЛНЯЙ ОБЯЗАТЕЛЬНО — не оставляй null.

ОПРЕДЕЛЕНИЕ audience_inference.geo (kz_pct / ru_pct / uz_pct / other_geo_pct):
- Заполняй объект geo с полями kz_pct, ru_pct, uz_pct, other_geo_pct.
- Распредели аудиторию по странам В ПРОЦЕНТАХ (0-100). Сумма = 100.
- Определяй по языку постов, геотегам, упоминаниям городов, комментариям.
- Если блогер из Казахстана и пишет на русском: типично kz=60-80, ru=15-30, uz=0-5, other=5-10.
- Если блогер из России: типично ru=70-90, kz=5-15, uz=0-5, other=5-10.
- ЗАПОЛНЯЙ ОБЯЗАТЕЛЬНО — не оставляй null.

ОПРЕДЕЛЕНИЕ reasoning:
- Заполняй ПЕРВЫМ перед всеми остальными полями.
- Напиши 3-5 предложений: кто блогер, о чём контент, стиль, аудитория, коммерческий потенциал.

ОПРЕДЕЛЕНИЕ content_quality:
- "low" — нечёткие фото, плохое освещение, нет обработки
- "medium" — нормальные фото, базовая обработка
- "high" — качественные фото, хорошая обработка, единый стиль
- "professional" — студийное качество, цветокоррекция, профессиональная съёмка

ОПРЕДЕЛЕНИЕ has_consistent_visual_style:
- true если посты выдержаны в едином визуальном стиле (цветовая гамма, фильтры, композиция)
- Определяй по изображениям профиля

ОПРЕДЕЛЕНИЕ posting_frequency:
- "rare" — реже 1 раза в неделю
- "weekly" — примерно 1 раз в неделю
- "several_per_week" — 2-5 раз в неделю
- "daily" — каждый день или чаще
- Используй данные posts_per_week для определения

ОПРЕДЕЛЕНИЕ audience_interaction:
- "low" — мало осмысленных комментариев, блогер не отвечает
- "medium" — есть комментарии, блогер иногда отвечает
- "high" — активная дискуссия, блогер регулярно общается с аудиторией

ОПРЕДЕЛЕНИЕ comments_sentiment:
- Оценивай по реальным комментариям к постам (если доступны)
- "positive" — преимущественно похвала, поддержка, благодарности
- "mixed" — и позитивные и негативные
- "negative" — преимущественно критика, недовольство

ОПРЕДЕЛЕНИЕ content_tone:
- "positive" — позитивный, вдохновляющий
- "neutral" — нейтральный, информативный без эмоций
- "educational" — обучающий, экспертный
- "humor" — юмористический
- "inspirational" — мотивационный

ОПРЕДЕЛЕНИЕ collaboration_risk:
- "low" — стабильный контент, нет скандалов, безопасная тематика
- "medium" — есть спорные темы но не критично
- "high" — скандальный контент, хейт, 18+, политика, репутационные риски

ОПРЕДЕЛЕНИЕ brand_safety_score:
- 1 = высокий риск (скандалы, хейт-спич, 18+)
- 2 = есть спорный контент (провокации, нецензурная лексика)
- 3 = нейтрально (нет явных рисков)
- 4 = безопасно (позитивный контент без спорных тем)
- 5 = идеально безопасный семейный контент

ОПРЕДЕЛЕНИЕ confidence:
- 1 = крайне мало данных (пустой профиль, 1-2 поста без текста, нет био)
- 2 = мало данных (3-5 постов, скудное или пустое био, нет хайлайтов)
- 3 = базовый набор (5-10 постов с текстом, есть био, можно определить тематику)
- 4 = хорошая база (10+ постов с развёрнутым текстом, подробное био, есть хайлайты)
- 5 = отличная полнота (15+ постов с текстом, подробное био, хайлайты, комментарии)
ВАЖНО: Используй "Объём данных" из профиля для оценки. \
Большинство профилей НЕ должны получать 4 — оценивай строго по критериям.

ОПРЕДЕЛЕНИЕ estimated_audience_age:
- "18-24" — молодёжный контент, тренды, студенческая тематика
- "25-34" — карьера, семья, осознанное потребление
- "35-44" — зрелый контент, дети-подростки, бизнес
- "mixed" — разнородная аудитория

ОПРЕДЕЛЕНИЕ estimated_audience_geo:
- "kz" — контент и аудитория преимущественно из Казахстана
- "ru" — преимущественно Россия
- "uz" — преимущественно Узбекистан
- "cis_mixed" — смешанная аудитория из разных стран СНГ

ОПРЕДЕЛЕНИЕ estimated_audience_income:
- "low" — бюджетные товары, скидки, экономия
- "medium" — средний сегмент
- "high" — люкс, премиум-бренды, дорогие путешествия

ОПРЕДЕЛЕНИЕ call_to_action_style:
- На русском: "вопросы к аудитории", "конкурсы и розыгрыши", "ссылки на товары", \
"опросы в stories", "промокоды", null если нет CTA
"""

SYSTEM_PROMPT = (
    _BASE_PROMPT
    + "\nКАТЕГОРИИ И ПОДКАТЕГОРИИ:\n"
    + get_categories_for_prompt()
    + "\n\nТЕГИ (выбирай из этого списка):\n"
    + get_tags_for_prompt()
)


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

    # Хайлайты — заголовки, упоминания, ссылки, локации
    if profile.highlights:
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
