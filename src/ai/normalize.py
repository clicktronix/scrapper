"""Нормализация полей AI-анализа: country, posting_frequency, дедупликация."""
from difflib import get_close_matches
from typing import Literal

from loguru import logger

# ---------------------------------------------------------------------------
# Country нормализация
# ---------------------------------------------------------------------------

# Маппинг стран → русские названия (lowercase ключи)
# ISO-коды, английские/нативные названия, частые опечатки GPT
COUNTRY_NORMALIZE: dict[str, str] = {
    # Казахстан — все варианты
    "kazakhstan": "Казахстан",
    "kazahstan": "Казахстан",
    "kazakstan": "Казахстан",
    "kazakhtan": "Казахстан",
    "kazakistan": "Казахстан",
    "republic of kazakhstan": "Казахстан",
    "qazaqstan": "Казахстан",
    "қазақстан": "Казахстан",
    "kz": "Казахстан",
    # Россия
    "russia": "Россия",
    "russian federation": "Россия",
    "россия": "Россия",
    "рф": "Россия",
    "ru": "Россия",
    # Узбекистан
    "uzbekistan": "Узбекистан",
    "uzbekstan": "Узбекистан",
    "republic of uzbekistan": "Узбекистан",
    "ўзбекистон": "Узбекистан",
    "o'zbekiston": "Узбекистан",
    "uz": "Узбекистан",
    # Кыргызстан
    "kyrgyzstan": "Кыргызстан",
    "kirgizstan": "Кыргызстан",
    "kirgizia": "Кыргызстан",
    "kyrgyz republic": "Кыргызстан",
    "кыргызстан": "Кыргызстан",
    "киргизия": "Кыргызстан",
    "kg": "Кыргызстан",
    # Таджикистан
    "tajikistan": "Таджикистан",
    "tadzhikistan": "Таджикистан",
    "republic of tajikistan": "Таджикистан",
    "tj": "Таджикистан",
    # Туркменистан
    "turkmenistan": "Туркменистан",
    "tm": "Туркменистан",
    # Азербайджан
    "azerbaijan": "Азербайджан",
    "azerbaidjan": "Азербайджан",
    "az": "Азербайджан",
    # Грузия
    "georgia": "Грузия",
    "ge": "Грузия",
    # Армения
    "armenia": "Армения",
    "am": "Армения",
    # Турция
    "turkey": "Турция",
    "türkiye": "Турция",
    "turkiye": "Турция",
    "tr": "Турция",
    # Ближний Восток
    "uae": "ОАЭ",
    "united arab emirates": "ОАЭ",
    "объединённые арабские эмираты": "ОАЭ",
    "объединенные арабские эмираты": "ОАЭ",
    "эмираты": "ОАЭ",
    "оаэ": "ОАЭ",
    "ae": "ОАЭ",
    "qatar": "Катар",
    "qa": "Катар",
    "saudi arabia": "Саудовская Аравия",
    "sa": "Саудовская Аравия",
    "bahrain": "Бахрейн",
    "israel": "Израиль",
    "il": "Израиль",
    # Азия
    "china": "Китай",
    "cn": "Китай",
    "south korea": "Южная Корея",
    "korea": "Южная Корея",
    "kr": "Южная Корея",
    "japan": "Япония",
    "jp": "Япония",
    "india": "Индия",
    "in": "Индия",
    "malaysia": "Малайзия",
    "my": "Малайзия",
    "thailand": "Таиланд",
    "th": "Таиланд",
    "indonesia": "Индонезия",
    "id": "Индонезия",
    "singapore": "Сингапур",
    "sg": "Сингапур",
    "mongolia": "Монголия",
    "mn": "Монголия",
    "vietnam": "Вьетнам",
    "вьетнам": "Вьетнам",
    "iran": "Иран",
    "иран": "Иран",
    "iraq": "Ирак",
    "ирак": "Ирак",
    "pakistan": "Пакистан",
    "пакистан": "Пакистан",
    "nepal": "Непал",
    "непал": "Непал",
    "sri lanka": "Шри-Ланка",
    "шри-ланка": "Шри-Ланка",
    # Америка
    "usa": "США",
    "united states": "США",
    "united states of america": "США",
    "us": "США",
    "canada": "Канада",
    "ca": "Канада",
    "brazil": "Бразилия",
    "br": "Бразилия",
    "mexico": "Мексика",
    "mx": "Мексика",
    "argentina": "Аргентина",
    # Европа
    "uk": "Великобритания",
    "united kingdom": "Великобритания",
    "great britain": "Великобритания",
    "england": "Великобритания",
    "gb": "Великобритания",
    "germany": "Германия",
    "de": "Германия",
    "france": "Франция",
    "fr": "Франция",
    "italy": "Италия",
    "it": "Италия",
    "spain": "Испания",
    "es": "Испания",
    "australia": "Австралия",
    "au": "Австралия",
    "poland": "Польша",
    "pl": "Польша",
    "czech republic": "Чехия",
    "czechia": "Чехия",
    "cz": "Чехия",
    "netherlands": "Нидерланды",
    "nl": "Нидерланды",
    "belgium": "Бельгия",
    "austria": "Австрия",
    "at": "Австрия",
    "switzerland": "Швейцария",
    "ch": "Швейцария",
    "sweden": "Швеция",
    "se": "Швеция",
    "norway": "Норвегия",
    "finland": "Финляндия",
    "denmark": "Дания",
    "portugal": "Португалия",
    "greece": "Греция",
    "hungary": "Венгрия",
    "romania": "Румыния",
    "bulgaria": "Болгария",
    "serbia": "Сербия",
    "croatia": "Хорватия",
    # СНГ / Восточная Европа
    "ukraine": "Украина",
    "ua": "Украина",
    "belarus": "Беларусь",
    "by": "Беларусь",
    "moldova": "Молдова",
    "md": "Молдова",
    "latvia": "Латвия",
    "lithuania": "Литва",
    "estonia": "Эстония",
    # Африка
    "egypt": "Египет",
    "eg": "Египет",
    "morocco": "Марокко",
    "tunisia": "Тунис",
    "south africa": "ЮАР",
    # Дополнительные русские варианты
    "корея": "Южная Корея",
    "ливан": "Ливан",
    "португалия": "Португалия",
    "нидерланды": "Нидерланды",
    "бельгия": "Бельгия",
    "австрия": "Австрия",
    "норвегия": "Норвегия",
    "финляндия": "Финляндия",
    "дания": "Дания",
    "венгрия": "Венгрия",
    "румыния": "Румыния",
    "болгария": "Болгария",
    "сербия": "Сербия",
    "хорватия": "Хорватия",
    "греция": "Греция",
}

# Значения → null
_COUNTRY_NULL_VALUES: frozenset[str] = frozenset({
    "unknown", "не указано", "неизвестно", "n/a", "none", "-", "—",
    "международный", "не определено", "не установлено",
})

# Валидные русские названия
_VALID_RUSSIAN_COUNTRIES: frozenset[str] = frozenset(COUNTRY_NORMALIZE.values())

# Замена латинских символов-двойников на кириллицу
_LATIN_TO_CYRILLIC: dict[str, str] = {
    "a": "а", "c": "с", "e": "е", "o": "о", "p": "р",
    "x": "х", "y": "у", "k": "к", "h": "н",
}


def _fix_mixed_alphabet(text: str) -> str:
    """Заменить латинские символы-двойники на кириллицу в кириллической строке."""
    cyrillic_count = sum(1 for ch in text if "\u0400" <= ch <= "\u04ff")
    if cyrillic_count < len(text) * 0.5:
        return text
    return "".join(_LATIN_TO_CYRILLIC.get(ch, ch) for ch in text)


def normalize_country(country: str | None) -> str | None:
    """Нормализовать страну к русскому названию."""
    if not country:
        return None

    cleaned = country.strip().rstrip("?.!,;:")
    if not cleaned:
        return None

    # Убираем пояснения в скобках: "Kazakhstan (likely)" → "Kazakhstan"
    if "(" in cleaned:
        cleaned = cleaned.split("(")[0].strip()

    # Убираем суффикс после запятой: "Almaty, Kazakhstan" → первую часть не трогаем (это для city)
    if cleaned.lower() in _COUNTRY_NULL_VALUES:
        return None

    key = cleaned.lower()
    if key in COUNTRY_NORMALIZE:
        return COUNTRY_NORMALIZE[key]

    fixed = _fix_mixed_alphabet(cleaned)
    if fixed != cleaned:
        fixed_key = fixed.lower()
        if fixed_key in COUNTRY_NORMALIZE:
            return COUNTRY_NORMALIZE[fixed_key]
        if fixed in _VALID_RUSSIAN_COUNTRIES:
            return fixed

    if cleaned in _VALID_RUSSIAN_COUNTRIES:
        return cleaned

    matches = get_close_matches(key, COUNTRY_NORMALIZE.keys(), n=1, cutoff=0.75)
    if matches:
        result = COUNTRY_NORMALIZE[matches[0]]
        logger.debug(f"[normalize] country fuzzy: '{cleaned}' -> '{result}' (matched '{matches[0]}')")
        return result

    return cleaned


# ---------------------------------------------------------------------------
# City нормализация
# ---------------------------------------------------------------------------

# Казахстанские города: английские/нестандартные → русские
CITY_NORMALIZE: dict[str, str] = {
    # Алматы
    "almaty": "Алматы",
    "alma-ata": "Алматы",
    "alma ata": "Алматы",
    "алма-ата": "Алматы",
    "алмата": "Алматы",
    # Астана
    "astana": "Астана",
    "nur-sultan": "Астана",
    "nursultan": "Астана",
    "нур-султан": "Астана",
    # Шымкент
    "shymkent": "Шымкент",
    "chimkent": "Шымкент",
    "чимкент": "Шымкент",
    # Караганда
    "karaganda": "Караганда",
    "karagandy": "Караганда",
    "qaragandy": "Караганда",
    # Актобе
    "aktobe": "Актобе",
    "aqtobe": "Актобе",
    "aktobe city": "Актобе",
    "актюбинск": "Актобе",
    # Актау
    "aktau": "Актау",
    "aqtau": "Актау",
    # Атырау
    "atyrau": "Атырау",
    # Тараз
    "taraz": "Тараз",
    "жамбыл": "Тараз",
    # Павлодар
    "pavlodar": "Павлодар",
    # Усть-Каменогорск
    "ust-kamenogorsk": "Усть-Каменогорск",
    "oskemen": "Усть-Каменогорск",
    "öskemen": "Усть-Каменогорск",
    # Семей
    "semey": "Семей",
    "semipalatinsk": "Семей",
    "семипалатинск": "Семей",
    # Кызылорда
    "kyzylorda": "Кызылорда",
    "qyzylorda": "Кызылорда",
    # Костанай
    "kostanay": "Костанай",
    "kostanai": "Костанай",
    "qostanay": "Костанай",
    "кустанай": "Костанай",
    # Уральск
    "uralsk": "Уральск",
    "oral": "Уральск",
    # Петропавловск
    "petropavlovsk": "Петропавловск",
    # Кокшетау
    "kokshetau": "Кокшетау",
    "kökshetau": "Кокшетау",
    # Талдыкорган
    "taldykorgan": "Талдыкорган",
    # Туркестан
    "turkestan": "Туркестан",
    "turkistan": "Туркестан",
    # Экибастуз
    "ekibastuz": "Экибастуз",
    # Степногорск
    "stepnogorsk": "Степногорск",
    # Темиртау
    "temirtau": "Темиртау",
    # Жанаозен
    "zhanaozen": "Жанаозен",
    # Балхаш
    "balkhash": "Балхаш",
    # Байконур
    "baikonur": "Байконур",
    # Россия
    "moscow": "Москва",
    "saint petersburg": "Санкт-Петербург",
    "st petersburg": "Санкт-Петербург",
    "st. petersburg": "Санкт-Петербург",
    "novosibirsk": "Новосибирск",
    "yekaterinburg": "Екатеринбург",
    "kazan": "Казань",
    "krasnoyarsk": "Красноярск",
    "sochi": "Сочи",
    # Узбекистан
    "tashkent": "Ташкент",
    "samarkand": "Самарканд",
    "bukhara": "Бухара",
    # Кыргызстан
    "bishkek": "Бишкек",
    "osh": "Ош",
    # Другие
    "dubai": "Дубай",
    "istanbul": "Стамбул",
    "antalya": "Анталья",
    "bali": "Бали",
    "bangkok": "Бангкок",
    "london": "Лондон",
    "paris": "Париж",
    "new york": "Нью-Йорк",
    "los angeles": "Лос-Анджелес",
    "berlin": "Берлин",
    "rome": "Рим",
    "barcelona": "Барселона",
    "milan": "Милан",
    "tokyo": "Токио",
    "seoul": "Сеул",
    "beijing": "Пекин",
    "shanghai": "Шанхай",
}

_VALID_RUSSIAN_CITIES: frozenset[str] = frozenset(CITY_NORMALIZE.values())

# Null-значения для городов
_CITY_NULL_VALUES: frozenset[str] = frozenset({
    "unknown", "не указано", "неизвестно", "n/a", "none", "-", "—",
})


def normalize_city(city: str | None) -> str | None:
    """Нормализовать город к русскому названию."""
    if not city:
        return None

    # Убираем суффиксы вроде ", Kazakhstan", ", Kazakstan"
    cleaned = city.strip().rstrip("?.!,;:")
    if not cleaned:
        return None

    if cleaned.lower() in _CITY_NULL_VALUES:
        return None

    # Убираем ", Country" суффикс ("Almaty, Kazakhstan" → "Almaty")
    if ", " in cleaned:
        cleaned = cleaned.split(",")[0].strip()

    key = cleaned.lower()
    if key in CITY_NORMALIZE:
        return CITY_NORMALIZE[key]

    # Фикс смешанных алфавитов
    fixed = _fix_mixed_alphabet(cleaned)
    if fixed != cleaned:
        fixed_key = fixed.lower()
        if fixed_key in CITY_NORMALIZE:
            return CITY_NORMALIZE[fixed_key]
        if fixed in _VALID_RUSSIAN_CITIES:
            return fixed

    if cleaned in _VALID_RUSSIAN_CITIES:
        return cleaned

    # Fuzzy только для латиницы
    if cleaned.isascii():
        matches = get_close_matches(key, CITY_NORMALIZE.keys(), n=1, cutoff=0.8)
        if matches:
            result = CITY_NORMALIZE[matches[0]]
            logger.debug(f"[normalize] city fuzzy: '{cleaned}' -> '{result}' (matched '{matches[0]}')")
            return result

    return cleaned


# ---------------------------------------------------------------------------
# Posting frequency
# ---------------------------------------------------------------------------

PostingFrequency = Literal["rare", "weekly", "several_per_week", "daily"]


def normalize_posting_frequency(
    ai_freq: str | None,
    posts_per_week: float | None,
) -> str | None:
    """Переопределить posting_frequency по фактическому posts_per_week."""
    if posts_per_week is None:
        return ai_freq
    if posts_per_week < 0.5:
        return "rare"
    if posts_per_week < 1.5:
        return "weekly"
    if posts_per_week < 5:
        return "several_per_week"
    return "daily"


# ---------------------------------------------------------------------------
# Дедупликация
# ---------------------------------------------------------------------------

def deduplicate_list(items: list[str]) -> list[str]:
    """Дедупликация списка строк с сохранением порядка."""
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result
