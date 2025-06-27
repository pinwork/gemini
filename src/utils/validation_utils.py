#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import phonenumbers
import logging
from urllib.parse import urlparse, urlunparse

# Константи
URL_FIELDS = ["blog_url", "recruits_affiliates_url", "contact_page_url", "api_documentation_url"]

# Налаштування логера для validation_utils (використовуємо segmentation_validation)
logger = logging.getLogger("segmentation_validation")

# Популярні назви мов для конвертації
LANGUAGE_NAME_TO_CODE = {
    # Англійські назви
    "english": "en", "german": "de", "japanese": "ja", "french": "fr", "spanish": "es",
    "indonesian": "id", "russian": "ru", "portuguese": "pt", "dutch": "nl", "italian": "it",
    "chinese": "zh", "korean": "ko", "vietnamese": "vi", "polish": "pl", "turkish": "tr",
    "ukrainian": "uk", "thai": "th", "arabic": "ar", "swedish": "sv", "czech": "cs",
    "hungarian": "hu", "finnish": "fi", "danish": "da", "norwegian": "no", "greek": "el",
    "hebrew": "he", "hindi": "hi",
    
    # Альтернативні назви
    "deutsch": "de", "français": "fr", "francais": "fr", "español": "es", "espanol": "es",
    "português": "pt", "portugues": "pt", "italiano": "it", "русский": "ru", "russkiy": "ru",
    "nederlands": "nl", "svenska": "sv", "norsk": "no", "suomi": "fi", "magyar": "hu",
    "čeština": "cs", "cestina": "cs", "polski": "pl", "türkçe": "tr", "turkce": "tr",
    
    # Скорочені варіанти  
    "eng": "en", "ger": "de", "jap": "ja", "jpn": "ja", "fre": "fr", "spa": "es",
    "por": "pt", "ita": "it", "rus": "ru", "chi": "zh", "kor": "ko", "vie": "vi",
    "pol": "pl", "tur": "tr", "ukr": "uk", "ara": "ar", "swe": "sv", "cze": "cs",
    "hun": "hu", "fin": "fi", "dan": "da", "nor": "no"
}


def clean_phone_for_validation(phone: str) -> str:
    """
    Мінімальна очистка номера для підвищення шансів валідації
    
    Args:
        phone: Номер телефону для очистки
        
    Returns:
        Очищений номер телефону
    """
    if not phone:
        return ""
    
    # Видаляємо пробіли, дужки, тире
    cleaned = re.sub(r'[\s\(\)\-\.]', '', phone)
    
    # Якщо немає +, але є цифри, додаємо +
    if cleaned and not cleaned.startswith('+') and cleaned[0].isdigit():
        cleaned = '+' + cleaned
    
    return cleaned


def format_summary(summary_text: str) -> str:
    """
    Форматує summary text - робить першу літеру великою, додає крапку в кінці
    
    Args:
        summary_text: Текст summary для форматування
        
    Returns:
        Відформатований summary text
    """
    if not summary_text:
        return summary_text
    
    text = summary_text.strip()
    if not text:
        return text
    
    # Робимо першу літеру великою
    text = text[0].upper() + text[1:] if len(text) > 1 else text.upper()
    
    # Робимо великими літери після крапок
    text = re.sub(r'\. ([a-z])', lambda m: '. ' + m.group(1).upper(), text)
    
    # Додаємо крапку в кінці якщо немає
    if not text.endswith('.'):
        text += '.'
    
    # Прибираємо подвійні крапки
    text = re.sub(r'\.\.+', '.', text)
    
    return text


def clean_it_prefix(text_value: str) -> str:
    """
    Прибирає префікс "it " з початку тексту
    
    Args:
        text_value: Текст для обробки
        
    Returns:
        Текст без "it " префікса
    """
    if not text_value:
        return text_value
    
    if text_value.lower().startswith("it "):
        return text_value[3:].strip()
    
    return text_value


def clean_app_platforms(app_platforms_value) -> str:
    """
    Конвертує app_platforms з array в відсортований string через кому
    
    Args:
        app_platforms_value: Array або string платформ від Gemini API
        
    Returns:
        Відсортований string через кому або порожній рядок
    """
    if not app_platforms_value:
        return ""
    
    if isinstance(app_platforms_value, list):
        # Фільтруємо порожні значення та дублікати
        valid_platforms = [platform.strip().lower() for platform in app_platforms_value if platform and platform.strip()]
        unique_platforms = list(dict.fromkeys(valid_platforms))  # Видаляємо дублікати зберігаючи порядок
        
        # Сортуємо по алфавіту
        sorted_platforms = sorted(unique_platforms)
        
        return ", ".join(sorted_platforms)
    
    elif isinstance(app_platforms_value, str):
        # Якщо прийшов string - обробляємо як раніше
        platforms = [p.strip().lower() for p in app_platforms_value.split(",") if p.strip()]
        unique_platforms = list(dict.fromkeys(platforms))
        sorted_platforms = sorted(unique_platforms)
        return ", ".join(sorted_platforms)
    
    return ""


def has_access_issues(field_value: str, field_name: str = "") -> bool:
    """
    Перевіряє чи містить поле проблеми доступу або некоректні значення
    
    Args:
        field_value: Значення поля для перевірки
        field_name: Назва поля (для спеціальної обробки)
        
    Returns:
        True якщо поле має проблеми доступу
    """
    if not field_value:
        return False
        
    field_lower = field_value.strip().lower()
    
    # Спеціальна обробка для enum полів
    enum_fields_with_unspecified = ["target_age_group", "target_gender", "domain_formation_pattern"]
    if field_name in enum_fields_with_unspecified and field_lower == "unspecified":
        return False
    
    # Спеціальна обробка для segments_language
    if field_name == "segments_language":
        special_values = {"mixed", "unknown"}
        if field_lower in special_values or (len(field_value.strip()) == 2 and field_value.strip().isalpha()):
            return False
    
    # Список проблемних значень
    access_issues = [
        "unclear" in field_lower,
        field_lower == "unspecified",
        "unavailable" in field_lower,
        "not available" in field_lower,
        "not accessible" in field_lower,
        "inaccessible" in field_lower,
        "not determinable" in field_lower,
        field_lower == "cannot be determined",
        field_lower == "not detected",
        "unable" in field_lower,
        "cannot access" in field_lower,
        "can't access" in field_lower,
        "access denied" in field_lower,
        "access failed" in field_lower,
        "access error" in field_lower,
        "no access" in field_lower,
        field_lower == "error",
        field_lower == "failed",
        field_lower == "blocked",
        field_lower == "forbidden",
        field_lower == "restricted",
        field_lower == "timeout",
        field_lower == "unreachable",
        "site blocked" in field_lower,
        "website error" in field_lower,
        "site error" in field_lower,
        "website failed" in field_lower,
        "site failed" in field_lower,
        "website timeout" in field_lower,
        "site timeout" in field_lower,
        "website unreachable" in field_lower,
        "site unreachable" in field_lower,
        "access_unable" in field_lower,
        field_lower == "this platform",
        field_lower == "string",
        field_lower == "n/a",
        field_lower == "none",
        field_lower == "null",
    ]
    
    # "unknown" є проблемою доступу для всіх полів, крім segments_language
    if field_lower == "unknown" and field_name != "segments_language":
        access_issues.append(True)
    
    return any(access_issues)


def validate_country_code(country_code: str) -> bool:
    """
    Валідує 2-літерний ISO код країни
    
    Args:
        country_code: Код країни для валідації
        
    Returns:
        True якщо код валідний
    """
    if not country_code or len(country_code.strip()) != 2:
        return False
    return country_code.strip().isalpha()


def validate_and_clean_language_code(language_value: str) -> str:
    """
    Розумна валідація та очистка коду мови
    
    Args:
        language_value: Значення мови від Gemini API
        
    Returns:
        Валідний ISO 639-1 код або порожній рядок
    """
    if not language_value:
        return ""
    
    # Очищаємо та нормалізуємо
    cleaned = language_value.strip().lower()
    if not cleaned:
        return ""
    
    # 1. ✅ ЯКЩО ВЖЕ 2 БУКВИ - ПРОПУСКАЄМО БЕЗ ВАЛІДАЦІЇ
    if len(cleaned) == 2 and cleaned.isalpha():
        return cleaned  # xy, zz, qq - все пропускаємо!
    
    # 2. Обробка locale кодів з дефісом (zh-tw, en-us, fr-ca)
    if "-" in cleaned and len(cleaned) <= 6:
        language_part = cleaned.split("-")[0]
        if len(language_part) == 2 and language_part.isalpha():
            return language_part  # Будь-який 2-буквенний код
    
    # 3. Обробка underscore кодів (en_US, zh_CN)
    if "_" in cleaned and len(cleaned) <= 6:
        language_part = cleaned.split("_")[0]
        if len(language_part) == 2 and language_part.isalpha():
            return language_part  # Будь-який 2-буквенний код
    
    # 4. Пошук у словнику популярних назв
    if cleaned in LANGUAGE_NAME_TO_CODE:
        return LANGUAGE_NAME_TO_CODE[cleaned]
    
    # 5. Часткове співпадіння для популярних мов (english -> en)
    for lang_name, lang_code in LANGUAGE_NAME_TO_CODE.items():
        if lang_name in cleaned or cleaned in lang_name:
            # Додаткова перевірка щоб уникнути false positives
            if len(lang_name) >= 4 and len(cleaned) >= 4:
                return lang_code
    
    # 6. Якщо нічого не підійшло - порожній рядок
    return ""


def validate_email(email: str) -> bool:
    """
    Базова валідація email адреси
    
    Args:
        email: Email для валідації
        
    Returns:
        True якщо email валідний
    """
    if not email or "@" not in email:
        return False
    email = email.strip().lower()
    if email.count("@") != 1:
        return False
    local, domain = email.split("@")
    if not local or not domain or "." not in domain:
        return False
    return True


def validate_phone_e164(phone: str) -> bool:
    """
    Валідує номер телефону в E164 форматі
    
    Args:
        phone: Номер телефону для валідації
        
    Returns:
        True якщо номер валідний
    """
    if not phone or not phone.startswith("+"):
        return False
    digits = phone[1:]
    if not digits.isdigit() or len(digits) < 7 or len(digits) > 15:
        return False
    return True


def validate_segments_language(segments_language: str) -> bool:
    """
    Валідує код мови сегментів домену.
    Повертає True якщо це валідний ISO 639-1 код або дозволене спеціальне значення.
    
    Args:
        segments_language: Код мови для валідації
        
    Returns:
        True якщо код валідний
    """
    if not segments_language:
        return False
    
    language_code = segments_language.strip().lower()
    
    # Дозволені спеціальні значення
    special_values = {"mixed", "unknown"}
    if language_code in special_values:
        return True
    
    # Перевірка двобуквенного ISO 639-1 коду
    if len(language_code) == 2 and language_code.isalpha():
        return True
    
    logger.warning(f"Invalid segments_language: '{segments_language}' - must be 2-letter ISO code or 'mixed'/'unknown'")
    return False


def normalize_url(url_value: str) -> str:
    """
    Нормалізує URL до канонічного формату
    
    Args:
        url_value: URL для нормалізації
        
    Returns:
        Нормалізований URL або порожній рядок якщо невалідний
    """
    if not url_value:
        return url_value
    
    url_stripped = url_value.strip()
    
    if not (url_stripped.lower().startswith('http://') or url_stripped.lower().startswith('https://')):
        return ""
    
    try:
        parsed = urlparse(url_stripped)
        
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            ''
        ))
        
        if not normalized.endswith('/'):
            normalized += '/'
            
        return normalized
        
    except Exception:
        return ""


def validate_url_field(url_value: str, target_uri: str) -> str:
    """
    Валідує URL поле та порівнює з target_uri
    
    Args:
        url_value: URL для валідації
        target_uri: Базовий URI для порівняння
        
    Returns:
        Валідний URL або порожній рядок
    """
    if not url_value:
        return url_value
    
    normalized_url = normalize_url(url_value)
    
    if not normalized_url:
        return ""
    
    normalized_target = normalize_url(target_uri)
    
    # Якщо URL ідентичний target_uri, повертаємо порожній рядок
    if normalized_url.lower() == normalized_target.lower():
        return ""
    
    return normalized_url


def _segments_norm(s: str) -> str:
    """
    Нормалізуємо сегменти: прибираємо пробіли та регістр
    
    Args:
        s: Рядок для нормалізації
        
    Returns:
        Нормалізований рядок
    """
    return s.replace(' ', '').lower() if s else ''


def validate_segments_full(segment_combined: str, segments_full: str, domain_full: str = "") -> bool:
    """
    Перевіряє, чи коректно ШІ сегментував домен з мінімальним логуванням
    
    Args:
        segment_combined: Оригінальна сегментація (з пробілами)
        segments_full: AI повна сегментація (з пробілами)
        domain_full: Домен для логування (опціонально)
        
    Returns:
        True якщо валідація пройшла
    """
    # Перевірка на порожні значення
    if not segment_combined:
        return False
    
    if not segments_full:
        if domain_full:
            # 🎯 КОРОТКЕ логування в правильний файл
            logger.warning(f"Domain {domain_full}: segments_full validation failed | AI returned: <empty>")
        return False

    # Нормалізуємо: прибираємо пробіли та регістр
    original_normalized = _segments_norm(segment_combined)
    ai_normalized = _segments_norm(segments_full)

    # Склейка має збігатися
    validation_passed = original_normalized == ai_normalized
    
    if not validation_passed and domain_full:
        # 🎯 МІНІМАЛЬНЕ логування - тільки домен і що повернув AI
        logger.warning(f"Domain {domain_full}: segments_full validation failed | AI returned: '{segments_full}'")
    
    return validation_passed


def clean_segments_language(language_value: str) -> str:
    """
    Очищає segments_language - вибирає ПЕРШЕ валідне значення
    
    Args:
        language_value: Значення мови (може бути "en en" або "en fr")
        
    Returns:
        Одне валідне значення або порожній рядок
    """
    if not language_value:
        return ""
    
    # Розділяємо на частини
    parts = language_value.strip().split()
    if not parts:
        return ""
    
    # Пріоритети для вибору
    special_values = {"mixed", "unknown"}
    
    # Спочатку шукаємо спеціальні значення
    for part in parts:
        part_lower = part.lower()
        if part_lower in special_values:
            return part_lower
    
    # Потім шукаємо валідні ISO коди (2 літери)
    for part in parts:
        part_clean = part.strip().lower()
        if len(part_clean) == 2 and part_clean.isalpha():
            return part_clean
    
    # Якщо нічого не знайдено, повертаємо перший елемент
    return parts[0].lower()


def clean_segmentation_field(field_value: str, field_name: str) -> str:
    """
    Очищає поле сегментації від проблемних значень
    
    Args:
        field_value: Значення поля
        field_name: Назва поля
        
    Returns:
        Очищене значення або порожній рядок
    """
    if not field_value or has_access_issues(field_value, field_name):
        return ""
    return field_value.strip()


def clean_all_segmentation_fields(segment_combined: str, gemini_result: dict) -> dict:
    """
    Очищає ВСІ поля сегментації від сегментів що не входять в domain_core
    
    Args:
        segment_combined: Оригінальна сегментація domain_core
        gemini_result: Результати від Gemini
        
    Returns:
        Очищений словник з валідними сегментами
    """
    if not segment_combined:
        return gemini_result
    
    # Генеруємо domain_core segments (джерело правди)
    valid_segments = set(segment_combined.split())
    
    # Очищаємо ВСІ сегментаційні поля однаково
    segmentation_fields = [
        "segments_full", "segments_primary", "segments_descriptive", 
        "segments_prefix", "segments_suffix", "segments_thematic", "segments_common"
    ]
    
    for field_name in segmentation_fields:
        if field_name in gemini_result:
            field_value = gemini_result[field_name]
            if field_value:
                # Залишаємо тільки сегменти що входять в domain_core
                cleaned_segments = [seg for seg in field_value.split() 
                                  if seg in valid_segments]
                gemini_result[field_name] = " ".join(cleaned_segments)
    
    return gemini_result


def clean_geo_fields(gemini_result: dict) -> dict:
    """
    Очищає географічні поля з валідацією country code
    Якщо geo_country невалідний - очищає всі geo поля
    
    Args:
        gemini_result: Результати від Gemini
        
    Returns:
        Очищений словник з валідними geo полями
    """
    geo_country = gemini_result.get("geo_country", "").strip()
    
    # Валідуємо geo_country
    if geo_country and validate_country_code(geo_country):
        # Country валідний - залишаємо всі geo поля
        gemini_result["geo_country"] = geo_country.upper()  # ISO коди зазвичай uppercase
        # geo_region і geo_city залишаються як є
    else:
        # Country невалідний - очищаємо ВСІ geo поля
        gemini_result["geo_country"] = ""
        gemini_result["geo_region"] = ""
        gemini_result["geo_city"] = ""
    
    return gemini_result


def handle_segments_full_validation(gemini_result: dict, domain_full: str = "") -> dict:
    """
    Спеціальна обробка для поля segments_full
    Якщо після очистки воно стає порожнім - записує "validation_failed"
    
    Args:
        gemini_result: Результати після очистки
        domain_full: Домен для логування (опціонально)
        
    Returns:
        Модифікований словник з обробленим segments_full
    """
    segments_full = gemini_result.get("segments_full", "").strip()
    
    # Якщо segments_full порожнє після очистки - записуємо validation_failed
    if not segments_full:
        gemini_result["segments_full"] = "validation_failed"
        if domain_full:
            logger.info(f"Domain {domain_full}: segments_full set to 'validation_failed' due to empty value after cleaning")
        else:
            logger.info(f"segments_full set to 'validation_failed' due to empty value after cleaning")
    
    return gemini_result


def clean_gemini_results(gemini_result: dict, segment_combined: str = "", domain_full: str = "") -> dict:
    """
    Очищає результати від Gemini API - валідує номери телефонів та прибирає проблемні значення
    
    Args:
        gemini_result: Словник результатів від Gemini
        segment_combined: Оригінальна сегментація для очистки
        domain_full: Домен для детального логування (опціонально)
        
    Returns:
        Очищений словник результатів
    """
    cleaned_result = {}
    
    # Список нових полів сегментації
    segmentation_fields = [
        "segments_full", "segments_primary", "segments_descriptive", 
        "segments_prefix", "segments_suffix", "segments_thematic", "segments_common"
    ]
    
    for key, value in gemini_result.items():
        if key == "phone_list" and isinstance(value, list):
            # Валідація номерів телефонів
            validated_phones = []
            for phone_data in value:
                if isinstance(phone_data, dict) and phone_data.get("phone_number"):
                    phone = phone_data.get("phone_number", "").strip()
                    
                    # Мінімальна очистка
                    cleaned_phone = clean_phone_for_validation(phone)
                    
                    # Валідація через phonenumbers
                    try:
                        if cleaned_phone:
                            parsed = phonenumbers.parse(cleaned_phone, None)
                            if phonenumbers.is_valid_number(parsed):
                                formatted_number = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                                region_code = phonenumbers.region_code_for_number(parsed) or "UNKNOWN"
                                
                                validated_phones.append({
                                    "phone_number": formatted_number,
                                    "region_code": region_code.upper(),
                                    "whatsapp": phone_data.get("whatsapp", False),
                                    "contact_type": phone_data.get("contact_type", "")
                                })
                    except phonenumbers.NumberParseException:
                        # Якщо валідація не пройшла - пропускаємо номер
                        pass
            
            cleaned_result[key] = validated_phones
            
        elif key == "app_platforms":
            # 🆕 НОВА ОБРОБКА: array → sorted string
            cleaned_result[key] = clean_app_platforms(value)
            
        elif isinstance(value, str):
            if key == "segments_language":
                # Спеціальна очистка для segments_language з правильними пріоритетами
                cleaned_lang = clean_segments_language(value)
                cleaned_result[key] = cleaned_lang
            elif key == "primary_language":
                # 🆕 РОЗУМНА ВАЛІДАЦІЯ для primary_language
                cleaned_result[key] = validate_and_clean_language_code(value)
            elif key in segmentation_fields:
                # Спеціальна обробка для полів сегментації
                cleaned_result[key] = clean_segmentation_field(value, key)
            elif has_access_issues(value, key):
                cleaned_result[key] = ""
            elif key == "summary":
                cleaned_result[key] = format_summary(clean_it_prefix(value))
            elif key in ["similarity_search_phrases", "vector_search_phrase"]:
                cleaned_result[key] = clean_it_prefix(value)
            else:
                cleaned_result[key] = value
        elif isinstance(value, int):
            cleaned_result[key] = value
        else:
            cleaned_result[key] = value
    
    # Застосовуємо очистку сегментаційних полів
    if segment_combined:
        cleaned_result = clean_all_segmentation_fields(segment_combined, cleaned_result)
    
    # 🌍 НОВА ОБРОБКА: валідація географічних полів
    cleaned_result = clean_geo_fields(cleaned_result)
    
    # 🔧 НОВА ФУНКЦІОНАЛЬНІСТЬ: спеціальна обробка segments_full
    cleaned_result = handle_segments_full_validation(cleaned_result, domain_full)
    
    return cleaned_result


if __name__ == "__main__":
    # Тестування validation_utils модуля
    print("=== Validation Utils Test Suite ===\n")
    
    # Тест 1: Email валідація
    print("1. Email Validation Tests:")
    test_emails = [
        "valid@example.com",
        "user.name@domain.co.uk", 
        "invalid-email",
        "@invalid.com",
        "user@",
        "test@domain",
        ""
    ]
    for email in test_emails:
        result = validate_email(email)
        print(f"   '{email}' → {result}")
    
    # Тест 2: Перевірка проблем доступу
    print("\n2. Access Issues Detection:")
    test_texts = [
        "Product management platform",  # Valid
        "unclear",                      # Access issue
        "not detected",                # Access issue
        "Payment service provider",     # Valid
        "unavailable",                 # Access issue
        "en",                          # Valid for language
        "unknown"                      # Context dependent
    ]
    for text in test_texts:
        result = has_access_issues(text)
        print(f"   '{text}' → Has issues: {result}")
    
    # Тест 3: Очистка segments_language
    print("\n3. Segments Language Cleaning:")
    test_languages = [
        "en en",
        "de de de",
        "mixed",
        "en fr", 
        "unknown",
        "mixed unknown",
        "garbage en",
        "123 mixed de"
    ]
    for lang in test_languages:
        result = clean_segments_language(lang)
        print(f"   '{lang}' → '{result}'")
    
    # Тест 4: Очистка app_platforms
    print("\n4. App Platforms Cleaning:")
    test_platforms = [
        ["windows", "android", "chrome", "android"],  # Array з дублікатами
        ["ios", "safari"],                             # Array без дублікатів
        [],                                            # Порожній array
        "windows, chrome, android",                    # String (старий формат)
        "",                                            # Порожній string
        None                                           # None
    ]
    for platforms in test_platforms:
        result = clean_app_platforms(platforms)
        print(f"   {platforms} → '{result}'")
    
    # Тест 5: Географічна валідація
    print("\n5. Geo Fields Validation:")
    test_geo_cases = [
        {"geo_country": "US", "geo_region": "CA", "geo_city": "San Francisco"},      # Валідний
        {"geo_country": "GB", "geo_region": "London", "geo_city": "London"},        # Валідний
        {"geo_country": "USA", "geo_region": "CA", "geo_city": "San Francisco"},    # Невалідний country
        {"geo_country": "123", "geo_region": "CA", "geo_city": "San Francisco"},    # Невалідний country
        {"geo_country": "", "geo_region": "CA", "geo_city": "San Francisco"},       # Порожній country
        {"geo_country": "X", "geo_region": "CA", "geo_city": "San Francisco"},      # Короткий country
    ]
    
    for geo_data in test_geo_cases:
        original = geo_data.copy()
        cleaned = clean_geo_fields(geo_data)
        print(f"   {original} → {cleaned}")
    
    # Тест 6: Валідація мов
    print("\n6. Language Code Validation:")
    test_languages = [
        "en", "DE", "fr", "xy", "zz",       # Двобуквенні коди (ВСІ пропускаються!)
        "zh-tw", "en-us", "fr-ca",          # Locale з дефісом → перші 2 букви
        "en_US", "zh_CN",                   # Locale з underscore → перші 2 букви
        "english", "german", "japanese",    # Повні назви → конвертація
        "français", "español", "português", # Альтернативні назви → конвертація
        "eng", "ger", "jap",                # Скорочені → конвертація
        "123", "toolong", "x", "",          # Невалідні → порожньо
        "unclear", "not detected"           # Access issues → порожньо
    ]
    for lang in test_languages:
        result = validate_and_clean_language_code(lang)
        print(f"   '{lang}' → '{result}'")
    
    # Тест 7: Повний clean_gemini_results з геогафією та мовами
    print("\n7. Full Gemini Results with Geo and Language Validation:")
    test_gemini_result = {
        "segments_full": "w 3 web",
        "segments_language": "en en",
        "app_platforms": ["windows", "chrome"],
        "primary_language": "english",  # Повна назва → en
        "geo_country": "USA",  # Невалідний!
        "geo_region": "California",
        "geo_city": "San Francisco"
    }
    
    print(f"   Before: {test_gemini_result}")
    cleaned_full = clean_gemini_results(test_gemini_result, "w 3", "test-domain.com")
    print(f"   After:  {cleaned_full}")
    
    # 🆕 Тест 8: Спеціальна обробка segments_full
    print("\n8. Segments Full Validation Failed Handling:")
    test_cases = [
        {"segments_full": "valid segment"},   # Валідний - залишається
        {"segments_full": ""},                # Порожній - стає validation_failed
        {"segments_full": "   "},            # Пробіли - стає validation_failed
        {}                                   # Відсутнє - стає validation_failed
    ]
    
    for i, case in enumerate(test_cases):
        original = case.copy()
        result = handle_segments_full_validation(case, f"test-domain-{i}.com")
        print(f"   {original} → {result}")
    
    # 🆕 Тест 9: Детальна валідація segments_full
    print("\n9. Detailed Segments Full Validation:")
    validation_test_cases = [
        ("w 3", "w 3", "match"),                    # Точне співпадіння
        ("w 3", "w3", "normalized_match"),          # Нормалізоване співпадіння  
        ("book store", "bookstore", "normalized_match"), # Нормалізоване співпадіння
        ("w 3", "w 3 extra", "mismatch"),          # Додаткові сегменти
        ("w 3", "web 3", "mismatch"),              # Інші слова
        ("w 3", "", "empty_ai"),                   # Порожній AI результат
        ("", "w 3", "empty_original"),             # Порожній оригінал
    ]
    
    for original, ai_output, expected in validation_test_cases:
        result = validate_segments_full(original, ai_output, f"test-{expected}.com")
        print(f"   '{original}' vs '{ai_output}' → {result} ({expected})")
    
    print(f"\n=== Test completed ===")
    print(f"Module loaded successfully with DETAILED validation logging")
    print("🆕 NEW FEATURES:")
    print("   - validate_segments_full() now shows expected vs actual segments")
    print("   - handle_segments_full_validation() sets 'validation_failed' for empty fields")
    print("   - clean_gemini_results() supports domain_full parameter for logging")
    print("   - All validation errors now include specific domain context")