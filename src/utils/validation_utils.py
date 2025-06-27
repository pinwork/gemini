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
    
    # Спеціальна обробка для ISO кодів
    iso_code_fields = ["primary_language", "geo_country"]
    if field_name in iso_code_fields and len(field_value.strip()) < 2:
        return True
    
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


def validate_segments_full(segment_combined: str, segments_full: str) -> bool:
    """
    Перевіряє, чи коректно ШІ сегментував домен
    
    Args:
        segment_combined: Оригінальна сегментація (з пробілами)
        segments_full: AI повна сегментація (з пробілами)
        
    Returns:
        True якщо валідація пройшла
    """
    if not segment_combined or not segments_full:
        return False

    # Нормалізуємо: прибираємо пробіли та регістр
    original_normalized = _segments_norm(segment_combined)
    ai_normalized = _segments_norm(segments_full)

    # Склейка має збігатися
    return original_normalized == ai_normalized


def clean_segments_language(language_value: str) -> str:
    """
    Очищає segments_language від подвійних значень
    
    Args:
        language_value: Значення мови (може бути "en en")
        
    Returns:
        Очищене значення ("en")
    """
    if not language_value:
        return language_value
    
    # Розділяємо на слова, прибираємо дублікати, з'єднуємо
    unique_parts = list(dict.fromkeys(language_value.split()))
    return ' '.join(unique_parts) if len(unique_parts) > 1 else unique_parts[0] if unique_parts else ""


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


def clean_gemini_results(gemini_result: dict, segment_combined: str = "") -> dict:
    """
    Очищає результати від Gemini API - валідує номери телефонів та прибирає проблемні значення
    
    Args:
        gemini_result: Словник результатів від Gemini
        segment_combined: Оригінальна сегментація для очистки
        
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
        elif isinstance(value, str):
            if key == "segments_language":
                # Спеціальна очистка для segments_language
                cleaned_lang = clean_segments_language(value)
                cleaned_result[key] = cleaned_lang
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
        "unknown"
    ]
    for lang in test_languages:
        result = clean_segments_language(lang)
        print(f"   '{lang}' → '{result}'")
    
    # Тест 4: Очистка всіх сегментаційних полів
    print("\n4. Segmentation Fields Cleaning:")
    test_data = {
        "segments_full": "w 3 web",
        "segments_primary": "w web", 
        "segments_thematic": "w web tech"
    }
    segment_combined = "w 3"
    
    print(f"   Original: {test_data}")
    cleaned = clean_all_segmentation_fields(segment_combined, test_data.copy())
    print(f"   Cleaned:  {cleaned}")
    
    # Тест 5: Повний clean_gemini_results
    print("\n5. Full Gemini Results Cleaning:")
    test_gemini_result = {
        "segments_full": "w 3 web",
        "segments_language": "en en",
        "segments_thematic": "w web tech",
        "summary": "test platform"
    }
    
    print(f"   Before: {test_gemini_result}")
    cleaned_full = clean_gemini_results(test_gemini_result, "w 3")
    print(f"   After:  {cleaned_full}")
    
    print(f"\n=== Test completed ===")
    print(f"Module loaded successfully with new cleaning logic")
    print("Key changes: segments cleaning, language deduplication, unified validation")