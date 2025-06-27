#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import phonenumbers
import logging
from urllib.parse import urlparse, urlunparse

# Константи
URL_FIELDS = ["blog_url", "recruits_affiliates_url", "contact_page_url", "api_documentation_url"]

# Налаштування логера для validation_utils
logger = logging.getLogger("validation_utils")


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


def _ai_norm(s: str) -> str:
    """
    Нормалізуємо відповідь ШІ: дефіси → пробіли, далі прибираємо усі пробіли та регістр
    
    Args:
        s: Рядок для нормалізації
        
    Returns:
        Нормалізований рядок
    """
    return re.sub(r'\s+', '', s.replace('-', ' ').lower()) if s else ''


def _orig_norm(s: str) -> str:
    """
    Нормалізуємо segment_combined: прибираємо пробіли та регістр
    
    Args:
        s: Рядок для нормалізації
        
    Returns:
        Нормалізований рядок
    """
    return s.replace(' ', '').lower() if s else ''


def _in(base: str, w: str) -> bool:
    """
    Чи входить слово w (ігноруючи пробіли/дефіси) у base
    
    Args:
        base: Базовий рядок
        w: Слово для пошуку
        
    Returns:
        True якщо слово входить в базовий рядок
    """
    return re.sub(r'[\s\-]+', '', w.lower()) in base


def validate_ai_segmentation(segment_combined: str,
                             ai_semantic_segmentation: str,
                             domain_thematic_parts: str,
                             domain_generic_parts: str) -> bool:
    """
    Перевіряє, чи коректно ШІ сегментував домен
    
    Args:
        segment_combined: Оригінальна сегментація
        ai_semantic_segmentation: AI сегментація
        domain_thematic_parts: Тематичні частини
        domain_generic_parts: Загальні частини
        
    Returns:
        True якщо валідація пройшла
    """
    if not segment_combined or not ai_semantic_segmentation:
        return False

    base = _orig_norm(segment_combined)
    ai = _ai_norm(ai_semantic_segmentation)

    # 1️⃣ Склейка має збігатися
    if base != ai:
        return False

    # 2️⃣ Тематичні та 3️⃣ загальні слова повинні міститися в оригіналі
    for part in (domain_thematic_parts, domain_generic_parts):
        if part:
            for w in part.split():
                if w and not _in(base, w):
                    return False
    return True


def clean_gemini_results(gemini_result: dict) -> dict:
    """
    Очищає результати від Gemini API - валідує номери телефонів та прибирає проблемні значення
    
    Args:
        gemini_result: Словник результатів від Gemini
        
    Returns:
        Очищений словник результатів
    """
    cleaned_result = {}
    
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
            if has_access_issues(value, key):
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
    
    # Тест 3: Телефонна валідація
    print("\n3. Phone Number Validation (E164):")
    test_phones = [
        "+1234567890",
        "+380501234567", 
        "1234567890",     # Missing +
        "+12345",         # Too short
        "+123456789012345678",  # Too long
        ""
    ]
    for phone in test_phones:
        result = validate_phone_e164(phone)
        print(f"   '{phone}' → {result}")
    
    # Тест 4: Очистка телефонів
    print("\n4. Phone Cleaning:")
    dirty_phones = [
        "(555) 123-4567",
        "555.123.4567",
        " +1 555 123 4567 ",
        "123-456-7890"
    ]
    for phone in dirty_phones:
        cleaned = clean_phone_for_validation(phone)
        print(f"   '{phone}' → '{cleaned}'")
    
    # Тест 5: Форматування summary
    print("\n5. Summary Formatting:")
    test_summaries = [
        "payment platform providing secure transactions",
        "This platform offers financial services.",
        "secure messaging app"
    ]
    for summary in test_summaries:
        formatted = format_summary(summary)
        print(f"   '{summary}' → '{formatted}'")
    
    # Тест 6: AI сегментація валідація
    print("\n6. AI Segmentation Validation:")
    test_cases = [
        ("book store", "book store", "book", "store"),      # Valid
        ("bookstore", "book store", "book", "store"),       # Valid  
        ("example", "test fail", "test", "fail"),           # Invalid
        ("techstart", "tech start", "tech", "start")        # Valid
    ]
    for orig, ai_seg, thematic, generic in test_cases:
        result = validate_ai_segmentation(orig, ai_seg, thematic, generic)
        print(f"   '{orig}' → AI:'{ai_seg}' T:'{thematic}' G:'{generic}' → {result}")
    
    # Тест 7: URL валідація
    print("\n7. URL Validation:")
    test_urls = [
        "https://example.com/blog",
        "http://site.org/contact", 
        "invalid-url",
        "ftp://not-supported.com",
        ""
    ]
    target = "https://example.com"
    for url in test_urls:
        result = validate_url_field(url, target)
        print(f"   '{url}' → '{result}'")
    
    print(f"\n=== Test completed ===")
    print(f"Module loaded successfully with {len(globals())} functions")
