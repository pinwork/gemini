#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import phonenumbers
import logging
from typing import Optional
from urllib.parse import urlparse, urlunparse

URL_FIELDS = ["blog_url", "recruits_affiliates_url", "contact_page_url", "api_documentation_url"]

LANGUAGE_NAME_TO_CODE = {
    "english": "en", "german": "de", "japanese": "ja", "french": "fr", "spanish": "es",
    "indonesian": "id", "russian": "ru", "portuguese": "pt", "dutch": "nl", "italian": "it",
    "chinese": "zh", "korean": "ko", "vietnamese": "vi", "polish": "pl", "turkish": "tr",
    "ukrainian": "uk", "thai": "th", "arabic": "ar", "swedish": "sv", "czech": "cs",
    "hungarian": "hu", "finnish": "fi", "danish": "da", "norwegian": "no", "greek": "el",
    "hebrew": "he", "hindi": "hi",
    
    "deutsch": "de", "français": "fr", "francais": "fr", "español": "es", "espanol": "es",
    "português": "pt", "portugues": "pt", "italiano": "it", "русский": "ru", "russkiy": "ru",
    "nederlands": "nl", "svenska": "sv", "norsk": "no", "suomi": "fi", "magyar": "hu",
    "čeština": "cs", "cestina": "cs", "polski": "pl", "türkçe": "tr", "turkce": "tr",
    
    "eng": "en", "ger": "de", "jap": "ja", "jpn": "ja", "fre": "fr", "spa": "es",
    "por": "pt", "ita": "it", "rus": "ru", "chi": "zh", "kor": "ko", "vie": "vi",
    "pol": "pl", "tur": "tr", "ukr": "uk", "ara": "ar", "swe": "sv", "cze": "cs",
    "hun": "hu", "fin": "fi", "dan": "da", "nor": "no"
}


def clean_phone_for_validation(phone: str) -> str:
    if not phone:
        return ""
    
    cleaned = re.sub(r'[\s\(\)\-\.]', '', phone)
    
    if cleaned and not cleaned.startswith('+') and cleaned[0].isdigit():
        cleaned = '+' + cleaned
    
    return cleaned


def format_summary(summary_text: str) -> str:
    if not summary_text:
        return summary_text
    
    text = summary_text.strip()
    if not text:
        return text
    
    text = text[0].upper() + text[1:] if len(text) > 1 else text.upper()
    
    text = re.sub(r'\. ([a-z])', lambda m: '. ' + m.group(1).upper(), text)
    
    if not text.endswith('.'):
        text += '.'
    
    text = re.sub(r'\.\.+', '.', text)
    
    return text


def clean_it_prefix(text_value: str) -> str:
    if not text_value:
        return text_value
    
    if text_value.lower().startswith("it "):
        return text_value[3:].strip()
    
    return text_value


def clean_app_platforms(app_platforms_value) -> str:
    if not app_platforms_value:
        return ""
    
    VALID_PLATFORMS = {
        "android", "ios", "windows", "macos", "linux", 
        "chrome", "firefox", "edge", "safari", "opera"
    }
    
    if isinstance(app_platforms_value, list):
        platforms_text = ", ".join(str(item) for item in app_platforms_value if item)
    else:
        platforms_text = str(app_platforms_value)
    
    platforms = []
    for item in platforms_text.replace(",", " ").split():
        platform = item.strip().lower()
        if platform in VALID_PLATFORMS:
            platforms.append(platform)
    
    unique_platforms = list(dict.fromkeys(platforms))
    
    return ", ".join(sorted(unique_platforms))


def has_access_issues(field_value: str, field_name: str = "") -> bool:
    if not field_value:
        return False
        
    field_lower = field_value.strip().lower()
    
    enum_fields_with_unspecified = ["target_age_group", "target_gender", "domain_formation_pattern"]
    if field_name in enum_fields_with_unspecified and field_lower == "unspecified":
        return False
    
    if field_name == "segments_language":
        special_values = {"mixed", "unknown"}
        if field_lower in special_values or (len(field_value.strip()) == 2 and field_value.strip().isalpha()):
            return False
    
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
    
    if field_lower == "unknown" and field_name != "segments_language":
        access_issues.append(True)
    
    return any(access_issues)


def validate_country_code(country_code: str) -> bool:
    if not country_code or len(country_code.strip()) != 2:
        return False
    return country_code.strip().isalpha()


def validate_and_clean_language_code(language_value: str) -> str:
    if not language_value:
        return ""
    
    cleaned = language_value.strip().lower()
    if not cleaned:
        return ""
    
    if len(cleaned) == 2 and cleaned.isalpha():
        return cleaned
    
    if "-" in cleaned and len(cleaned) <= 6:
        language_part = cleaned.split("-")[0]
        if len(language_part) == 2 and language_part.isalpha():
            return language_part
    
    if "_" in cleaned and len(cleaned) <= 6:
        language_part = cleaned.split("_")[0]
        if len(language_part) == 2 and language_part.isalpha():
            return language_part
    
    if cleaned in LANGUAGE_NAME_TO_CODE:
        return LANGUAGE_NAME_TO_CODE[cleaned]
    
    for lang_name, lang_code in LANGUAGE_NAME_TO_CODE.items():
        if lang_name in cleaned or cleaned in lang_name:
            if len(lang_name) >= 4 and len(cleaned) >= 4:
                return lang_code
    
    return ""


def validate_email(email: str) -> bool:
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
    if not phone or not phone.startswith("+"):
        return False
    digits = phone[1:]
    if not digits.isdigit() or len(digits) < 7 or len(digits) > 15:
        return False
    return True


def validate_segments_language(segments_language: str, segmentation_logger: Optional[logging.Logger] = None) -> bool:
    if not segments_language:
        return False
    
    language_code = segments_language.strip().lower()
    
    special_values = {"mixed", "unknown"}
    if language_code in special_values:
        return True
    
    if len(language_code) == 2 and language_code.isalpha():
        return True
    
    if segmentation_logger:
        segmentation_logger.warning(f"Invalid segments_language: '{segments_language}' - must be 2-letter ISO code or 'mixed'/'unknown'")
    
    return False


def normalize_url(url_value: str) -> str:
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


def validate_url_field(url_value: str, base_domain: str) -> str:
    if not url_value:
        return url_value
    
    normalized_url = normalize_url(url_value)
    
    if not normalized_url:
        return ""
    
    base_url = f"https://{base_domain}/"
    normalized_base = normalize_url(base_url)
    
    if normalized_url.lower() == normalized_base.lower():
        return ""
    
    return normalized_url


def _segments_norm(s: str) -> str:
    return s.replace(' ', '').lower() if s else ''


def validate_segments_full(segment_combined: str, segments_full: str, domain_full: str = "", segmentation_logger: Optional[logging.Logger] = None) -> bool:
    if not segment_combined:
        return False
    
    if not segments_full:
        if domain_full and segmentation_logger:
            segmentation_logger.warning(f"Domain {domain_full}: segments_full validation failed | AI returned: <empty>")
        return False

    original_normalized = _segments_norm(segment_combined)
    ai_normalized = _segments_norm(segments_full)

    validation_passed = original_normalized == ai_normalized
    
    if not validation_passed and domain_full and segmentation_logger:
        segmentation_logger.warning(f"Domain {domain_full}: segments_full validation failed | AI returned: '{segments_full}'")
    
    return validation_passed


def validate_segments_full_only(segment_combined: str, segments_full: str, domain_full: str = "") -> bool:
    if not segment_combined:
        return False
    
    if not segments_full:
        return False

    if has_access_issues(segments_full, "segments_full"):
        return False

    original_normalized = _segments_norm(segment_combined)
    ai_normalized = _segments_norm(segments_full)

    return original_normalized == ai_normalized


def clean_segments_language(language_value: str) -> str:
    if not language_value:
        return ""
    
    parts = language_value.strip().split()
    if not parts:
        return ""
    
    special_values = {"mixed", "unknown"}
    
    for part in parts:
        part_lower = part.lower()
        if part_lower in special_values:
            return part_lower
    
    for part in parts:
        part_clean = part.strip().lower()
        if len(part_clean) == 2 and part_clean.isalpha():
            return part_clean
    
    return parts[0].lower()


def clean_segmentation_field(field_value: str, field_name: str) -> str:
    if not field_value or has_access_issues(field_value, field_name):
        return ""
    return field_value.strip()


def clean_all_segmentation_fields(segment_combined: str, gemini_result: dict) -> dict:
    if not segment_combined:
        return gemini_result
    
    segment_combined_joined = segment_combined.replace(" ", "")
    
    segmentation_fields = [
        "segments_full", "segments_primary", "segments_descriptive", 
        "segments_prefix", "segments_suffix", "segments_thematic", "segments_common"
    ]
    
    for field_name in segmentation_fields:
        if field_name in gemini_result:
            field_value = gemini_result[field_name]
            if field_value:
                cleaned_segments = [seg for seg in field_value.split() 
                                  if seg in segment_combined_joined]
                gemini_result[field_name] = " ".join(cleaned_segments)
    
    return gemini_result


def clean_geo_fields(gemini_result: dict) -> dict:
    geo_country = gemini_result.get("geo_country", "").strip()
    
    if geo_country and validate_country_code(geo_country):
        gemini_result["geo_country"] = geo_country.upper()
    else:
        gemini_result["geo_country"] = ""
        gemini_result["geo_region"] = ""
        gemini_result["geo_city"] = ""
    
    return gemini_result


def handle_segments_full_validation(gemini_result: dict, domain_full: str = "", segmentation_logger: Optional[logging.Logger] = None) -> dict:
    segments_full = gemini_result.get("segments_full", "").strip()
    
    if not segments_full:
        gemini_result["segments_full"] = "validation_failed"
        if domain_full and segmentation_logger:
            segmentation_logger.info(f"Domain {domain_full}: segments_full set to 'validation_failed' due to empty value after cleaning")
    
    return gemini_result


def clean_gemini_results(gemini_result: dict, segment_combined: str = "", domain_full: str = "", segmentation_logger: Optional[logging.Logger] = None) -> dict:
    cleaned_result = {}
    
    segmentation_fields = [
        "segments_full", "segments_primary", "segments_descriptive", 
        "segments_prefix", "segments_suffix", "segments_thematic", "segments_common"
    ]
    
    for key, value in gemini_result.items():
        if key == "phone_list" and isinstance(value, list):
            validated_phones = []
            for phone_data in value:
                if isinstance(phone_data, dict) and phone_data.get("phone_number"):
                    phone = phone_data.get("phone_number", "").strip()
                    
                    cleaned_phone = clean_phone_for_validation(phone)
                    
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
                        pass
            
            cleaned_result[key] = validated_phones
            
        elif key == "app_platforms":
            cleaned_result[key] = clean_app_platforms(value)
            
        elif isinstance(value, str):
            if key == "segments_language":
                cleaned_lang = clean_segments_language(value)
                cleaned_result[key] = cleaned_lang
            elif key == "primary_language":
                cleaned_result[key] = validate_and_clean_language_code(value)
            elif key in segmentation_fields:
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
    
    if segment_combined:
        cleaned_result = clean_all_segmentation_fields(segment_combined, cleaned_result)
    
    cleaned_result = clean_geo_fields(cleaned_result)
    
    cleaned_result = handle_segments_full_validation(cleaned_result, domain_full, segmentation_logger)
    
    return cleaned_result


if __name__ == "__main__":
    print("=== Validation Utils Test Suite ===\n")
    
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
    
    print("\n2. Access Issues Detection:")
    test_texts = [
        "Product management platform",
        "unclear",
        "not detected",
        "Payment service provider",
        "unavailable",
        "en",
        "unknown"
    ]
    for text in test_texts:
        result = has_access_issues(text)
        print(f"   '{text}' → Has issues: {result}")
    
    print("\n3. NEW: Segments Full Only Validation (for retry logic):")
    test_cases = [
        ("book store", "book store", "Perfect match"),
        ("book store", "bookstore", "Normalized match"),  
        ("w 3", "w3", "Short normalized match"),
        ("book store", "book shop", "Different words"),
        ("book store", "book store extra", "Extra segments"),
        ("book store", "", "Empty AI result"),
        ("book store", "unclear", "Access issue"),
        ("book store", "not detected", "Access issue"),
        ("", "book store", "Empty original"),
    ]
    
    for original, ai_result, description in test_cases:
        is_valid = validate_segments_full_only(original, ai_result, "test-domain.com")
        status = "✅ VALID" if is_valid else "❌ INVALID"
        print(f"   '{original}' vs '{ai_result}' → {status} ({description})")
    
    print("\n4. Segments Language Cleaning:")
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
    
    print("\n5. NEW: App Platforms String Cleaning:")
    test_platforms = [
        "windows, android, chrome, android",
        "ios, safari, badplatform, chrome",
        "WINDOWS CHROME android",
        "ios,safari,chrome,firefox,edge,safari",
        "",
        ["windows", "android", "chrome", "android"],
        ["ios", "safari", "badplatform"],
        [],
        None
    ]
    for platforms in test_platforms:
        result = clean_app_platforms(platforms)
        print(f"   {platforms} → '{result}'")
    
    print("\n6. Geo Fields Validation:")
    test_geo_cases = [
        {"geo_country": "US", "geo_region": "CA", "geo_city": "San Francisco"},
        {"geo_country": "GB", "geo_region": "London", "geo_city": "London"},
        {"geo_country": "USA", "geo_region": "CA", "geo_city": "San Francisco"},
        {"geo_country": "123", "geo_region": "CA", "geo_city": "San Francisco"},
        {"geo_country": "", "geo_region": "CA", "geo_city": "San Francisco"},
        {"geo_country": "X", "geo_region": "CA", "geo_city": "San Francisco"},
    ]
    
    for geo_data in test_geo_cases:
        original = geo_data.copy()
        cleaned = clean_geo_fields(geo_data)
        print(f"   {original} → {cleaned}")
    
    print("\n7. Language Code Validation:")
    test_languages = [
        "en", "DE", "fr", "xy", "zz",
        "zh-tw", "en-us", "fr-ca",
        "en_US", "zh_CN",
        "english", "german", "japanese",
        "français", "español", "português",
        "eng", "ger", "jap",
        "123", "toolong", "x", "",
        "unclear", "not detected"
    ]
    for lang in test_languages:
        result = validate_and_clean_language_code(lang)
        print(f"   '{lang}' → '{result}'")
    
    print(f"\n=== Test completed ===")
    print(f"🆕 NEW FUNCTION: validate_segments_full_only() for retry logic")
    print(f"🆕 UPDATED FUNCTION: clean_app_platforms() now handles string input with validation")
    print(f"🆕 This function validates against specific platform list and removes duplicates")
    print(f"Module loaded successfully with ENHANCED validation and retry support")