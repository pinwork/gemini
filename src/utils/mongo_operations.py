#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import logging
import inspect
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Tuple, Optional, Dict
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import (
    DuplicateKeyError,
    AutoReconnect,
    NetworkTimeout,
    ServerSelectionTimeoutError,
    ConnectionFailure,
    OperationFailure
)
from bson import ObjectId

RETRY_DELAY = 10

def _retry_forever(coro):
    async def wrapper(*args, **kwargs):
        logger = logging.getLogger("mongo_operations")
        retry_count = 0
        
        while True:
            try:
                return await coro(*args, **kwargs)
            except (
                AutoReconnect,
                NetworkTimeout,
                ServerSelectionTimeoutError,
                ConnectionFailure,
                OperationFailure,
            ) as e:
                retry_count += 1
                
                error_type = type(e).__name__
                error_str = str(e)
                
                if "getaddrinfo failed" in error_str:
                    short_msg = "DNS resolution failed - check MongoDB host config"
                elif "authentication failed" in error_str.lower():
                    short_msg = "MongoDB authentication failed"
                elif "timeout" in error_str.lower():
                    short_msg = "MongoDB connection timeout"
                else:
                    short_msg = f"MongoDB connection issue ({error_type})"
                
                if retry_count == 1:
                    logger.warning(f"🔄 {short_msg}, retrying every {RETRY_DELAY}s...")
                elif retry_count % 6 == 0:
                    logger.warning(f"🔄 Still retrying MongoDB connection (attempt #{retry_count})")
                
                await asyncio.sleep(RETRY_DELAY)
                
            except Exception:
                raise
    return wrapper

for _cls in (
    AsyncIOMotorClient,
    AsyncIOMotorClient.__bases__[0],
    AsyncIOMotorClient.__bases__[0].__bases__[0],
):
    for _name, _attr in _cls.__dict__.items():
        if inspect.iscoroutinefunction(_attr):
            setattr(_cls, _name, _retry_forever(_attr))

try:
    from .proxy_config import ProxyConfig
    from .validation_utils import (
        has_access_issues, validate_country_code, validate_email, validate_phone_e164,
        validate_segments_language, clean_gemini_results, validate_url_field,
        validate_segments_full
    )
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent.parent))
    from utils.proxy_config import ProxyConfig
    from utils.validation_utils import (
        has_access_issues, validate_country_code, validate_email, validate_phone_e164,
        validate_segments_language, clean_gemini_results, validate_url_field,
        validate_segments_full
    )

API_KEY_WAIT_TIME = 60
DOMAIN_WAIT_TIME = 60

logger = logging.getLogger("mongo_operations")

def _load_mongo_config() -> dict:
    config_path = Path(__file__).parent.parent.parent / "config" / "mongo_config.json"
    try:
        with config_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"MongoDB configuration file not found at {config_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in MongoDB configuration file at {config_path}")

def _load_script_control() -> dict:
    config_path = Path(__file__).parent.parent.parent / "config" / "script_control.json"
    try:
        with config_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "stage_timings": {
                "stage1": {"cooldown_minutes": 3, "api_provider": "gemini"},
                "stage2": {"cooldown_minutes": 2, "api_provider": "gemini"}
            }
        }
    except json.JSONDecodeError:
        logger.error("Invalid JSON in script control file, using defaults")
        return {
            "stage_timings": {
                "stage1": {"cooldown_minutes": 3, "api_provider": "gemini"},
                "stage2": {"cooldown_minutes": 2, "api_provider": "gemini"}
            }
        }

MONGO_CONFIG = _load_mongo_config()
SCRIPT_CONFIG = _load_script_control()

def get_timestamp_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def needs_ip_refresh(key_rec: dict) -> bool:
    ip = key_rec.get("current_ip", "")
    return not ("." in ip or ":" in ip)

async def get_domain_for_analysis(mongo_client: AsyncIOMotorClient) -> Tuple[str, str, str]:
    db_name = MONGO_CONFIG["databases"]["main_db"]["name"]
    collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["domain_main"]
    
    while True:
        domain_collection = mongo_client[db_name][collection_name]
        
        domain_record = await domain_collection.find_one_and_update(
            {"status": "processed"},
            {
                "$set": {"status": "processed_gemini"},
                "$inc": {"url_context_try": 1}
            },
            return_document=ReturnDocument.AFTER
        )
        
        if not domain_record:
            if not hasattr(get_domain_for_analysis, 'wait_count'):
                get_domain_for_analysis.wait_count = 0
            get_domain_for_analysis.wait_count += 1
            
            if get_domain_for_analysis.wait_count % 10 == 0:
                logger.warning(f"No domains available for analysis, waiting... (attempt {get_domain_for_analysis.wait_count})")
            
            await asyncio.sleep(DOMAIN_WAIT_TIME)
            continue
        
        return domain_record["target_uri"], domain_record["domain_full"], str(domain_record["_id"])

async def get_api_key_and_proxy(mongo_client: AsyncIOMotorClient, stage: str = "stage1") -> Tuple[str, ProxyConfig, str, dict]:
    stage_config = SCRIPT_CONFIG["stage_timings"].get(stage, SCRIPT_CONFIG["stage_timings"]["stage1"])
    cooldown_minutes = stage_config["cooldown_minutes"]
    api_provider = stage_config["api_provider"]
    
    api_db_name = MONGO_CONFIG["databases"]["api_db"]["name"]
    api_collection_name = MONGO_CONFIG["databases"]["api_db"]["collections"]["keys"]
    
    while True:
        current_time = datetime.now(timezone.utc)
        cooldown_ago = current_time - timedelta(minutes=cooldown_minutes)
        
        api_keys_collection = mongo_client[api_db_name][api_collection_name]
        
        api_key_record = await api_keys_collection.find_one_and_update(
            {
                "api_provider": api_provider,
                "api_status": "active",
                "api_last_used_date": {"$lt": cooldown_ago},
                "proxy_ip": {"$ne": None, "$ne": ""}
            },
            {
                "$set": {"api_last_used_date": current_time}
            },
            return_document=ReturnDocument.AFTER
        )
        
        if not api_key_record:
            if not hasattr(get_api_key_and_proxy, 'wait_count'):
                get_api_key_and_proxy.wait_count = 0
            get_api_key_and_proxy.wait_count += 1
            
            if get_api_key_and_proxy.wait_count % 10 == 0:
                logger.warning(f"No available {api_provider} API keys for {stage} (cooldown: {cooldown_minutes}min), waiting... (attempt {get_api_key_and_proxy.wait_count})")
            await asyncio.sleep(API_KEY_WAIT_TIME)
            continue
        
        try:
            api_key = api_key_record["api_key"]
            key_record_id = str(api_key_record["_id"])
            
            protocol = api_key_record.get("proxy_protocol", "").strip().lower()
            ip = api_key_record.get("proxy_ip", "").strip()
            port = api_key_record.get("proxy_port")
            username = api_key_record.get("proxy_username", "").strip() or None
            password = api_key_record.get("proxy_password", "").strip() or None
            
            if not protocol or not ip or not port:
                logger.error(f"Invalid proxy data in API key record: protocol={protocol}, ip={ip}, port={port}")
                continue
            
            proxy_config = ProxyConfig(
                protocol=protocol,
                ip=ip,
                port=int(port),
                username=username,
                password=password
            )
            
            return api_key, proxy_config, key_record_id, api_key_record
            
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error parsing API key record: {e}")
            continue

async def finalize_api_key_usage(mongo_client: AsyncIOMotorClient, key_record_id: str, 
                                status_code: Optional[int] = None, is_proxy_error: bool = False, 
                                working_proxy: Optional[ProxyConfig] = None, 
                                freeze_minutes: Optional[int] = None) -> None:
    try:
        api_db_name = MONGO_CONFIG["databases"]["api_db"]["name"]
        api_collection_name = MONGO_CONFIG["databases"]["api_db"]["collections"]["keys"]
        
        api_keys_collection = mongo_client[api_db_name][api_collection_name]
        current_time = datetime.now(timezone.utc)
        
        update_query = {"$set": {"api_last_used_date": current_time}}
        
        if status_code == 200:
            update_query["$inc"] = {"request_count_200": 1}
            update_query["$set"]["request_count_429"] = 0
        elif status_code == 429:
            update_query["$inc"] = {"request_count_429": 1}
        
        if status_code is not None:
            update_query["$set"]["last_response_status"] = status_code
        
        if is_proxy_error:
            if "$inc" not in update_query:
                update_query["$inc"] = {}
            update_query["$inc"]["proxy_error_count"] = 1
        
        if working_proxy and working_proxy.username:
            update_query["$set"]["proxy_username"] = working_proxy.username
        
        result = await api_keys_collection.update_one(
            {"_id": ObjectId(key_record_id)},
            update_query
        )
        
        if result.modified_count == 0:
            logger.warning(f"Failed to finalize API key usage for ID: {key_record_id}")
            
    except Exception as e:
        logger.error(f"Error finalizing API key usage: {e}")

async def revert_domain_status(mongo_client: AsyncIOMotorClient, domain_id: str, 
                              reason: str = "", revert_logger: Optional[logging.Logger] = None) -> None:
    try:
        db_name = MONGO_CONFIG["databases"]["main_db"]["name"]
        collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["domain_main"]
        
        domain_collection = mongo_client[db_name][collection_name]
        
        result = await domain_collection.update_one(
            {"_id": ObjectId(domain_id)},
            {
                "$set": {
                    "status": "processed",
                    "updated_at": get_timestamp_ms()
                },
                "$inc": {"url_context_try": -1}
            }
        )
        
        if result.modified_count > 0:
            if revert_logger:
                revert_logger.info(f"Domain ID: {domain_id} | Reason: {reason}")
        else:
            logger.warning(f"Could not revert status for domain_id: {domain_id}")
            
    except Exception as e:
        logger.error(f"Error reverting domain status: {e}")

async def set_domain_error_status(mongo_client: AsyncIOMotorClient, domain_id: str, error_reason: str = "") -> None:
    try:
        db_name = MONGO_CONFIG["databases"]["main_db"]["name"]
        collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["domain_main"]
        
        domain_collection = mongo_client[db_name][collection_name]
        
        update_data = {
            "status": "processed_gemini_error",
            "updated_at": get_timestamp_ms()
        }
        
        if error_reason:
            update_data["error"] = error_reason
        
        result = await domain_collection.update_one(
            {"_id": ObjectId(domain_id)},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            pass
        else:
            logger.warning(f"Could not set error status for domain_id: {domain_id}")
            
    except Exception as e:
        logger.error(f"Error setting domain error status: {e}")

async def get_domain_segmentation_info(mongo_client: AsyncIOMotorClient, domain_full: str) -> str:
    try:
        db_name = MONGO_CONFIG["databases"]["main_db"]["name"]
        collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["domain_segmented"]
        
        segmentation_collection = mongo_client[db_name][collection_name]
        
        segmentation_record = await segmentation_collection.find_one(
            {"domain_full": domain_full},
            {"segment_combined": 1}
        )
        
        if segmentation_record and segmentation_record.get("segment_combined"):
            return segmentation_record["segment_combined"]
        
        return ""
    except Exception as e:
        logger.error(f"Error getting domain segmentation info for {domain_full}: {e}")
        return ""

async def save_contact_information(mongo_client: AsyncIOMotorClient, domain_full: str, gemini_result: dict) -> None:
    try:
        db_name = MONGO_CONFIG["databases"]["main_db"]["name"]
        
        email_list = gemini_result.get("email_list", [])
        if email_list and isinstance(email_list, list):
            email_collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["gemini_email_list"]
            email_collection = mongo_client[db_name][email_collection_name]
            
            for email_data in email_list:
                if isinstance(email_data, dict) and email_data.get("contact_email"):
                    email = email_data.get("contact_email", "").strip()
                    contact_type = email_data.get("contact_type", "").strip()
                    
                    if (has_access_issues(email) or has_access_issues(contact_type) or 
                        not validate_email(email)):
                        continue
                    
                    email_doc = {
                        "domain_full": domain_full,
                        "contact_email": email.lower(),
                        "contact_type": contact_type.lower(),
                        "corporate": email_data.get("corporate", False)
                    }
                    await email_collection.insert_one(email_doc)
        
        phone_list = gemini_result.get("phone_list", [])
        if phone_list and isinstance(phone_list, list):
            phone_collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["gemini_phone_list"]
            phone_collection = mongo_client[db_name][phone_collection_name]
            
            for phone_data in phone_list:
                if isinstance(phone_data, dict) and phone_data.get("phone_number"):
                    phone = phone_data.get("phone_number", "").strip()
                    contact_type = phone_data.get("contact_type", "").strip()
                    region_code = phone_data.get("region_code", "").strip()
                    
                    if (has_access_issues(phone) or has_access_issues(contact_type) or 
                        not validate_phone_e164(phone)):
                        continue
                    
                    phone_doc = {
                        "domain_full": domain_full,
                        "phone_number": phone,
                        "region_code": region_code,
                        "whatsapp": phone_data.get("whatsapp", False),
                        "contact_type": contact_type.lower()
                    }
                    await phone_collection.insert_one(phone_doc)
        
        address_list = gemini_result.get("address_list", [])
        if address_list and isinstance(address_list, list):
            address_collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["gemini_address_list"]
            address_collection = mongo_client[db_name][address_collection_name]
            
            for address_data in address_list:
                if isinstance(address_data, dict) and address_data.get("full_address"):
                    full_address = address_data.get("full_address", "").strip()
                    address_type = address_data.get("address_type", "").strip()
                    country_code = address_data.get("country", "").strip()
                    
                    if (has_access_issues(full_address) or has_access_issues(address_type) or 
                        has_access_issues(country_code) or len(full_address) < 10):
                        continue
                    
                    if country_code and not validate_country_code(country_code):
                        country_code = ""
                    
                    address_doc = {
                        "domain_full": domain_full,
                        "full_address": full_address,
                        "address_type": address_type.lower(),
                        "country": country_code.lower()
                    }
                    await address_collection.insert_one(address_doc)
                    
    except Exception as e:
        logger.error(f"Error saving contact information for {domain_full}: {e}", exc_info=True)

def _segments_norm(s: str) -> str:
    return s.replace(' ', '').lower() if s else ''

async def save_gemini_results(mongo_client: AsyncIOMotorClient, domain_full: str, 
                             gemini_result: dict, grounding_status: str, domain_id: str, 
                             segment_combined: str = "", revert_logger: Optional[logging.Logger] = None,
                             segmentation_logger: Optional[logging.Logger] = None) -> None:
    db_name = MONGO_CONFIG["databases"]["main_db"]["name"]
    gemini_collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["gemini"]
    gemini_collection = mongo_client[db_name][gemini_collection_name]
    
    original_segments_full = gemini_result.get("segments_full", "")
    
    cleaned_result = clean_gemini_results(gemini_result, segment_combined, domain_full)
    
    summary = cleaned_result.get("summary", "").strip()
    similarity_search_phrases = cleaned_result.get("similarity_search_phrases", "").strip()
    vector_search_phrase = cleaned_result.get("vector_search_phrase", "").strip()
    
    if not summary or not similarity_search_phrases or not vector_search_phrase:
        await revert_domain_status(mongo_client, domain_id, "missing_required_fields", revert_logger)
        return
    
    if (has_access_issues(summary, "summary") or 
        has_access_issues(similarity_search_phrases, "similarity_search_phrases") or
        has_access_issues(vector_search_phrase, "vector_search_phrase")):
        await revert_domain_status(mongo_client, domain_id, "access_issues", revert_logger)
        return
    
    if len(summary) < 15:
        await revert_domain_status(mongo_client, domain_id, "summary_too_short", revert_logger)
        return
    
    base_url = f"https://{domain_full}"
    
    document = {
        "domain_full": domain_full,
        "updated_at": datetime.now(timezone.utc),
        "grounding": grounding_status == "URL_RETRIEVAL_STATUS_SUCCESS",
        
        "summary": cleaned_result.get("summary", ""),
        "similarity_search_phrases": cleaned_result.get("similarity_search_phrases", "").lower(),
        "vector_search_phrase": cleaned_result.get("vector_search_phrase", "").lower(),
        "target_age_group": cleaned_result.get("target_age_group", "all_ages").lower(),
        "target_gender": cleaned_result.get("target_gender", "unspecified").lower(),
        "geo_scope": cleaned_result.get("geo_scope", "").lower(),
        "cms_platform": cleaned_result.get("cms_platform", "").lower(),
        "primary_language": cleaned_result.get("primary_language", "").lower(),
        
        "external_links_count": cleaned_result.get("external_links_count", 0),
        "external_domains_count": cleaned_result.get("external_domains_count", 0),
        "internal_links_count": cleaned_result.get("internal_links_count", 0),
        "internal_pages_count": cleaned_result.get("internal_pages_count", 0),
        
        "b2c_detected": cleaned_result.get("b2c_detected", False),
        "b2b_detected": cleaned_result.get("b2b_detected", False),
        
        "pricing_page_detected": cleaned_result.get("pricing_page_detected", False),
        "blog_detected": cleaned_result.get("blog_detected", False),
        "ecommerce_detected": cleaned_result.get("ecommerce_detected", False),
        "hiring_detected": cleaned_result.get("hiring_detected", False),
        "api_available_detected": cleaned_result.get("api_available_detected", False),
        "contact_page_detected": cleaned_result.get("contact_page_detected", False),
        "payment_methods_detected": cleaned_result.get("payment_methods_detected", False),
        "analytics_tools_detected": cleaned_result.get("analytics_tools_detected", False),
        "knowledge_base_detected": cleaned_result.get("knowledge_base_detected", False),
        
        "subscription_detected": cleaned_result.get("subscription_detected", False),
        "monetizes_via_ads_detected": cleaned_result.get("monetizes_via_ads_detected", False),
        "saas_detected": cleaned_result.get("saas_detected", False),
        "recruits_affiliates_detected": cleaned_result.get("recruits_affiliates_detected", False),
        "community_platform_detected": cleaned_result.get("community_platform_detected", False),
        "funding_received_detected": cleaned_result.get("funding_received_detected", False),
        "disposable_site_detected": cleaned_result.get("disposable_site_detected", False),
        
        "personal_project_detected": cleaned_result.get("personal_project_detected", False),
        "local_business_detected": cleaned_result.get("local_business_detected", False),
        "mobile_first_detected": cleaned_result.get("mobile_first_detected", False),
        
        "blog_url": validate_url_field(cleaned_result.get("blog_url", ""), base_url).lower(),
        "recruits_affiliates_url": validate_url_field(cleaned_result.get("recruits_affiliates_url", ""), base_url).lower(),
        "contact_page_url": validate_url_field(cleaned_result.get("contact_page_url", ""), base_url).lower(),
        "api_documentation_url": validate_url_field(cleaned_result.get("api_documentation_url", ""), base_url).lower(),
        
        "app_platforms": cleaned_result.get("app_platforms", "").lower(),
        
        "geo_country": cleaned_result.get("geo_country", "").lower(),
        "geo_region": cleaned_result.get("geo_region", "").lower(),
        "geo_city": cleaned_result.get("geo_city", "").lower()
    }
    
    await gemini_collection.insert_one(document)
    
    await save_contact_information(mongo_client, domain_full, cleaned_result)
    
    try:
        segmentation_collection_name = MONGO_CONFIG["databases"]["main_db"]["collections"]["domain_segmented"]
        segmentation_collection = mongo_client[db_name][segmentation_collection_name]
        segmentation_update = {}
        
        segments_full = cleaned_result.get("segments_full", "")
        segments_primary = cleaned_result.get("segments_primary", "")
        segments_descriptive = cleaned_result.get("segments_descriptive", "")
        segments_prefix = cleaned_result.get("segments_prefix", "")
        segments_suffix = cleaned_result.get("segments_suffix", "")
        segments_thematic = cleaned_result.get("segments_thematic", "")
        segments_common = cleaned_result.get("segments_common", "")
        
        if segments_full:
            if segments_full == "validation_failed":
                segmentation_update["segments_full"] = segments_full
            elif not segment_combined:
                segmentation_update["segments_full"] = segments_full
            else:
                original_normalized = _segments_norm(segment_combined)
                ai_normalized = _segments_norm(segments_full)
                
                if original_normalized == ai_normalized:
                    segmentation_update["segments_full"] = segments_full
                    
                    if segments_primary:
                        segmentation_update["segments_primary"] = segments_primary
                    if segments_descriptive:
                        segmentation_update["segments_descriptive"] = segments_descriptive
                    if segments_prefix:
                        segmentation_update["segments_prefix"] = segments_prefix
                    if segments_suffix:
                        segmentation_update["segments_suffix"] = segments_suffix
                    if segments_thematic:
                        segmentation_update["segments_thematic"] = segments_thematic
                    if segments_common:
                        segmentation_update["segments_common"] = segments_common
                else:
                    if segmentation_logger:
                        segmentation_logger.warning(f"Domain {domain_full}: segments_full validation failed | AI returned: '{original_segments_full}' | After cleaning: '{segments_full}'")
        else:
            if segmentation_logger:
                segmentation_logger.warning(f"Domain {domain_full}: segments_full validation failed | AI returned: '{original_segments_full}' | After cleaning: <empty>")
        
        segments_language = cleaned_result.get("segments_language", "")
        if segments_language and validate_segments_language(segments_language):
            segmentation_update["segments_language"] = segments_language
        elif segments_language:
            if segmentation_logger:
                segmentation_logger.warning(f"Invalid segments_language '{segments_language}' for domain: {domain_full} - not saved")
            else:
                logger.warning(f"Invalid segments_language '{segments_language}' for domain: {domain_full} - not saved")
        
        if cleaned_result.get("domain_formation_pattern"):
            segmentation_update["domain_formation_pattern"] = cleaned_result.get("domain_formation_pattern", "unknown_type")
        
        if segmentation_update:
            await segmentation_collection.update_one(
                {"domain_full": domain_full},
                {"$set": segmentation_update}
            )
    except Exception as e:
        logger.error(f"Error updating domain_segmented collection for {domain_full}: {e}")


async def save_gemini_results_with_validation_failed(mongo_client: AsyncIOMotorClient, domain_full: str, 
                                                   gemini_result: dict, grounding_status: str, domain_id: str, 
                                                   segment_combined: str = "", retry_count: int = 0,
                                                   stage2_retries_logger: Optional[logging.Logger] = None,
                                                   last_failed_segments_full: str = "",
                                                   last_cleaned_segments_full: str = "") -> None:
    if stage2_retries_logger:
        stage2_retries_logger.info(f"Domain {domain_full}: MAX RETRIES EXCEEDED ({retry_count} attempts) - using validation_failed fallback | Expected: '{segment_combined}' | AI original: '{last_failed_segments_full}' | AI cleaned: '{last_cleaned_segments_full}'")
    else:
        logger.warning(f"Domain {domain_full}: MAX RETRIES EXCEEDED ({retry_count} attempts) - using validation_failed fallback")
    
    gemini_result_copy = gemini_result.copy()
    gemini_result_copy["segments_full"] = "validation_failed"
    
    await save_gemini_results(
        mongo_client=mongo_client,
        domain_full=domain_full,
        gemini_result=gemini_result_copy,
        grounding_status=grounding_status,
        domain_id=domain_id,
        segment_combined=segment_combined,
        revert_logger=None,
        segmentation_logger=None
    )

async def update_api_key_ip(mongo_client: AsyncIOMotorClient, key_id: str, ip: str, 
                           ip_logger: Optional[logging.Logger] = None) -> bool:
    try:
        api_db_name = MONGO_CONFIG["databases"]["api_db"]["name"]
        api_collection_name = MONGO_CONFIG["databases"]["api_db"]["collections"]["keys"]
        
        api_keys_coll = mongo_client[api_db_name][api_collection_name]
        
        await api_keys_coll.update_one(
            {"_id": ObjectId(key_id)},
            {"$set": {"current_ip": ip}}
        )
        
        if ip_logger:
            ip_logger.info(f"IP assigned: {ip} | Key: {key_id}")
        
        return True
        
    except DuplicateKeyError:
        logger.warning(f"Duplicate IP {ip} for key {key_id}")
        return False

async def retry_mongo_operation(operation, *args, **kwargs):
    while True:
        try:
            return await operation(*args, **kwargs)
        except (
            AutoReconnect,
            NetworkTimeout,
            ServerSelectionTimeoutError,
            ConnectionFailure,
            OperationFailure,
        ) as e:
            logger.warning(f"MongoDB operation failed: {e}. Retrying in {RETRY_DELAY} seconds...")
            await asyncio.sleep(RETRY_DELAY)
        except Exception:
            raise

if __name__ == "__main__":
    print("=== MongoDB Operations Module Test ===\n")
    
    print("✅ MongoDB Operations Module loaded successfully with GLOBAL RETRY PATCH + DYNAMIC STAGE COOLDOWNS")
    print(f"📁 Config loaded from: {MONGO_CONFIG}")
    print(f"🏠 Main DB: {MONGO_CONFIG['databases']['main_db']['name']}")
    print(f"🔑 API DB: {MONGO_CONFIG['databases']['api_db']['name']}")
    print(f"🔄 Retry delay: {RETRY_DELAY} seconds")
    
    print(f"\n⏱️  Stage Cooldowns:")
    for stage, config in SCRIPT_CONFIG["stage_timings"].items():
        cooldown = config["cooldown_minutes"]
        provider = config["api_provider"]
        print(f"   📊 {stage}: {cooldown} minutes ({provider} keys)")
    
    print("\n📋 Available Functions (ALL with automatic retries + stage-aware cooldowns):")
    functions = [
        "get_domain_for_analysis",
        "get_api_key_and_proxy (now stage-aware)", 
        "finalize_api_key_usage",
        "revert_domain_status",
        "set_domain_error_status",
        "get_domain_segmentation_info",
        "save_contact_information", 
        "save_gemini_results",
        "save_gemini_results_with_validation_failed (🆕 UPDATED with simplified logging)",
        "update_api_key_ip",
        "retry_mongo_operation (fallback)"
    ]
    
    for func in functions:
        print(f"   ✓ {func}")