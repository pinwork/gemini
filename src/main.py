#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import aiohttp
import asyncio
import ssl
import certifi
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple, Optional, List, Union
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from aiohttp_socks import ProxyConnector, ProxyConnectionError, ProxyTimeoutError, ProxyError
from aiohttp import (
    ClientError, ClientConnectionError, ClientOSError, ClientConnectorError,
    ClientConnectorDNSError, ClientSSLError, ClientConnectorSSLError, 
    ClientConnectorCertificateError, ServerConnectionError, ServerDisconnectedError,
    ServerTimeoutError, ConnectionTimeoutError, SocketTimeoutError,
    ClientResponseError, ClientPayloadError, ClientConnectionResetError
)
import logging
import logging.handlers
import re
import random
from urllib.parse import urlparse, urlunparse
import ipaddress
import phonenumbers

# Імпорти наших модулів
from prompts.stage1_prompt_generator import generate_stage1_prompt
from prompts.stage2_system_prompt_generator import generate_system_prompt
from utils.proxy_config import ProxyConfig
from utils.gemini_client import GeminiClient, create_gemini_client
from utils.mongo_operations import (
    get_domain_for_analysis, get_api_key_and_proxy, finalize_api_key_usage,
    revert_domain_status, set_domain_error_status, get_domain_segmentation_info,
    save_contact_information, save_gemini_results, update_api_key_ip, needs_ip_refresh
)
from utils.validation_utils import (
    has_access_issues, validate_country_code, validate_email, validate_phone_e164,
    validate_segments_language, clean_gemini_results, normalize_url, validate_url_field,
    format_summary, clean_it_prefix, validate_segments_full, clean_phone_for_validation
)
from utils.logging_config import (
    setup_all_loggers, log_success_timing, log_rate_limit, log_http_error,
    log_stage1_issue, log_error_details, log_proxy_error
)
from utils.network_error_classifier import (
    ErrorType, ErrorDetails, classify_exception, is_proxy_error
)

CONFIG_DIR = Path("config")
LOG_DIR = Path("logs")
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo_config.json"
STAGE2_SCHEMA_PATH = CONFIG_DIR / "stage2_schema.json"

def load_mongo_config() -> dict:
    try:
        with MONGO_CONFIG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"MongoDB configuration file not found at {MONGO_CONFIG_PATH}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in MongoDB configuration file at {MONGO_CONFIG_PATH}")

def load_stage2_schema() -> dict:
    try:
        with STAGE2_SCHEMA_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Stage2 schema file not found at {STAGE2_SCHEMA_PATH}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in Stage2 schema file at {STAGE2_SCHEMA_PATH}")

MONGO_CONFIG = load_mongo_config()
STAGE2_SCHEMA = load_stage2_schema()
API_DB_URI = MONGO_CONFIG["databases"]["main_db"]["uri"]
CLIENT_PARAMS = MONGO_CONFIG["client_params"]

CONCURRENT_WORKERS = 40
WORKER_STARTUP_DELAY_SECONDS = 0

API_KEY_WAIT_TIME = 60
DOMAIN_WAIT_TIME = 60
CONNECT_TIMEOUT = 6
SOCK_CONNECT_TIMEOUT = 6
SOCK_READ_TIMEOUT = 240
TOTAL_TIMEOUT = 250
RATE_LIMIT_FREEZE_MINUTES = 3

CONTROL_FILE_PATH = CONFIG_DIR / "script_control.json"

SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

CONNECTION_ERRORS = (
    aiohttp.ClientOSError,
    aiohttp.ServerDisconnectedError,
    ProxyConnectionError,
    ProxyTimeoutError,
    ProxyError,
    ClientConnectionError,
    ClientConnectorError,
    ServerConnectionError,
    ServerTimeoutError,
    ClientSSLError
)

STAGE1_MODEL = "gemini-2.5-flash"
STAGE2_MODEL = "gemini-2.0-flash"

# Налаштовуємо всі логгери
all_loggers = setup_all_loggers()
logger = all_loggers['system_errors']
success_timing_logger = all_loggers['success_timing']
rate_limits_logger = all_loggers['rate_limits']
http_errors_logger = all_loggers['http_errors']
stage1_issues_logger = all_loggers['stage1_issues']
proxy_errors_logger = all_loggers['proxy_errors']
network_errors_logger = all_loggers['network_errors']
api_errors_logger = all_loggers['api_errors']
payload_errors_logger = all_loggers['payload_errors']
unknown_errors_logger = all_loggers['unknown_errors']
ip_usage_logger = all_loggers['ip_usage']
revert_reasons_logger = all_loggers['revert_reasons']
short_response_debug_logger = all_loggers['short_response_debug']
segmentation_validation_logger = all_loggers['segmentation_validation']

def log_success_timing_wrapper(worker_id: int, stage: str, api_key: str, target_uri: str, response_time: float):
    log_success_timing(worker_id, stage, api_key, target_uri, response_time, success_timing_logger)

def log_rate_limit_wrapper(worker_id: int, stage: str, api_key: str, target_uri: str, freeze_minutes: int):
    log_rate_limit(worker_id, stage, api_key, target_uri, freeze_minutes, rate_limits_logger)

def log_http_error_wrapper(worker_id: int, stage: str, api_key: str, target_uri: str, status_code: int, error_msg: str):
    log_http_error(worker_id, stage, api_key, target_uri, status_code, error_msg, http_errors_logger)

def log_stage1_issue_wrapper(worker_id: int, api_key: str, target_uri: str, issue_type: str, details: str = ""):
    log_stage1_issue(worker_id, api_key, target_uri, issue_type, stage1_issues_logger, details)

def log_error_details_wrapper(worker_id: int, stage: str, api_key: str, target_uri: str, 
                     error_details: ErrorDetails, response_time: float = 0.0):
    log_error_details(worker_id, stage, api_key, target_uri, error_details, response_time,
                     proxy_errors_logger, network_errors_logger, api_errors_logger, 
                     payload_errors_logger, unknown_errors_logger)

def log_proxy_error_wrapper(worker_id: int, stage: str, proxy_config, target_uri: str, error_msg: str):
    log_proxy_error(worker_id, stage, proxy_config, target_uri, error_msg, proxy_errors_logger)

def get_key_suffix(api_key: str) -> str:
    return f"...{api_key[-4:]}" if len(api_key) >= 4 else "***"

def ensure_control_file():
    try:
        CONTROL_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with CONTROL_FILE_PATH.open("w", encoding="utf-8") as f:
            json.dump({"enabled": True}, f, indent=2)
    except Exception as e:
        logger.error(f"Error setting up control file: {e}")

def is_script_enabled() -> bool:
    try:
        if not CONTROL_FILE_PATH.exists():
            ensure_control_file()
            return True
        with CONTROL_FILE_PATH.open("r", encoding="utf-8") as f:
            control_data = json.load(f)
            return control_data.get("enabled", True)
    except Exception as e:
        logger.error(f"Error reading control file: {e}")
        return True

ensure_control_file()

def clear_logs():
    try:
        if LOG_DIR.exists():
            for log_file in LOG_DIR.glob("*.log*"):
                try:
                    log_file.unlink()
                except Exception:
                    pass
    except Exception:
        pass

clear_logs()

async def get_current_ip_with_retry(proxy_config: ProxyConfig, mongo_client: AsyncIOMotorClient, key_id: str, max_attempts: int = 4) -> Tuple[ProxyConfig, str]:
    """
    Отримує поточну IP адресу через проксі з ретраями
    Тепер використовує спрощений підхід без safe_session_request
    """
    current_proxy = proxy_config
    
    for attempt in range(max_attempts):
        try:
            # Налаштовуємо проксі connector
            connector_params = current_proxy.get_connection_params()
            connector_params.update({
                'ssl': SSL_CONTEXT,
                'rdns': True
            })
            connector = ProxyConnector(**connector_params)
            
            timeout = aiohttp.ClientTimeout(
                total=20,
                connect=CONNECT_TIMEOUT,
                sock_connect=SOCK_CONNECT_TIMEOUT,
                sock_read=15
            )
            
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get("https://icanhazip.com/", timeout=timeout) as response:
                    if response.status != 200:
                        raise RuntimeError(f"Bad status {response.status}")
                    
                    ip = (await response.text()).strip()
                    if not ip:
                        raise RuntimeError("Empty IP response")
                    
                    # Використовуємо функцію з mongo_operations модуля
                    if await update_api_key_ip(mongo_client, key_id, ip, ip_usage_logger):
                        return current_proxy, ip
                    else:
                        # Duplicate IP, try new session
                        current_proxy = current_proxy.generate_new_sessid()
                        continue
                        
        except Exception:
            if attempt < max_attempts - 1:
                current_proxy = current_proxy.generate_new_sessid()
                continue
            
    return current_proxy, ""

async def handle_stage_result(mongo_client, worker_id, stage_name, api_key, target_uri, proxy_config, key_record_id, result):
    """Обробляє результат виконання етапу аналізу"""
    status_code = result.get("status_code")
    response_time = result.get("response_time", 0)
    
    if status_code == 200:
        log_success_timing_wrapper(worker_id, stage_name, api_key, target_uri, response_time)
    elif status_code == 429:
        freeze_minutes = 3
        log_rate_limit_wrapper(worker_id, stage_name, api_key, target_uri, freeze_minutes)
    elif status_code is not None:
        error_details = classify_exception(None, status_code)
        log_error_details_wrapper(worker_id, stage_name, api_key, target_uri, error_details, response_time)
    else:
        exception = result.get("exception")
        if exception:
            error_details = result.get("error_details") or classify_exception(exception)
            log_error_details_wrapper(worker_id, stage_name, api_key, target_uri, error_details, response_time)
        else:
            unknown_errors_logger.info(f"Worker-{worker_id:02d} | {stage_name:6s} | UNKNOWN | No exception or status code | {target_uri}")
    
    api_key_consumed = True
    is_proxy_err = False
    
    if not result.get("success"):
        error_details = result.get("error_details")
        if error_details:
            api_key_consumed = error_details.api_key_consumed
            is_proxy_err = error_details.error_type == ErrorType.PROXY
        else:
            exception = result.get("exception")
            if exception:
                is_proxy_err = is_proxy_error(exception)
                api_key_consumed = not is_proxy_err
    
    freeze_minutes_param = None
    await finalize_api_key_usage(mongo_client, key_record_id, status_code, is_proxy_err, proxy_config, freeze_minutes_param)

async def worker(worker_id: int):
    """Основна функція worker'а з використанням GeminiClient"""
    mongo_client = AsyncIOMotorClient(API_DB_URI, **CLIENT_PARAMS)
    
    # 🆕 Створюємо Gemini клієнт
    gemini_client = create_gemini_client(STAGE2_SCHEMA)
    
    try:
        while True:
            if not is_script_enabled():
                break
                
            try:
                target_uri, domain_full, domain_id = await get_domain_for_analysis(mongo_client)
                
                # Get segmentation info for the domain
                segment_combined = await get_domain_segmentation_info(mongo_client, domain_full)
                
                api_key1, proxy_config1, key_record_id1, key_rec1 = await get_api_key_and_proxy(mongo_client)
                if needs_ip_refresh(key_rec1):
                    working_proxy1, detected_ip1 = await get_current_ip_with_retry(
                        proxy_config1, 
                        mongo_client, 
                        key_record_id1
                    )
                else:
                    working_proxy1 = proxy_config1
                    detected_ip1 = key_rec1["current_ip"]
                
                if not detected_ip1:
                    await finalize_api_key_usage(mongo_client, key_record_id1, None, True, working_proxy1)
                    await revert_domain_status(mongo_client, domain_id, "proxy_ip_refresh_failed", revert_reasons_logger)
                    continue
                
                try:
                    # 🆕 Використовуємо GeminiClient для Stage1 з Google Search
                    # Щоб відключити Google Search: use_google_search=False
                    stage1_prompt = generate_stage1_prompt()
                    stage1_result = await gemini_client.analyze_content(
                        target_uri, api_key1, working_proxy1, stage1_prompt, use_google_search=True
                    )
                    await handle_stage_result(mongo_client, worker_id, "Stage1", api_key1, target_uri, working_proxy1, key_record_id1, stage1_result)
                    
                    if not stage1_result["success"] or stage1_result.get("status_code") != 200:
                        await revert_domain_status(mongo_client, domain_id, "stage1_request_failed", revert_reasons_logger)
                        continue
                    
                    grounding_status = stage1_result.get("grounding_status", "UNKNOWN")
                    text_response = stage1_result.get("text_response", "")
                    
                    if grounding_status == "NO_CANDIDATES":
                        log_stage1_issue_wrapper(worker_id, api_key1, target_uri, "NO_CANDIDATES", "")
                        await revert_domain_status(mongo_client, domain_id, "no_candidates", revert_reasons_logger)
                        continue
                    
                    if len(text_response.strip()) < 200:
                        # Check content of short response
                        response_lower = text_response.lower()
                        if "inaccessible" in response_lower:
                            log_stage1_issue_wrapper(worker_id, api_key1, target_uri, "WEBSITE_INACCESSIBLE", "Short response with inaccessible")
                            await set_domain_error_status(mongo_client, domain_id, "inaccessible")
                            continue
                        elif "placeholder" in response_lower:
                            log_stage1_issue_wrapper(worker_id, api_key1, target_uri, "PLACEHOLDER_PAGE", "Short response with placeholder")
                            await set_domain_error_status(mongo_client, domain_id, "placeholder")
                            continue
                        else:
                            log_stage1_issue_wrapper(worker_id, api_key1, target_uri, "SHORT_RESPONSE", f"{len(text_response)} chars")
                            # Log full response content for debugging
                            short_response_debug_logger.info(f"Domain: {domain_full} | Length: {len(text_response)} | Content: {text_response}")
                            await revert_domain_status(mongo_client, domain_id, "short_response", revert_reasons_logger)
                            continue
                    
                    if grounding_status == "URL_RETRIEVAL_STATUS_ERROR":
                        log_stage1_issue_wrapper(worker_id, api_key1, target_uri, "URL_RETRIEVAL_ERROR", "")
                        await revert_domain_status(mongo_client, domain_id, "url_retrieval_error", revert_reasons_logger)
                        continue
                    
                    if grounding_status == "NON_JSON_RESPONSE":
                        log_stage1_issue_wrapper(worker_id, api_key1, target_uri, "NON_JSON_RESPONSE", "API returned HTML")
                        await revert_domain_status(mongo_client, domain_id, "non_json_response", revert_reasons_logger)
                        continue
                    
                except Exception as stage1_exception:
                    error_details = classify_exception(stage1_exception)
                    log_error_details_wrapper(worker_id, "Stage1", api_key1, target_uri, error_details)
                    
                    is_proxy_err = error_details.error_type == ErrorType.PROXY
                    await finalize_api_key_usage(mongo_client, key_record_id1, None, is_proxy_err, working_proxy1)
                    logger.error(f"Worker {worker_id}: Stage1 {error_details.exception_class} with {working_proxy1.connection_string}: {stage1_exception}")
                    await revert_domain_status(mongo_client, domain_id, f"stage1_exception:{error_details.exception_class}", revert_reasons_logger)
                    continue
                
                api_key2, proxy_config2, key_record_id2, key_rec2 = await get_api_key_and_proxy(mongo_client)
                if needs_ip_refresh(key_rec2):
                    working_proxy2, detected_ip2 = await get_current_ip_with_retry(
                        proxy_config2, 
                        mongo_client, 
                        key_record_id2
                    )
                else:
                    working_proxy2 = proxy_config2
                    detected_ip2 = key_rec2["current_ip"]
                
                if not detected_ip2:
                    await finalize_api_key_usage(mongo_client, key_record_id2, None, True, working_proxy2)
                    await revert_domain_status(mongo_client, domain_id, "proxy_ip_refresh_failed", revert_reasons_logger)
                    continue
                
                try:
                    # 🆕 Використовуємо GeminiClient для Stage2
                    current_system_prompt = generate_system_prompt(segment_combined, domain_full)
                    stage2_result = await gemini_client.analyze_business(
                        target_uri, text_response, api_key2, working_proxy2, current_system_prompt
                    )
                    await handle_stage_result(mongo_client, worker_id, "Stage2", api_key2, target_uri, working_proxy2, key_record_id2, stage2_result)
                    
                    if stage2_result.get("success"):
                        result = stage2_result["result"]
                        
                        # Check for access issues in Stage2 results - now using revert logic in save_gemini_results
                        await save_gemini_results(
                            mongo_client, domain_full, target_uri, result, 
                            grounding_status, domain_id, segment_combined, 
                            revert_logger=revert_reasons_logger, 
                            segmentation_logger=segmentation_validation_logger
                        )
                        
                    else:
                        await revert_domain_status(mongo_client, domain_id, "stage2_request_failed", revert_reasons_logger)
                        
                except Exception as stage2_exception:
                    error_details = classify_exception(stage2_exception)
                    log_error_details_wrapper(worker_id, "Stage2", api_key2, target_uri, error_details)
                    
                    is_proxy_err = error_details.error_type == ErrorType.PROXY
                    await finalize_api_key_usage(mongo_client, key_record_id2, None, is_proxy_err, working_proxy2)
                    logger.error(f"Worker {worker_id}: Stage2 {error_details.exception_class} with {working_proxy2.connection_string}: {stage2_exception}")
                    await revert_domain_status(mongo_client, domain_id, f"stage2_exception:{error_details.exception_class}", revert_reasons_logger)
                    continue
                    
            except SystemExit:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id}: Unexpected error: {e}", exc_info=True)
                await asyncio.sleep(5)
                
    finally:
        mongo_client.close()

async def main():
    workers = []
    try:
        print(f"🚀 Starting {CONCURRENT_WORKERS} workers...")
        print(f"🧪 Model configuration: Stage1={STAGE1_MODEL} | Stage2={STAGE2_MODEL}")
        print(f"🔧 Using GeminiClient with URL Context + Google Search")
        
        workers = [
            asyncio.create_task(worker(worker_id))
            for worker_id in range(CONCURRENT_WORKERS)
        ]
        
        await asyncio.gather(*workers, return_exceptions=True)
        
    except KeyboardInterrupt:
        logger.warning("Received KeyboardInterrupt, shutting down...")
        
        for task in workers:
            if not task.done():
                task.cancel()
        
        await asyncio.gather(*workers, return_exceptions=True)
        
    except Exception as e:
        logger.error(f"Main function error: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())