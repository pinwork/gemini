#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import aiohttp
import asyncio
import ssl
import certifi
import time
import signal
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

from config import ConfigManager
from prompts.stage1_prompt_generator import generate_stage1_prompt
from prompts.stage2_system_prompt_generator import generate_system_prompt
from utils.proxy_config import ProxyConfig
from utils.gemini_client import GeminiClient, create_gemini_client
from utils.mongo_operations import (
    get_domain_for_analysis, finalize_api_key_usage, get_api_key_and_proxy,
    revert_domain_status, set_domain_error_status, get_domain_segmentation_info,
    save_contact_information, save_gemini_results, save_gemini_results_with_validation_failed,
    update_api_key_ip, needs_ip_refresh
)
from utils.validation_utils import (
    has_access_issues, validate_country_code, validate_email, validate_phone_e164,
    validate_segments_language, clean_gemini_results, normalize_url, validate_url_field,
    format_summary, clean_it_prefix, validate_segments_full, clean_phone_for_validation,
    validate_segments_full_only
)
from utils.logging_config import (
    setup_all_loggers, log_success_timing, log_rate_limit, log_http_error,
    log_stage1_issue, log_error_details, log_proxy_error
)
from utils.network_error_classifier import (
    ErrorType, ErrorDetails, classify_exception, is_proxy_error
)

# ==================== ГЛОБАЛЬНІ ЗМІННІ ====================

LOG_DIR = Path("logs")
MAX_STAGE2_RETRIES = 5

# Глобальний event для graceful shutdown
shutdown_event = asyncio.Event()

# Отримуємо всі конфігурації через ConfigManager
MONGO_CONFIG = ConfigManager.get_mongo_config()
SCRIPT_CONFIG = ConfigManager.get_script_config()
STAGE2_SCHEMA = ConfigManager.get_stage2_schema()

API_DB_URI = MONGO_CONFIG["databases"]["main_db"]["uri"]
CLIENT_PARAMS = MONGO_CONFIG["client_params"]

CONCURRENT_WORKERS = SCRIPT_CONFIG["workers"]["concurrent_workers"]
START_DELAY_MS = SCRIPT_CONFIG["timing"]["start_delay_ms"]
API_KEY_WAIT_TIME = SCRIPT_CONFIG["timing"]["api_key_wait_time"]
DOMAIN_WAIT_TIME = SCRIPT_CONFIG["timing"]["domain_wait_time"]
MAX_CONCURRENT_STARTS = ConfigManager.get_max_concurrent_starts()
STAGE1_MODEL = SCRIPT_CONFIG["stage_timings"]["stage1"]["model"]
STAGE2_MODEL = SCRIPT_CONFIG["stage_timings"]["stage2"]["model"]
STAGE2_RETRY_MODEL = SCRIPT_CONFIG["stage_timings"]["stage2"].get("retry_model")

WORKER_STARTUP_DELAY_SECONDS = 0
CONNECT_TIMEOUT = 6
SOCK_CONNECT_TIMEOUT = 6
SOCK_READ_TIMEOUT = 240
TOTAL_TIMEOUT = 250
RATE_LIMIT_FREEZE_MINUTES = 3

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

# ==================== ЛОГУВАННЯ ====================

all_loggers = setup_all_loggers()
logger = all_loggers['system_errors']
success_timing_logger = all_loggers['success_timing']
rate_limits_logger = all_loggers['rate_limits']
http_errors_logger = all_loggers['http_errors']
stage1_issues_logger = all_loggers['stage1_issues']
stage2_retries_logger = all_loggers['stage2_retries']
proxy_errors_logger = all_loggers['proxy_errors']
network_errors_logger = all_loggers['network_errors']
api_errors_logger = all_loggers['api_errors']
payload_errors_logger = all_loggers['payload_errors']
unknown_errors_logger = all_loggers['unknown_errors']
ip_usage_logger = all_loggers['ip_usage']
revert_reasons_logger = all_loggers['revert_reasons']
short_response_debug_logger = all_loggers['short_response_debug']
segmentation_validation_logger = all_loggers['segmentation_validation']

# ==================== ДОПОМІЖНІ ФУНКЦІЇ ====================

def log_success_timing_wrapper(worker_id: int, stage: str, api_key: str, domain_full: str, response_time: float):
    log_success_timing(worker_id, stage, api_key, domain_full, response_time, success_timing_logger)

def log_rate_limit_wrapper(worker_id: int, stage: str, api_key: str, domain_full: str, freeze_minutes: int):
    log_rate_limit(worker_id, stage, api_key, domain_full, freeze_minutes, rate_limits_logger)

def log_http_error_wrapper(worker_id: int, stage: str, api_key: str, domain_full: str, status_code: int, error_msg: str):
    log_http_error(worker_id, stage, api_key, domain_full, status_code, error_msg, http_errors_logger)

def log_stage1_issue_wrapper(worker_id: int, api_key: str, domain_full: str, issue_type: str, details: str = ""):
    log_stage1_issue(worker_id, api_key, domain_full, issue_type, stage1_issues_logger, details)

def log_error_details_wrapper(worker_id: int, stage: str, api_key: str, domain_full: str, 
                     error_details: ErrorDetails, response_time: float = 0.0):
    log_error_details(worker_id, stage, api_key, domain_full, error_details, response_time,
                     proxy_errors_logger, network_errors_logger, api_errors_logger, 
                     payload_errors_logger, unknown_errors_logger)

def log_proxy_error_wrapper(worker_id: int, stage: str, proxy_config, domain_full: str, error_msg: str):
    log_proxy_error(worker_id, stage, proxy_config, domain_full, error_msg, proxy_errors_logger)

def get_key_suffix(api_key: str) -> str:
    return f"...{api_key[-4:]}" if len(api_key) >= 4 else "***"

def setup_signal_handlers():
    """Налаштовує обробники сигналів для graceful shutdown"""
    def signal_handler(signum, frame):
        print(f"\n🛑 Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()
    
    # Реєструємо обробники для різних сигналів
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Термінація
    
    # SIGUSR1 для graceful restart (тільки на Unix)
    try:
        signal.signal(signal.SIGUSR1, signal_handler)
    except AttributeError:
        pass  # Windows не підтримує SIGUSR1

async def check_shutdown_periodically():
    """Періодично перевіряє конфігурацію і встановлює shutdown_event якщо треба"""
    while not shutdown_event.is_set():
        try:
            # Перевіряємо enabled статус кожні 30 секунд
            await asyncio.sleep(30)
            if not ConfigManager.is_script_enabled():
                print("🛑 Script disabled in config, initiating graceful shutdown...")
                shutdown_event.set()
                break
        except Exception as e:
            logger.error(f"Error checking script status: {e}")
            await asyncio.sleep(5)

# ==================== IP REFRESH ФУНКЦІЯ ====================

async def get_current_ip_with_retry(proxy_config: ProxyConfig, mongo_client: AsyncIOMotorClient, key_id: str, max_attempts: int = 4) -> Tuple[ProxyConfig, str]:
    current_proxy = proxy_config
    
    for attempt in range(max_attempts):
        try:
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
                    
                    if await update_api_key_ip(mongo_client, key_id, ip, ip_usage_logger):
                        return current_proxy, ip
                    else:
                        current_proxy = current_proxy.generate_new_sessid()
                        continue
                        
        except Exception:
            if attempt < max_attempts - 1:
                current_proxy = current_proxy.generate_new_sessid()
                continue
            
    return current_proxy, ""

# ==================== ОБРОБКА РЕЗУЛЬТАТІВ ====================

async def handle_stage_result(mongo_client, worker_id, stage_name, api_key, domain_full, proxy_config, key_record_id, result):
    status_code = result.get("status_code")
    response_time = result.get("response_time", 0)
    
    if status_code == 200:
        log_success_timing_wrapper(worker_id, stage_name, api_key, domain_full, response_time)
    elif status_code == 429:
        freeze_minutes = 3
        log_rate_limit_wrapper(worker_id, stage_name, api_key, domain_full, freeze_minutes)
    elif status_code is not None:
        error_details = classify_exception(None, status_code)
        log_error_details_wrapper(worker_id, stage_name, api_key, domain_full, error_details, response_time)
    else:
        exception = result.get("exception")
        if exception:
            error_details = result.get("error_details") or classify_exception(exception)
            log_error_details_wrapper(worker_id, stage_name, api_key, domain_full, error_details, response_time)
        else:
            unknown_errors_logger.info(f"Worker-{worker_id:02d} | {stage_name:6s} | UNKNOWN | No exception or status code | {domain_full}")
    
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

# ==================== ВОРКЕР ====================

async def worker(worker_id: int):
    mongo_client = AsyncIOMotorClient(API_DB_URI, **CLIENT_PARAMS)
    
    gemini_client = create_gemini_client(
        stage2_schema=STAGE2_SCHEMA,
        start_delay_ms=START_DELAY_MS,
        stage2_retry_model=STAGE2_RETRY_MODEL
    )
    
    try:
        while not shutdown_event.is_set():
            try:
                target_uri, domain_full, domain_id = await get_domain_for_analysis(mongo_client)
                
                segment_combined = await get_domain_segmentation_info(mongo_client, domain_full)
                
                api_key1, proxy_config1, key_record_id1, key_rec1 = await get_api_key_and_proxy(mongo_client, "stage1")
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
                    stage1_prompt = generate_stage1_prompt()
                    
                    stage1_result = await gemini_client.analyze_content(
                        domain_full, api_key1, working_proxy1, stage1_prompt, use_google_search=True
                    )
                    await handle_stage_result(mongo_client, worker_id, "Stage1", api_key1, domain_full, working_proxy1, key_record_id1, stage1_result)
                    
                    if not stage1_result["success"] or stage1_result.get("status_code") != 200:
                        await revert_domain_status(mongo_client, domain_id, "stage1_request_failed", revert_reasons_logger)
                        continue
                    
                    grounding_status = stage1_result.get("grounding_status", "UNKNOWN")
                    text_response = stage1_result.get("text_response", "")
                    
                    if grounding_status == "NO_CANDIDATES":
                        log_stage1_issue_wrapper(worker_id, api_key1, domain_full, "NO_CANDIDATES", "")
                        await revert_domain_status(mongo_client, domain_id, "no_candidates", revert_reasons_logger)
                        continue
                    
                    if len(text_response.strip()) < 200:
                        response_lower = text_response.lower()
                        if "inaccessible" in response_lower:
                            log_stage1_issue_wrapper(worker_id, api_key1, domain_full, "WEBSITE_INACCESSIBLE", "Short response with inaccessible")
                            await set_domain_error_status(mongo_client, domain_id, "inaccessible")
                            continue
                        elif "placeholder" in response_lower:
                            log_stage1_issue_wrapper(worker_id, api_key1, domain_full, "PLACEHOLDER_PAGE", "Short response with placeholder")
                            await set_domain_error_status(mongo_client, domain_id, "placeholder")
                            continue
                        else:
                            log_stage1_issue_wrapper(worker_id, api_key1, domain_full, "SHORT_RESPONSE", f"{len(text_response)} chars")
                            short_response_debug_logger.info(f"Domain: {domain_full} | Length: {len(text_response)} | Content: {text_response}")
                            await revert_domain_status(mongo_client, domain_id, "short_response", revert_reasons_logger)
                            continue
                    
                    if grounding_status == "URL_RETRIEVAL_STATUS_ERROR":
                        log_stage1_issue_wrapper(worker_id, api_key1, domain_full, "URL_RETRIEVAL_ERROR", "")
                        await revert_domain_status(mongo_client, domain_id, "url_retrieval_error", revert_reasons_logger)
                        continue
                    
                    if grounding_status == "NON_JSON_RESPONSE":
                        log_stage1_issue_wrapper(worker_id, api_key1, domain_full, "NON_JSON_RESPONSE", "API returned HTML")
                        await revert_domain_status(mongo_client, domain_id, "non_json_response", revert_reasons_logger)
                        continue
                    
                except Exception as stage1_exception:
                    error_details = classify_exception(stage1_exception)
                    log_error_details_wrapper(worker_id, "Stage1", api_key1, domain_full, error_details)
                    
                    is_proxy_err = error_details.error_type == ErrorType.PROXY
                    await finalize_api_key_usage(mongo_client, key_record_id1, None, is_proxy_err, working_proxy1)
                    logger.error(f"Worker {worker_id}: Stage1 {error_details.exception_class} with {working_proxy1.connection_string}: {stage1_exception}")
                    await revert_domain_status(mongo_client, domain_id, f"stage1_exception:{error_details.exception_class}", revert_reasons_logger)
                    continue
                
                retry_count = 0
                stage2_success = False
                final_stage2_result = None
                last_failed_segments_full = ""
                last_cleaned_segments_full = ""
                
                while retry_count <= MAX_STAGE2_RETRIES and not stage2_success:
                    try:
                        api_key2, proxy_config2, key_record_id2, key_rec2 = await get_api_key_and_proxy(mongo_client, "stage2")
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
                            retry_count += 1
                            continue
                        
                        use_retry_model = retry_count > 0 and STAGE2_RETRY_MODEL is not None
                        
                        current_system_prompt = generate_system_prompt(
                            segment_combined, 
                            domain_full, 
                            failed_segments_full=last_failed_segments_full if retry_count > 0 else ""
                        )
                        
                        stage2_result = await gemini_client.analyze_business(
                            domain_full, text_response, api_key2, working_proxy2, current_system_prompt, use_retry_model=use_retry_model
                        )
                        await handle_stage_result(mongo_client, worker_id, "Stage2", api_key2, domain_full, working_proxy2, key_record_id2, stage2_result)
                        
                        if stage2_result.get("success") and stage2_result.get("status_code") == 200:
                            result = stage2_result["result"]
                            
                            cleaned_result = clean_gemini_results(result, segment_combined, domain_full, segmentation_validation_logger)
                            cleaned_segments_full = cleaned_result.get("segments_full", "")
                            is_segments_valid = validate_segments_full_only(segment_combined, cleaned_segments_full, domain_full)
                            
                            if is_segments_valid:
                                stage2_success = True
                                final_stage2_result = cleaned_result
                                break
                            else:
                                original_segments_full = result.get("segments_full", "")
                                last_failed_segments_full = original_segments_full
                                last_cleaned_segments_full = cleaned_segments_full
                                retry_count += 1
                        else:
                            retry_count += 1
                            
                    except Exception as stage2_exception:
                        error_details = classify_exception(stage2_exception)
                        log_error_details_wrapper(worker_id, "Stage2", api_key2, domain_full, error_details)
                        
                        is_proxy_err = error_details.error_type == ErrorType.PROXY
                        await finalize_api_key_usage(mongo_client, key_record_id2, None, is_proxy_err, working_proxy2)
                        
                        retry_count += 1
                
                if stage2_success and final_stage2_result:
                    await save_gemini_results(
                        mongo_client, domain_full, final_stage2_result, 
                        grounding_status, domain_id, segment_combined, 
                        revert_logger=revert_reasons_logger, 
                        segmentation_logger=segmentation_validation_logger
                    )
                else:
                    if final_stage2_result is None and 'stage2_result' in locals():
                        final_stage2_result = stage2_result.get("result", {}) if stage2_result else {}
                    
                    await save_gemini_results_with_validation_failed(
                        mongo_client=mongo_client,
                        domain_full=domain_full,
                        gemini_result=final_stage2_result or {},
                        grounding_status=grounding_status,
                        domain_id=domain_id,
                        segment_combined=segment_combined,
                        retry_count=retry_count - 1,
                        stage2_retries_logger=stage2_retries_logger,
                        last_failed_segments_full=last_failed_segments_full,
                        last_cleaned_segments_full=last_cleaned_segments_full
                    )
                    
            except SystemExit:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id}: Unexpected error: {e}", exc_info=True)
                await asyncio.sleep(5)
                
    finally:
        mongo_client.close()

# ==================== ОСНОВНА ФУНКЦІЯ ====================

async def main():
    # Налаштовуємо signal handlers для graceful shutdown
    setup_signal_handlers()
    
    workers = []
    config_checker = None
    
    try:
        # Отримуємо конфігурацію через ConfigManager
        config_summary = ConfigManager.get_config_summary()
        
        current_workers = config_summary["concurrent_workers"]
        stage1_model = config_summary["stage1_model"]
        stage2_model = config_summary["stage2_model"]
        stage2_retry_model = config_summary.get("stage2_retry_model")
        stage1_cooldown = config_summary["stage1_cooldown"]
        stage2_cooldown = config_summary["stage2_cooldown"]
        max_concurrent_starts = ConfigManager.get_max_concurrent_starts()
        
        print(f"🚀 Starting {current_workers} workers...")
        print(f"🧪 Model configuration: Stage1={stage1_model} ({stage1_cooldown}min) | Stage2={stage2_model} ({stage2_cooldown}min)")
        if stage2_retry_model:
            print(f"🔄 Retry model: {stage2_retry_model} (used for Stage2 retries)")
        print(f"⏱️  Request interval: {START_DELAY_MS}ms between requests")
        
        # Стартуємо задачу для періодичної перевірки конфігурації
        config_checker = asyncio.create_task(check_shutdown_periodically())
        
        # Створюємо воркери
        workers = [
            asyncio.create_task(worker(worker_id))
            for worker_id in range(current_workers)
        ]
        
        # Чекаємо завершення воркерів або shutdown event
        await asyncio.gather(*workers, config_checker, return_exceptions=True)
        
    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt received, initiating graceful shutdown...")
        shutdown_event.set()
        
    except Exception as e:
        logger.error(f"Main function error: {e}", exc_info=True)
        shutdown_event.set()
        
    finally:
        # Graceful shutdown всіх задач
        print("🔄 Shutting down workers gracefully...")
        
        # Скасовуємо config checker
        if config_checker and not config_checker.done():
            config_checker.cancel()
            
        # Скасовуємо всі воркери
        for task in workers:
            if not task.done():
                task.cancel()
        
        # Чекаємо завершення всіх задач
        if workers or config_checker:
            await asyncio.gather(*workers, config_checker, return_exceptions=True)
            
        print("✅ All workers stopped gracefully")

if __name__ == "__main__":
    asyncio.run(main())