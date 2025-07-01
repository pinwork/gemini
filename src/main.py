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

from config import ConfigManager, get_next_stage_model, get_stage_retry_model
from prompts.stage1_prompt_generator import generate_stage1_prompt_default, generate_stage1_prompt_short_response_retry
from prompts.stage2_system_prompt_generator import generate_system_prompt
from utils.proxy_config import ProxyConfig
from utils.gemini_client import GeminiClient, create_gemini_client
from utils.mongo_operations import (
    get_domain_for_analysis, finalize_api_key_usage, get_api_key_and_proxy,
    revert_domain_status, set_domain_error_status, get_domain_segmentation_info,
    save_contact_information, save_gemini_results, save_gemini_results_with_validation_failed,
    update_api_key_ip, needs_ip_refresh, increment_short_response_attempts,
    get_short_response_attempts, revert_domain_status_with_short_response_tracking,
    reset_short_response_attempts
)
from utils.validation_utils import (
    has_access_issues, validate_country_code, validate_email, validate_phone_e164,
    validate_segments_language, clean_gemini_results, normalize_url, validate_url_field,
    format_summary, clean_it_prefix, validate_segments_full, clean_phone_for_validation,
    validate_segments_full_only
)
from utils.logging_config import (
    setup_all_loggers, log_success_timing, log_rate_limit, log_http_error,
    log_stage1_issue, log_error_details, log_proxy_error, log_short_response_with_retry_info,
    log_stage1_request_failed_with_reason, log_short_response_max_attempts,
    log_global_limit_rollback
)
from utils.network_error_classifier import (
    ErrorType, ErrorDetails, classify_exception, is_proxy_error
)
from utils.adaptive_delay_manager import AdaptiveDelayManager

LOG_DIR = Path("logs")
MAX_STAGE2_RETRIES = 5

shutdown_event = asyncio.Event()

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
segmentation_validation_logger = all_loggers['segmentation_validation']
adaptive_delay_logger = all_loggers['adaptive_delay']
missing_segmentation_logger = all_loggers['missing_segmentation']

def log_success_timing_wrapper(worker_id: int, stage: str, api_key: str, domain_full: str, response_time: float):
    log_success_timing(worker_id, stage, api_key, domain_full, response_time, success_timing_logger)

def log_rate_limit_wrapper(worker_id: int, stage: str, api_key: str, domain_full: str, freeze_minutes: int, limit_type: str = "UNKNOWN"):
    """Enhanced rate limit logging with 429 classification"""
    log_rate_limit(worker_id, stage, api_key, domain_full, freeze_minutes, limit_type, rate_limits_logger)

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
    def signal_handler(signum, frame):
        print(f"\n🛑 Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        signal.signal(signal.SIGUSR1, signal_handler)
    except AttributeError:
        pass

async def check_shutdown_periodically():
    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(30)
            if not ConfigManager.is_script_enabled():
                print("🛑 Script disabled in config, initiating graceful shutdown...")
                shutdown_event.set()
                break
        except Exception as e:
            logger.error(f"Error checking script status: {e}")
            await asyncio.sleep(5)

async def periodic_adaptive_delay_evaluation(mongo_client: AsyncIOMotorClient):
    await asyncio.sleep(10)
    
    while not shutdown_event.is_set():
        try:
            config = ConfigManager.get_script_config()
            adaptive_config = config.get("adaptive_delay", {})
            
            if not adaptive_config.get("enabled", False):
                await asyncio.sleep(3600)
                continue
            
            evaluation_interval_hours = adaptive_config.get("evaluation_interval_hours", 6)
            sleep_seconds = evaluation_interval_hours * 3600
            
            await asyncio.sleep(sleep_seconds)
            
            if shutdown_event.is_set():
                break
            
            await AdaptiveDelayManager.evaluate_and_adjust(mongo_client, adaptive_delay_logger)
            
        except Exception as e:
            logger.error(f"Error in adaptive delay evaluation: {e}")
            await asyncio.sleep(300)

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

async def handle_stage_result(mongo_client, worker_id, stage_name, api_key, domain_full, proxy_config, key_record_id, result):
    status_code = result.get("status_code")
    response_time = result.get("response_time", 0)
    limit_type = result.get("limit_type", "UNKNOWN")
    
    if status_code == 200:
        log_success_timing_wrapper(worker_id, stage_name, api_key, domain_full, response_time)
    elif status_code == 429:
        if limit_type == "GLOBAL_LIMIT":
            log_global_limit_rollback(worker_id, stage_name, api_key, domain_full, 6, rate_limits_logger)
        else:
            # ВИПРАВЛЕННЯ: Використовуємо реальний cooldown з конфігурації
            cooldown_minutes = ConfigManager.get_stage_cooldown(stage_name.lower())  # stage1 або stage2
            log_rate_limit_wrapper(worker_id, stage_name, api_key, domain_full, cooldown_minutes, limit_type)
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
    await finalize_api_key_usage(mongo_client, key_record_id, status_code, is_proxy_err, proxy_config, freeze_minutes_param, limit_type)

async def worker(worker_id: int, shared_mongo_client: AsyncIOMotorClient):
    gemini_client = create_gemini_client(
        stage2_schema=STAGE2_SCHEMA,
        start_delay_ms=START_DELAY_MS,
        stage2_retry_model=""
    )
    
    try:
        while not shutdown_event.is_set():
            try:
                target_uri, domain_full, domain_id = await get_domain_for_analysis(shared_mongo_client)
                
                segment_combined = await get_domain_segmentation_info(shared_mongo_client, domain_full, missing_segmentation_logger)
                
                current_short_attempts = await get_short_response_attempts(shared_mongo_client, domain_id)
                
                api_key1, proxy_config1, key_record_id1, key_rec1 = await get_api_key_and_proxy(shared_mongo_client, "stage1")
                if needs_ip_refresh(key_rec1):
                    working_proxy1, detected_ip1 = await get_current_ip_with_retry(
                        proxy_config1, 
                        shared_mongo_client, 
                        key_record_id1
                    )
                else:
                    working_proxy1 = proxy_config1
                    detected_ip1 = key_rec1["current_ip"]
                
                if not detected_ip1:
                    await finalize_api_key_usage(shared_mongo_client, key_record_id1, None, True, working_proxy1, None, "UNKNOWN")
                    await revert_domain_status(shared_mongo_client, domain_id, "proxy_ip_refresh_failed", revert_reasons_logger)
                    continue
                
                try:
                    if current_short_attempts > 0:
                        stage1_prompt = generate_stage1_prompt_short_response_retry(current_short_attempts + 1)
                    else:
                        stage1_prompt = generate_stage1_prompt_default()
                    
                    current_stage1_model = get_next_stage_model("stage1")
                    stage1_result = await gemini_client.analyze_content(
                        domain_full, api_key1, working_proxy1, stage1_prompt, 
                        use_google_search=True, model_override=current_stage1_model
                    )
                    await handle_stage_result(shared_mongo_client, worker_id, "Stage1", api_key1, domain_full, working_proxy1, key_record_id1, stage1_result)
                    
                    if not stage1_result["success"] or stage1_result.get("status_code") != 200:
                        failure_reason = "unknown_error"
                        
                        if stage1_result.get("status_code"):
                            if stage1_result["status_code"] == 429:
                                failure_reason = "HTTP_429_rate_limit"
                            elif stage1_result["status_code"] == 401:
                                failure_reason = "HTTP_401_unauthorized"
                            elif stage1_result["status_code"] == 403:
                                failure_reason = "HTTP_403_forbidden"
                            elif stage1_result["status_code"] >= 500:
                                failure_reason = f"HTTP_{stage1_result['status_code']}_server_error"
                            else:
                                failure_reason = f"HTTP_{stage1_result['status_code']}_client_error"
                        elif stage1_result.get("error_details"):
                            error_details = stage1_result["error_details"]
                            failure_reason = f"{error_details.error_type.value}_{error_details.exception_class.lower()}"
                        elif "proxy" in str(stage1_result.get("error", "")).lower():
                            failure_reason = "proxy_connection_error"
                        elif "timeout" in str(stage1_result.get("error", "")).lower():
                            failure_reason = "request_timeout"
                        
                        log_stage1_request_failed_with_reason(worker_id, api_key1, domain_full, failure_reason, stage1_issues_logger)
                        await revert_domain_status(shared_mongo_client, domain_id, "stage1_request_failed", revert_reasons_logger)
                        continue
                    
                    grounding_status = stage1_result.get("grounding_status", "UNKNOWN")
                    text_response = stage1_result.get("text_response", "")
                    
                    if grounding_status == "NO_CANDIDATES":
                        log_stage1_request_failed_with_reason(worker_id, api_key1, domain_full, "no_candidates_found", stage1_issues_logger)
                        await revert_domain_status(shared_mongo_client, domain_id, "no_candidates", revert_reasons_logger)
                        continue
                    
                    if len(text_response.strip()) < 200:
                        response_lower = text_response.lower()
                        
                        if "inaccessible" in response_lower:
                            await reset_short_response_attempts(shared_mongo_client, domain_id)
                            log_stage1_request_failed_with_reason(worker_id, api_key1, domain_full, "website_inaccessible", stage1_issues_logger)
                            await set_domain_error_status(shared_mongo_client, domain_id, "inaccessible")
                            continue
                        elif "placeholder" in response_lower:
                            await reset_short_response_attempts(shared_mongo_client, domain_id)
                            log_stage1_request_failed_with_reason(worker_id, api_key1, domain_full, "placeholder_page", stage1_issues_logger)
                            await set_domain_error_status(shared_mongo_client, domain_id, "placeholder")
                            continue
                        else:
                            should_continue, attempts_count = await revert_domain_status_with_short_response_tracking(
                                shared_mongo_client, domain_id, "short_response", revert_reasons_logger
                            )
                            
                            if should_continue:
                                log_short_response_with_retry_info(
                                    worker_id, api_key1, domain_full, 
                                    len(text_response), text_response.strip(), 
                                    attempts_count, stage1_issues_logger
                                )
                                continue
                            else:
                                log_short_response_max_attempts(
                                    worker_id, api_key1, domain_full, 
                                    attempts_count, stage1_issues_logger
                                )
                                continue
                    else:
                        if current_short_attempts > 0:
                            await reset_short_response_attempts(shared_mongo_client, domain_id)
                    
                    if grounding_status == "URL_RETRIEVAL_STATUS_ERROR":
                        log_stage1_request_failed_with_reason(worker_id, api_key1, domain_full, "url_retrieval_error", stage1_issues_logger)
                        await revert_domain_status(shared_mongo_client, domain_id, "url_retrieval_error", revert_reasons_logger)
                        continue
                    
                    if grounding_status == "NON_JSON_RESPONSE":
                        log_stage1_request_failed_with_reason(worker_id, api_key1, domain_full, "non_json_response", stage1_issues_logger)
                        await revert_domain_status(shared_mongo_client, domain_id, "non_json_response", revert_reasons_logger)
                        continue
                    
                except Exception as stage1_exception:
                    error_details = classify_exception(stage1_exception)
                    log_error_details_wrapper(worker_id, "Stage1", api_key1, domain_full, error_details)
                    
                    is_proxy_err = error_details.error_type == ErrorType.PROXY
                    await finalize_api_key_usage(shared_mongo_client, key_record_id1, None, is_proxy_err, working_proxy1, None, "UNKNOWN")
                    logger.error(f"Worker {worker_id}: Stage1 {error_details.exception_class} with {working_proxy1.connection_string}: {stage1_exception}")
                    
                    exception_reason = f"exception_{error_details.exception_class.lower()}"
                    log_stage1_request_failed_with_reason(worker_id, api_key1, domain_full, exception_reason, stage1_issues_logger)
                    await revert_domain_status(shared_mongo_client, domain_id, f"stage1_exception:{error_details.exception_class}", revert_reasons_logger)
                    continue
                
                retry_count = 0
                stage2_success = False
                final_stage2_result = None
                last_failed_segments_full = ""
                last_cleaned_segments_full = ""
                
                while retry_count <= MAX_STAGE2_RETRIES and not stage2_success:
                    try:
                        api_key2, proxy_config2, key_record_id2, key_rec2 = await get_api_key_and_proxy(shared_mongo_client, "stage2")
                        if needs_ip_refresh(key_rec2):
                            working_proxy2, detected_ip2 = await get_current_ip_with_retry(
                                proxy_config2, 
                                shared_mongo_client, 
                                key_record_id2
                            )
                        else:
                            working_proxy2 = proxy_config2
                            detected_ip2 = key_rec2["current_ip"]
                        
                        if not detected_ip2:
                            await finalize_api_key_usage(shared_mongo_client, key_record_id2, None, True, working_proxy2, None, "UNKNOWN")
                            retry_count += 1
                            continue
                        
                        use_retry_model = retry_count > 0
                        
                        current_system_prompt = generate_system_prompt(
                            segment_combined, 
                            domain_full, 
                            failed_segments_full=last_failed_segments_full if retry_count > 0 else ""
                        )
                        
                        current_stage2_model = get_next_stage_model("stage2")
                        current_retry_model = get_stage_retry_model("stage2")
                        
                        stage2_result = await gemini_client.analyze_business(
                            domain_full, text_response, api_key2, working_proxy2, current_system_prompt, 
                            use_retry_model=use_retry_model, 
                            model_override=current_stage2_model,
                            retry_model_override=current_retry_model
                        )
                        await handle_stage_result(shared_mongo_client, worker_id, "Stage2", api_key2, domain_full, working_proxy2, key_record_id2, stage2_result)
                        
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
                        await finalize_api_key_usage(shared_mongo_client, key_record_id2, None, is_proxy_err, working_proxy2, None, "UNKNOWN")
                        
                        retry_count += 1
                
                if stage2_success and final_stage2_result:
                    await save_gemini_results(
                        shared_mongo_client, domain_full, final_stage2_result, 
                        grounding_status, domain_id, segment_combined, 
                        revert_logger=revert_reasons_logger, 
                        segmentation_logger=segmentation_validation_logger
                    )
                else:
                    if final_stage2_result is None and 'stage2_result' in locals():
                        final_stage2_result = stage2_result.get("result", {}) if stage2_result else {}
                    
                    await save_gemini_results_with_validation_failed(
                        mongo_client=shared_mongo_client,
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
                
    except Exception as e:
        logger.error(f"Worker {worker_id}: Fatal error: {e}", exc_info=True)

async def main():
    setup_signal_handlers()
    
    shared_mongo_client = None
    workers = []
    config_checker = None
    adaptive_task = None
    
    try:
        shared_mongo_client = AsyncIOMotorClient(API_DB_URI, **CLIENT_PARAMS)
        
        reset_count = await AdaptiveDelayManager.startup_reset(shared_mongo_client, adaptive_delay_logger)
        
        config_summary = ConfigManager.get_config_summary()
        
        current_workers = config_summary["concurrent_workers"]
        stage1_models = ConfigManager.get_stage_models("stage1")
        stage2_models = ConfigManager.get_stage_models("stage2")
        stage2_retry_model = ConfigManager.get_stage_retry_model_single("stage2")
        stage1_cooldown = config_summary["stage1_cooldown"]
        stage2_cooldown = config_summary["stage2_cooldown"]
        max_concurrent_starts = ConfigManager.get_max_concurrent_starts()
        
        adaptive_config = ConfigManager.get_script_config().get("adaptive_delay", {})
        adaptive_enabled = adaptive_config.get("enabled", False)
        current_delay = adaptive_config.get("current_delay_ms", 700)
        evaluation_interval = adaptive_config.get("evaluation_interval_hours", 6)
        step_ms = adaptive_config.get("step_ms", 20)
        
        print(f"🧹 Clean slate: Reset counters for {reset_count} Gemini API keys")
        print(f"🚀 Starting {current_workers} workers...")
        print(f"🧪 NEW: Model rotation configuration:")
        print(f"   Stage1 models: {stage1_models} ({stage1_cooldown}min)")
        print(f"   Stage2 models: {stage2_models} ({stage2_cooldown}min)")
        print(f"   Stage2 retry: {stage2_retry_model} (no rotation)")
        
        if adaptive_enabled:
            print(f"⏱️  Adaptive delay: {current_delay}ms (step: -{step_ms}ms every {evaluation_interval}h)")
            print(f"🎯 Adaptive range: {adaptive_config.get('min_delay_ms', 0)}ms - {adaptive_config.get('max_delay_ms', 700)}ms")
        else:
            print(f"⏱️  Fixed delay: {current_delay}ms (adaptive system disabled)")
        
        print(f"🔄 NEW: Round-robin model rotation reduces 429 errors by ~66%")
        print(f"🎯 NEW: Enhanced 429 error classification (PERSONAL_QUOTA/GLOBAL_LIMIT)")
        print(f"🛡️  NEW: GLOBAL_LIMIT rollback prevents unfair key penalization")
        
        config_checker = asyncio.create_task(check_shutdown_periodically())
        
        if adaptive_enabled:
            adaptive_task = asyncio.create_task(periodic_adaptive_delay_evaluation(shared_mongo_client))
        
        workers = [
            asyncio.create_task(worker(worker_id, shared_mongo_client))
            for worker_id in range(current_workers)
        ]
        
        tasks_to_run = [*workers, config_checker]
        if adaptive_task:
            tasks_to_run.append(adaptive_task)
        
        await asyncio.gather(*tasks_to_run, return_exceptions=True)
        
    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt received, initiating graceful shutdown...")
        shutdown_event.set()
        
    except Exception as e:
        logger.error(f"Main function error: {e}", exc_info=True)
        shutdown_event.set()
        
    finally:
        print("🔄 Shutting down workers gracefully...")
        
        if config_checker and not config_checker.done():
            config_checker.cancel()
            
        if adaptive_task and not adaptive_task.done():
            adaptive_task.cancel()
            
        for task in workers:
            if not task.done():
                task.cancel()
        
        tasks_to_wait = [*workers]
        if config_checker:
            tasks_to_wait.append(config_checker)
        if adaptive_task:
            tasks_to_wait.append(adaptive_task)
        
        if tasks_to_wait:
            await asyncio.gather(*tasks_to_wait, return_exceptions=True)
        
        if shared_mongo_client:
            print("🗃️  Closing shared MongoDB client...")
            shared_mongo_client.close()
            
        print("✅ All workers stopped gracefully")

if __name__ == "__main__":
    asyncio.run(main())