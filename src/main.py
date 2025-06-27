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
MAX_CONCURRENT_STARTS = 1
START_DELAY_MS = 700
WORKER_STARTUP_DELAY_SECONDS = 0

API_KEY_WAIT_TIME = 60
DOMAIN_WAIT_TIME = 60
CONNECT_TIMEOUT = 6
SOCK_CONNECT_TIMEOUT = 6
SOCK_READ_TIMEOUT = 240
TOTAL_TIMEOUT = 250
STAGE2_TIMEOUT_SECONDS = 90
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

async def safe_session_request(proxy_config: ProxyConfig, method: str, url: str, stage_name: str = None, **kwargs):
    if stage_name:
        await enforce_request_interval(stage_name)
    
    if 'timeout' not in kwargs:
        kwargs['timeout'] = aiohttp.ClientTimeout(
            total=TOTAL_TIMEOUT,
            connect=CONNECT_TIMEOUT,
            sock_connect=SOCK_CONNECT_TIMEOUT,
            sock_read=SOCK_READ_TIMEOUT
        )
    
    if method.upper() == 'POST' and 'json' in kwargs:
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']["Content-Type"] = "application/json"
    
    # Використовуємо метод get_connection_params() з ProxyConfig
    connector_params = proxy_config.get_connection_params()
    connector_params.update({
        'ssl': SSL_CONTEXT,
        'rdns': True
    })
    
    connector = ProxyConnector(**connector_params)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.request(method, url, **kwargs) as response:
            if method.upper() == 'POST':
                try:
                    resp_json = await response.json()
                    return response, resp_json
                except aiohttp.ContentTypeError:
                    resp_text = await response.text()
                    if len(resp_text) > 512:
                        resp_text = resp_text[:512] + "...[truncated]"
                    return response, resp_text
            else:
                resp_text = await response.text()
                return response, resp_text

_stage_timing = {
    "stage1": {"last_request_time": 0, "semaphore": None},
    "stage2": {"last_request_time": 0, "semaphore": None}
}

async def enforce_request_interval(stage_name: str):
    stage_key = stage_name.lower()
    
    if stage_key not in _stage_timing:
        return
    
    if _stage_timing[stage_key]["semaphore"] is None:
        _stage_timing[stage_key]["semaphore"] = asyncio.Semaphore(MAX_CONCURRENT_STARTS)
    
    async with _stage_timing[stage_key]["semaphore"]:
        current_time = time.time()
        last_time = _stage_timing[stage_key]["last_request_time"]
        time_since_last = current_time - last_time
        
        min_interval = START_DELAY_MS / 1000.0
        sleep_time = max(0, min_interval - time_since_last)
        
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)
        
        _stage_timing[stage_key]["last_request_time"] = time.time()

async def controlled_stage1_request(request_func, *args, **kwargs):
    return await request_func(*args, **kwargs)

async def controlled_stage2_request(request_func, *args, **kwargs):
    return await request_func(*args, **kwargs)

async def get_current_ip_with_retry(proxy_config: ProxyConfig, mongo_client: AsyncIOMotorClient, key_id: str, max_attempts: int = 4) -> Tuple[ProxyConfig, str]:
    current_proxy = proxy_config
    
    for attempt in range(max_attempts):
        try:
            response, text = await safe_session_request(
                current_proxy, 
                "GET", 
                "https://icanhazip.com/",
                None,
                timeout=aiohttp.ClientTimeout(
                    total=20,
                    connect=CONNECT_TIMEOUT,
                    sock_connect=SOCK_CONNECT_TIMEOUT,
                    sock_read=15
                )
            )
            
            if response.status != 200:
                raise RuntimeError(f"Bad status {response.status}")
            
            ip = text.strip()
            if not ip:
                raise RuntimeError("Empty IP response")
            
            # Використовуємо функцію з mongo_operations модуля
            if await update_api_key_ip(mongo_client, key_id, ip, ip_usage_logger):
                return current_proxy, ip
            else:
                # Duplicate IP, try new session
                old_proxy = current_proxy
                current_proxy = current_proxy.generate_new_sessid()
                continue
                
        except Exception:
            if attempt < max_attempts - 1:
                old_proxy = current_proxy
                current_proxy = current_proxy.generate_new_sessid()
                continue
            
    return current_proxy, ""

def format_api_error(raw_response: str) -> str:
    try:
        error_data = json.loads(raw_response)
        if "error" in error_data:
            error = error_data["error"]
            code = error.get("code", "Unknown")
            status = error.get("status", "Unknown")
            message = error.get("message", "No message")
            return f"{code} {status}: {message}"
    except (json.JSONDecodeError, KeyError, AttributeError):
        pass
    
    return raw_response[:200] + "..." if len(raw_response) > 200 else raw_response

def parse_url_status(response_data: dict) -> Tuple[str, str]:
    candidates = response_data.get("candidates", [])
    if not candidates:
        return "NO_CANDIDATES", ""
    
    candidate = candidates[0]
    
    text_response = candidate.get("content", {}).get("parts", [{}])[0].get("text", "")
    
    url_metadata = candidate.get("urlContextMetadata", {}).get("urlMetadata", [])
    if not url_metadata:
        return "NO_URL_METADATA", text_response
    
    grounding_status = url_metadata[0].get("urlRetrievalStatus", "UNKNOWN")
    
    return grounding_status, text_response

async def analyze_website_stage1(target_uri: str, api_key: str, proxy_config: ProxyConfig) -> dict:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{STAGE1_MODEL}:generateContent?key={api_key}"
    
    stage1_prompt = generate_stage1_prompt()
    
    user_message = f"Analyze website {target_uri}\n\n{stage1_prompt}"
    
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": user_message}
                ]
            }
        ],
        "tools": [{"urlContext": {}}],
        "generationConfig": {
            "temperature": 0.3
        }
    }

    start_time = asyncio.get_event_loop().time()
    
    try:
        response, resp_data = await safe_session_request(
            proxy_config,
            "POST", 
            url,
            "stage1",
            json=payload
        )
        
        end_time = asyncio.get_event_loop().time()
        response_time = end_time - start_time
        
        if response.status == 200:
            if isinstance(resp_data, dict):
                grounding_status, text_response = parse_url_status(resp_data)
            else:
                return {
                    "success": False,
                    "grounding_status": "NON_JSON_RESPONSE",
                    "text_response": "",
                    "error": f"API returned HTML instead of JSON: {resp_data}",
                    "status_code": response.status,
                    "response_time": response_time
                }
            
            return {
                "success": True,
                "grounding_status": grounding_status,
                "text_response": text_response,
                "status_code": response.status,
                "response_time": response_time
            }
        else:
            formatted_error = format_api_error(str(resp_data))
            return {
                "success": False,
                "grounding_status": "HTTP_ERROR",
                "text_response": "",
                "error": f"HTTP {response.status}: {formatted_error}",
                "status_code": response.status,
                "response_time": response_time
            }
                    
    except Exception as e:
        end_time = asyncio.get_event_loop().time()
        response_time = end_time - start_time
        
        error_details = classify_exception(e)
        
        error_msg = str(e)
        if "{" in error_msg and "}" in error_msg:
            try:
                json_start = error_msg.find("{")
                json_part = error_msg[json_start:]
                formatted_error = format_api_error(json_part)
                error_msg = error_msg[:json_start] + formatted_error
            except:
                pass
        
        return {
            "success": False,
            "grounding_status": "EXCEPTION",
            "text_response": "",
            "error": f"Request failed: {error_msg}",
            "exception": e,
            "error_details": error_details,
            "status_code": None,
            "response_time": response_time
        }

async def analyze_website_stage2(target_uri: str, text_content: str, api_key: str, proxy_config: ProxyConfig, 
                                segment_combined: str = "", domain_full: str = "") -> dict:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{STAGE2_MODEL}:generateContent?key={api_key}"
    
    # Передаємо обидва параметри в generate_system_prompt
    current_system_prompt = generate_system_prompt(segment_combined, domain_full)
    
    user_message = f"Analyze content review of website {target_uri}: {text_content}"
    
    schema = STAGE2_SCHEMA

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": user_message}
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.3,
            "responseMimeType": "application/json",
            "responseSchema": schema
        },
        "systemInstruction": {
            "parts": [
                {"text": current_system_prompt}
            ]
        }
    }

    start_time = asyncio.get_event_loop().time()

    try:
        timeout = aiohttp.ClientTimeout(
            total=TOTAL_TIMEOUT,
            connect=CONNECT_TIMEOUT,
            sock_connect=SOCK_CONNECT_TIMEOUT,
            sock_read=STAGE2_TIMEOUT_SECONDS
        )
        
        response, resp_data = await safe_session_request(
            proxy_config,
            "POST",
            url,
            "stage2",
            json=payload,
            timeout=timeout
        )
        
        end_time = asyncio.get_event_loop().time()
        response_time = end_time - start_time
        
        if response.status == 200:
            if not isinstance(resp_data, dict):
                return {
                    "success": False,
                    "status_code": 200,
                    "response_time": response_time,
                    "error": f"API returned HTML instead of JSON: {resp_data}"
                }
                
            candidates = resp_data.get("candidates", [])
            if not candidates:
                return {"success": False, "status_code": 200, "response_time": response_time, "error": "No candidates in response"}
            
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if not parts:
                return {"success": False, "status_code": 200, "response_time": response_time, "error": "No parts in content"}
            
            text = parts[0].get("text")
            if not text:
                return {"success": False, "status_code": 200, "response_time": response_time, "error": "No text in parts"}
            
            try:
                parsed_result = json.loads(text)
                return {"success": True, "status_code": 200, "response_time": response_time, "result": parsed_result}
            except json.JSONDecodeError:
                return {"success": False, "status_code": 200, "response_time": response_time, "error": "Invalid JSON"}
                        
        else:
            formatted_error = format_api_error(str(resp_data))
            return {"success": False, "status_code": response.status, "response_time": response_time, "error": f"HTTP {response.status}: {formatted_error}"}
                    
    except Exception as e:
        end_time = asyncio.get_event_loop().time()
        response_time = end_time - start_time
        
        error_details = classify_exception(e)
        
        error_msg = str(e)
        if "{" in error_msg and "}" in error_msg:
            try:
                json_start = error_msg.find("{")
                json_part = error_msg[json_start:]
                formatted_error = format_api_error(json_part)
                error_msg = error_msg[:json_start] + formatted_error
            except:
                pass
        
        return {
            "success": False, 
            "status_code": None, 
            "response_time": response_time, 
            "error": f"Request failed: {error_msg}", 
            "exception": e,
            "error_details": error_details
        }

async def handle_stage_result(mongo_client, worker_id, stage_name, api_key, target_uri, proxy_config, key_record_id, result):
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
    mongo_client = AsyncIOMotorClient(API_DB_URI, **CLIENT_PARAMS)
    
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
                    stage1_result = await controlled_stage1_request(analyze_website_stage1, target_uri, api_key1, working_proxy1)
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
                    # Передаємо domain_full в analyze_website_stage2
                    stage2_result = await controlled_stage2_request(
                        analyze_website_stage2, target_uri, text_response, api_key2, working_proxy2, 
                        segment_combined, domain_full
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