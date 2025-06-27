#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import logging.handlers
from pathlib import Path
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional

# Константи
LOG_DIR = Path("logs")

# Для типізації ErrorDetails (якщо потрібно)
class ErrorType(Enum):
    PROXY = "proxy"
    NETWORK = "network"
    API = "api"
    PAYLOAD = "payload"
    TIMEOUT = "timeout"
    SSL = "ssl"
    DNS = "dns"
    UNKNOWN = "unknown"

@dataclass
class ErrorDetails:
    error_type: ErrorType
    exception_class: str
    error_message: str
    should_retry: bool
    api_key_consumed: bool
    suggested_action: str


def configure_logging() -> logging.Logger:
    """Конфігурує основний системний logger"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "system_errors.log"
    logger = logging.getLogger("system_errors")
    logger.handlers = []
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ))
    logger.addHandler(file_handler)
    return logger


def configure_segmentation_validation_logging() -> logging.Logger:
    """Конфігурує logger для валідації AI сегментації"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    seg_val_file = LOG_DIR / "ai_segmentation_validation.log"

    seg_val_logger = logging.getLogger("ai_segmentation_validation")
    seg_val_logger.handlers = []
    seg_val_logger.setLevel(logging.INFO)
    seg_val_logger.propagate = False

    handler = logging.handlers.RotatingFileHandler(
        seg_val_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    seg_val_logger.addHandler(handler)
    return seg_val_logger


def configure_success_timing_logging() -> logging.Logger:
    """Конфігурує logger для успішних операцій та їх таймінгу"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    success_log_file = LOG_DIR / "success_timing.log"
    success_logger = logging.getLogger("success_timing")
    success_logger.handlers = []
    success_logger.setLevel(logging.INFO)
    success_logger.propagate = False
    success_file_handler = logging.handlers.RotatingFileHandler(
        success_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    success_file_handler.setLevel(logging.INFO)
    success_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    success_logger.addHandler(success_file_handler)
    return success_logger


def configure_rate_limits_logging() -> logging.Logger:
    """Конфігурує logger для rate limits"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    rate_limits_log_file = LOG_DIR / "rate_limits.log"
    rate_limits_logger = logging.getLogger("rate_limits")
    rate_limits_logger.handlers = []
    rate_limits_logger.setLevel(logging.INFO)
    rate_limits_logger.propagate = False
    rate_limits_file_handler = logging.handlers.RotatingFileHandler(
        rate_limits_log_file, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
    )
    rate_limits_file_handler.setLevel(logging.INFO)
    rate_limits_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    rate_limits_logger.addHandler(rate_limits_file_handler)
    return rate_limits_logger


def configure_http_errors_logging() -> logging.Logger:
    """Конфігурує logger для HTTP помилок"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    http_errors_log_file = LOG_DIR / "http_errors.log"
    http_errors_logger = logging.getLogger("http_errors")
    http_errors_logger.handlers = []
    http_errors_logger.setLevel(logging.INFO)
    http_errors_logger.propagate = False
    http_errors_file_handler = logging.handlers.RotatingFileHandler(
        http_errors_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    http_errors_file_handler.setLevel(logging.INFO)
    http_errors_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    http_errors_logger.addHandler(http_errors_file_handler)
    return http_errors_logger


def configure_stage1_issues_logging() -> logging.Logger:
    """Конфігурує logger для проблем Stage1"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stage1_issues_log_file = LOG_DIR / "stage1_issues.log"
    stage1_issues_logger = logging.getLogger("stage1_issues")
    stage1_issues_logger.handlers = []
    stage1_issues_logger.setLevel(logging.INFO)
    stage1_issues_logger.propagate = False
    stage1_issues_file_handler = logging.handlers.RotatingFileHandler(
        stage1_issues_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    stage1_issues_file_handler.setLevel(logging.INFO)
    stage1_issues_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    stage1_issues_logger.addHandler(stage1_issues_file_handler)
    return stage1_issues_logger


def configure_proxy_errors_logging() -> logging.Logger:
    """Конфігурує logger для помилок проксі"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    proxy_errors_log_file = LOG_DIR / "proxy_errors.log"
    proxy_errors_logger = logging.getLogger("proxy_errors")
    proxy_errors_logger.handlers = []
    proxy_errors_logger.setLevel(logging.INFO)
    proxy_errors_logger.propagate = False
    proxy_errors_file_handler = logging.handlers.RotatingFileHandler(
        proxy_errors_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    proxy_errors_file_handler.setLevel(logging.INFO)
    proxy_errors_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    proxy_errors_logger.addHandler(proxy_errors_file_handler)
    return proxy_errors_logger


def configure_network_errors_logging() -> logging.Logger:
    """Конфігурує logger для мережевих помилок"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    network_errors_log_file = LOG_DIR / "network_errors.log"
    network_errors_logger = logging.getLogger("network_errors")
    network_errors_logger.handlers = []
    network_errors_logger.setLevel(logging.INFO)
    network_errors_logger.propagate = False
    network_errors_file_handler = logging.handlers.RotatingFileHandler(
        network_errors_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    network_errors_file_handler.setLevel(logging.INFO)
    network_errors_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    network_errors_logger.addHandler(network_errors_file_handler)
    return network_errors_logger


def configure_api_errors_logging() -> logging.Logger:
    """Конфігурує logger для API помилок"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    api_errors_log_file = LOG_DIR / "api_errors.log"
    api_errors_logger = logging.getLogger("api_errors")
    api_errors_logger.handlers = []
    api_errors_logger.setLevel(logging.INFO)
    api_errors_logger.propagate = False
    api_errors_file_handler = logging.handlers.RotatingFileHandler(
        api_errors_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    api_errors_file_handler.setLevel(logging.INFO)
    api_errors_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    api_errors_logger.addHandler(api_errors_file_handler)
    return api_errors_logger


def configure_payload_errors_logging() -> logging.Logger:
    """Конфігурує logger для payload помилок"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    payload_errors_log_file = LOG_DIR / "payload_errors.log"
    payload_errors_logger = logging.getLogger("payload_errors")
    payload_errors_logger.handlers = []
    payload_errors_logger.setLevel(logging.INFO)
    payload_errors_logger.propagate = False
    payload_errors_file_handler = logging.handlers.RotatingFileHandler(
        payload_errors_log_file, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
    )
    payload_errors_file_handler.setLevel(logging.INFO)
    payload_errors_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    payload_errors_logger.addHandler(payload_errors_file_handler)
    return payload_errors_logger


def configure_unknown_errors_logging() -> logging.Logger:
    """Конфігурує logger для невідомих помилок"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    unknown_errors_log_file = LOG_DIR / "unknown_errors.log"
    unknown_errors_logger = logging.getLogger("unknown_errors")
    unknown_errors_logger.handlers = []
    unknown_errors_logger.setLevel(logging.INFO)
    unknown_errors_logger.propagate = False
    unknown_errors_file_handler = logging.handlers.RotatingFileHandler(
        unknown_errors_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    unknown_errors_file_handler.setLevel(logging.INFO)
    unknown_errors_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    unknown_errors_logger.addHandler(unknown_errors_file_handler)
    return unknown_errors_logger


def configure_revert_reasons_logging() -> logging.Logger:
    """Конфігурує logger для причин revert операцій"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    revert_reasons_log_file = LOG_DIR / "revert_reasons.log"
    revert_reasons_logger = logging.getLogger("revert_reasons")
    revert_reasons_logger.handlers = []
    revert_reasons_logger.setLevel(logging.INFO)
    revert_reasons_logger.propagate = False
    revert_reasons_file_handler = logging.handlers.RotatingFileHandler(
        revert_reasons_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    revert_reasons_file_handler.setLevel(logging.INFO)
    revert_reasons_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    revert_reasons_logger.addHandler(revert_reasons_file_handler)
    return revert_reasons_logger


def configure_short_response_debug_logging() -> logging.Logger:
    """Конфігурує logger для debug коротких відповідей"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    short_response_debug_log_file = LOG_DIR / "short_response_debug.log"
    short_response_debug_logger = logging.getLogger("short_response_debug")
    short_response_debug_logger.handlers = []
    short_response_debug_logger.setLevel(logging.INFO)
    short_response_debug_logger.propagate = False
    short_response_debug_file_handler = logging.handlers.RotatingFileHandler(
        short_response_debug_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    short_response_debug_file_handler.setLevel(logging.INFO)
    short_response_debug_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    short_response_debug_logger.addHandler(short_response_debug_file_handler)
    return short_response_debug_logger


def configure_ip_usage_logging() -> logging.Logger:
    """Конфігурує logger для використання IP адрес"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ip_usage_log_file = LOG_DIR / "ip_usage.log"
    ip_usage_logger = logging.getLogger("ip_usage")
    ip_usage_logger.handlers = []
    ip_usage_logger.setLevel(logging.INFO)
    ip_usage_logger.propagate = False
    ip_usage_file_handler = logging.handlers.RotatingFileHandler(
        ip_usage_log_file, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
    )
    ip_usage_file_handler.setLevel(logging.INFO)
    ip_usage_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    ip_usage_logger.addHandler(ip_usage_file_handler)
    return ip_usage_logger


def setup_all_loggers() -> Dict[str, logging.Logger]:
    """
    Налаштовує всі логгери системи
    
    Returns:
        Словник з усіма налаштованими логгерами
    """
    loggers = {
        'system_errors': configure_logging(),
        'segmentation_validation': configure_segmentation_validation_logging(),
        'success_timing': configure_success_timing_logging(),
        'rate_limits': configure_rate_limits_logging(),
        'http_errors': configure_http_errors_logging(),
        'stage1_issues': configure_stage1_issues_logging(),
        'proxy_errors': configure_proxy_errors_logging(),
        'network_errors': configure_network_errors_logging(),
        'api_errors': configure_api_errors_logging(),
        'payload_errors': configure_payload_errors_logging(),
        'unknown_errors': configure_unknown_errors_logging(),
        'ip_usage': configure_ip_usage_logging(),
        'revert_reasons': configure_revert_reasons_logging(),
        'short_response_debug': configure_short_response_debug_logging(),
    }
    
    # Налаштовуємо рівень логування для aiohttp
    logging.getLogger('aiohttp.client').setLevel(logging.WARNING)
    
    return loggers


# === ФУНКЦІЇ ЛОГУВАННЯ ===

def log_success_timing(worker_id: int, stage: str, api_key: str, target_uri: str, response_time: float, success_timing_logger: logging.Logger):
    """Логує успішну операцію з часом відповіді"""
    masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
    short_uri = target_uri[:60] + "..." if len(target_uri) > 60 else target_uri
    success_timing_logger.info(f"Worker-{worker_id:02d} | {stage:6s} | 200 | {response_time:.1f}s | Key: {masked_key} | {short_uri}")


def log_rate_limit(worker_id: int, stage: str, api_key: str, target_uri: str, freeze_minutes: int, rate_limits_logger: logging.Logger):
    """Логує досягнення rate limit"""
    masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
    short_uri = target_uri[:60] + "..." if len(target_uri) > 60 else target_uri
    rate_limits_logger.info(f"Worker-{worker_id:02d} | {stage:6s} | 429 | Key: {masked_key} | {short_uri} | UNAVAILABLE for 3min (natural filter)")


def log_http_error(worker_id: int, stage: str, api_key: str, target_uri: str, status_code: int, error_msg: str, http_errors_logger: logging.Logger):
    """Логує HTTP помилку"""
    masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
    short_uri = target_uri[:60] + "..." if len(target_uri) > 60 else target_uri
    short_error = error_msg[:80] + "..." if len(error_msg) > 80 else error_msg
    http_errors_logger.info(f"Worker-{worker_id:02d} | {stage:6s} | {status_code} | Key: {masked_key} | {short_uri} | {short_error}")


def log_stage1_issue(worker_id: int, api_key: str, target_uri: str, issue_type: str, stage1_issues_logger: logging.Logger, details: str = ""):
    """Логує проблему Stage1"""
    masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
    short_uri = target_uri[:60] + "..." if len(target_uri) > 60 else target_uri
    stage1_issues_logger.info(f"Worker-{worker_id:02d} | {issue_type} | Key: {masked_key} | {short_uri} | {details}")


def log_error_details(worker_id: int, stage: str, api_key: str, target_uri: str, 
                     error_details: ErrorDetails, response_time: float,
                     proxy_errors_logger: logging.Logger, network_errors_logger: logging.Logger,
                     api_errors_logger: logging.Logger, payload_errors_logger: logging.Logger,
                     unknown_errors_logger: logging.Logger):
    """Логує детальну інформацію про помилку"""
    masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
    short_uri = target_uri[:60] + "..." if len(target_uri) > 60 else target_uri
    
    log_message = (f"Worker-{worker_id:02d} | {stage:6s} | {error_details.exception_class} | "
                  f"Key: {masked_key} | {short_uri} | {response_time:.1f}s | "
                  f"Retry: {error_details.should_retry} | Consumed: {error_details.api_key_consumed} | "
                  f"Action: {error_details.suggested_action} | Details: {error_details.error_message}")
    
    if error_details.error_type == ErrorType.PROXY:
        proxy_errors_logger.info(log_message)
    elif error_details.error_type == ErrorType.NETWORK:
        network_errors_logger.info(log_message)
    elif error_details.error_type in [ErrorType.DNS, ErrorType.SSL, ErrorType.TIMEOUT]:
        network_errors_logger.info(log_message)
    elif error_details.error_type == ErrorType.API:
        api_errors_logger.info(log_message)
    elif error_details.error_type == ErrorType.PAYLOAD:
        payload_errors_logger.info(log_message)
    else:
        unknown_errors_logger.info(log_message)


def log_proxy_error(worker_id: int, stage: str, proxy_config, target_uri: str, error_msg: str, proxy_errors_logger: logging.Logger):
    """Логує помилку проксі"""
    short_uri = target_uri[:60] + "..." if len(target_uri) > 60 else target_uri
    short_error = error_msg[:100] + "..." if len(error_msg) > 100 else error_msg
    proxy_errors_logger.info(f"Worker-{worker_id:02d} | {stage:6s} | {proxy_config.connection_string} | {short_uri} | {short_error}")


if __name__ == "__main__":
    # Тестування logging_config модуля
    print("=== Logging Configuration Test Suite ===\n")
    
    # Тест 1: Налаштування всіх логгерів
    print("1. Setting up all loggers:")
    loggers = setup_all_loggers()
    for name, logger in loggers.items():
        handler_count = len(logger.handlers)
        level_name = logging.getLevelName(logger.level)
        print(f"   ✓ {name:25s} → Level: {level_name:8s} → Handlers: {handler_count}")
    
    # Тест 2: Тест log файлів
    print(f"\n2. Log files created in '{LOG_DIR}' directory:")
    if LOG_DIR.exists():
        log_files = list(LOG_DIR.glob("*.log"))
        for log_file in sorted(log_files):
            size = log_file.stat().st_size if log_file.exists() else 0
            print(f"   📄 {log_file.name:30s} → Size: {size:6d} bytes")
    else:
        print("   📁 Log directory will be created on first use")
    
    # Тест 3: Тестування функцій логування
    print(f"\n3. Testing logging functions:")
    
    # Створюємо тестові дані
    test_worker_id = 1
    test_stage = "Test"
    test_api_key = "test_key_1234567890abcdef"
    test_uri = "https://example.com/very/long/path/to/test/website"
    test_response_time = 1.234
    
    # Тестуємо success timing
    try:
        log_success_timing(test_worker_id, test_stage, test_api_key, test_uri, test_response_time, loggers['success_timing'])
        print("   ✓ log_success_timing() → SUCCESS")
    except Exception as e:
        print(f"   ✗ log_success_timing() → ERROR: {e}")
    
    # Тестуємо rate limit
    try:
        log_rate_limit(test_worker_id, test_stage, test_api_key, test_uri, 3, loggers['rate_limits'])
        print("   ✓ log_rate_limit() → SUCCESS")
    except Exception as e:
        print(f"   ✗ log_rate_limit() → ERROR: {e}")
    
    # Тестуємо HTTP error
    try:
        log_http_error(test_worker_id, test_stage, test_api_key, test_uri, 404, "Not Found", loggers['http_errors'])
        print("   ✓ log_http_error() → SUCCESS")
    except Exception as e:
        print(f"   ✗ log_http_error() → ERROR: {e}")
    
    # Тестуємо Stage1 issue
    try:
        log_stage1_issue(test_worker_id, test_api_key, test_uri, "TEST_ISSUE", loggers['stage1_issues'], "Test details")
        print("   ✓ log_stage1_issue() → SUCCESS")
    except Exception as e:
        print(f"   ✗ log_stage1_issue() → ERROR: {e}")
    
    # Тест 4: Перевірка створених log записів
    print(f"\n4. Checking created log entries:")
    test_logs = [
        ("success_timing.log", "Test successful timing log"),
        ("rate_limits.log", "Test rate limit log"),
        ("http_errors.log", "Test HTTP error log"),
        ("stage1_issues.log", "Test Stage1 issue log")
    ]
    
    for log_filename, description in test_logs:
        log_path = LOG_DIR / log_filename
        if log_path.exists() and log_path.stat().st_size > 0:
            print(f"   ✓ {log_filename:25s} → {description}")
        else:
            print(f"   ⚠ {log_filename:25s} → File not created or empty")
    
    print(f"\n=== Test completed ===")
    print(f"Total loggers configured: {len(loggers)}")
    print(f"Module loaded successfully with {len([name for name in globals() if callable(globals()[name]) and not name.startswith('_')])} functions")
