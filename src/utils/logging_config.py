#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import logging.handlers
from pathlib import Path
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, Callable

LOG_DIR = Path("logs")

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

class LazyLogFormatter:
    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
    
    def __str__(self):
        return self.func(*self.args, **self.kwargs)

def configure_logging() -> logging.Logger:
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

def configure_stage2_retries_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stage2_retries_log_file = LOG_DIR / "stage2_retries.log"
    stage2_retries_logger = logging.getLogger("stage2_retries")
    stage2_retries_logger.handlers = []
    stage2_retries_logger.setLevel(logging.INFO)
    stage2_retries_logger.propagate = False
    stage2_retries_file_handler = logging.handlers.RotatingFileHandler(
        stage2_retries_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
    )
    stage2_retries_file_handler.setLevel(logging.INFO)
    stage2_retries_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    stage2_retries_logger.addHandler(stage2_retries_file_handler)
    return stage2_retries_logger

def configure_proxy_errors_logging() -> logging.Logger:
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

def configure_ip_usage_logging() -> logging.Logger:
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

def configure_adaptive_delay_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    adaptive_delay_log_file = LOG_DIR / "adaptive_delay.log"
    adaptive_delay_logger = logging.getLogger("adaptive_delay")
    adaptive_delay_logger.handlers = []
    adaptive_delay_logger.setLevel(logging.INFO)
    adaptive_delay_logger.propagate = False
    adaptive_delay_file_handler = logging.handlers.RotatingFileHandler(
        adaptive_delay_log_file, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
    )
    adaptive_delay_file_handler.setLevel(logging.INFO)
    adaptive_delay_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    adaptive_delay_logger.addHandler(adaptive_delay_file_handler)
    return adaptive_delay_logger

def configure_missing_segmentation_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    missing_segmentation_log_file = LOG_DIR / "missing_segmentation.log"
    missing_segmentation_logger = logging.getLogger("missing_segmentation")
    missing_segmentation_logger.handlers = []
    missing_segmentation_logger.setLevel(logging.INFO)
    missing_segmentation_logger.propagate = False
    missing_segmentation_file_handler = logging.handlers.RotatingFileHandler(
        missing_segmentation_log_file, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
    )
    missing_segmentation_file_handler.setLevel(logging.INFO)
    missing_segmentation_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    missing_segmentation_logger.addHandler(missing_segmentation_file_handler)
    return missing_segmentation_logger

def setup_all_loggers() -> Dict[str, logging.Logger]:
    loggers = {
        'system_errors': configure_logging(),
        'segmentation_validation': configure_segmentation_validation_logging(),
        'success_timing': configure_success_timing_logging(),
        'rate_limits': configure_rate_limits_logging(),
        'http_errors': configure_http_errors_logging(),
        'stage1_issues': configure_stage1_issues_logging(),
        'stage2_retries': configure_stage2_retries_logging(),
        'proxy_errors': configure_proxy_errors_logging(),
        'network_errors': configure_network_errors_logging(),
        'api_errors': configure_api_errors_logging(),
        'payload_errors': configure_payload_errors_logging(),
        'unknown_errors': configure_unknown_errors_logging(),
        'ip_usage': configure_ip_usage_logging(),
        'revert_reasons': configure_revert_reasons_logging(),
        'adaptive_delay': configure_adaptive_delay_logging(),
        'missing_segmentation': configure_missing_segmentation_logging(),
    }
    
    logging.getLogger('aiohttp.client').setLevel(logging.WARNING)
    
    return loggers

def _format_masked_key(api_key: str) -> str:
    return f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"

def _format_short_domain(domain_full: str) -> str:
    return domain_full[:60] + "..." if len(domain_full) > 60 else domain_full

def log_success_timing(worker_id: int, stage: str, api_key: str, domain_full: str, response_time: float, success_timing_logger: logging.Logger):
    if not success_timing_logger.isEnabledFor(logging.INFO):
        return
    
    def format_message():
        masked_key = _format_masked_key(api_key)
        short_domain = _format_short_domain(domain_full)
        return f"Worker-{worker_id:02d} | {stage:6s} | 200 | {response_time:.1f}s | Key: {masked_key} | {short_domain}"
    
    success_timing_logger.info(LazyLogFormatter(format_message))

def log_rate_limit(worker_id: int, stage: str, api_key: str, domain_full: str, freeze_minutes: int, limit_type: str, rate_limits_logger: logging.Logger):
    """Enhanced rate limit logging with 429 classification"""
    if not rate_limits_logger.isEnabledFor(logging.INFO):
        return
    
    def format_message():
        masked_key = _format_masked_key(api_key)
        short_domain = _format_short_domain(domain_full)
        return f"Worker-{worker_id:02d} | {stage:6s} | 429 | Type:{limit_type} | Key: {masked_key} | {short_domain} | UNAVAILABLE for {freeze_minutes}min"
    
    rate_limits_logger.info(LazyLogFormatter(format_message))

def log_http_error(worker_id: int, stage: str, api_key: str, domain_full: str, status_code: int, error_msg: str, http_errors_logger: logging.Logger):
    if not http_errors_logger.isEnabledFor(logging.INFO):
        return
    
    def format_message():
        masked_key = _format_masked_key(api_key)
        short_domain = _format_short_domain(domain_full)
        short_error = error_msg[:80] + "..." if len(error_msg) > 80 else error_msg
        return f"Worker-{worker_id:02d} | {stage:6s} | {status_code} | Key: {masked_key} | {short_domain} | {short_error}"
    
    http_errors_logger.info(LazyLogFormatter(format_message))

def log_stage1_issue_enhanced(worker_id: int, api_key: str, domain_full: str, issue_type: str, 
                             stage1_issues_logger: logging.Logger, details: str = "", 
                             attempt_info: str = "", content_preview: str = "", 
                             response_length: int = 0):
    if not stage1_issues_logger.isEnabledFor(logging.INFO):
        return
    
    def format_message():
        masked_key = _format_masked_key(api_key)
        short_domain = _format_short_domain(domain_full)
        
        if issue_type == "short_response":
            content_safe = repr(content_preview) if content_preview else '""'
            return (f"Worker-{worker_id:02d} | {issue_type} | Key: {masked_key} | {short_domain} | "
                   f"Attempt: {attempt_info} | Length: {response_length} chars | Content: {content_safe}")
        
        elif issue_type == "stage1_request_failed":
            specific_reason = details if details else "unknown_error"
            return (f"Worker-{worker_id:02d} | {issue_type} | Key: {masked_key} | {short_domain} | "
                   f"Reason: {specific_reason}")
        
        else:
            details_part = f" | {details}" if details else ""
            return (f"Worker-{worker_id:02d} | {issue_type} | Key: {masked_key} | {short_domain}{details_part}")
    
    stage1_issues_logger.info(LazyLogFormatter(format_message))

def log_short_response_with_retry_info(worker_id: int, api_key: str, domain_full: str, 
                                      response_length: int, content_preview: str, 
                                      current_attempt: int, stage1_issues_logger: logging.Logger):
    attempt_info = f"{current_attempt}/5"
    log_stage1_issue_enhanced(
        worker_id=worker_id,
        api_key=api_key,
        domain_full=domain_full,
        issue_type="short_response",
        stage1_issues_logger=stage1_issues_logger,
        attempt_info=attempt_info,
        content_preview=content_preview,
        response_length=response_length
    )

def log_stage1_request_failed_with_reason(worker_id: int, api_key: str, domain_full: str, 
                                         failure_reason: str, stage1_issues_logger: logging.Logger):
    log_stage1_issue_enhanced(
        worker_id=worker_id,
        api_key=api_key,
        domain_full=domain_full,
        issue_type="stage1_request_failed",
        stage1_issues_logger=stage1_issues_logger,
        details=failure_reason
    )

def log_short_response_max_attempts(worker_id: int, api_key: str, domain_full: str, 
                                   total_attempts: int, stage1_issues_logger: logging.Logger):
    log_stage1_issue_enhanced(
        worker_id=worker_id,
        api_key=api_key,
        domain_full=domain_full,
        issue_type="short_response_max_attempts",
        stage1_issues_logger=stage1_issues_logger,
        details=f"Reached maximum {total_attempts} attempts, setting error status"
    )

def log_stage1_issue(worker_id: int, api_key: str, domain_full: str, issue_type: str, stage1_issues_logger: logging.Logger, details: str = ""):
    log_stage1_issue_enhanced(worker_id, api_key, domain_full, issue_type, stage1_issues_logger, details)

def log_stage2_retry(worker_id: int, api_key: str, domain_full: str, retry_count: int, segments_full: str, stage2_retries_logger: logging.Logger):
    if not stage2_retries_logger.isEnabledFor(logging.INFO):
        return
    
    def format_message():
        masked_key = _format_masked_key(api_key)
        short_domain = _format_short_domain(domain_full)
        short_segments = segments_full[:50] + "..." if len(segments_full) > 50 else segments_full
        return f"Worker-{worker_id:02d} | Retry #{retry_count} | Key: {masked_key} | {short_domain} | Invalid segments_full: '{short_segments}'"
    
    stage2_retries_logger.info(LazyLogFormatter(format_message))

def log_error_details(worker_id: int, stage: str, api_key: str, domain_full: str, 
                     error_details: ErrorDetails, response_time: float,
                     proxy_errors_logger: logging.Logger, network_errors_logger: logging.Logger,
                     api_errors_logger: logging.Logger, payload_errors_logger: logging.Logger,
                     unknown_errors_logger: logging.Logger):
    
    def format_message():
        masked_key = _format_masked_key(api_key)
        short_domain = _format_short_domain(domain_full)
        return (f"Worker-{worker_id:02d} | {stage:6s} | {error_details.exception_class} | "
               f"Key: {masked_key} | {short_domain} | {response_time:.1f}s | "
               f"Retry: {error_details.should_retry} | Consumed: {error_details.api_key_consumed} | "
               f"Action: {error_details.suggested_action} | Details: {error_details.error_message}")
    
    if error_details.error_type == ErrorType.PROXY:
        if proxy_errors_logger.isEnabledFor(logging.INFO):
            proxy_errors_logger.info(LazyLogFormatter(format_message))
    elif error_details.error_type == ErrorType.NETWORK:
        if network_errors_logger.isEnabledFor(logging.INFO):
            network_errors_logger.info(LazyLogFormatter(format_message))
    elif error_details.error_type in [ErrorType.DNS, ErrorType.SSL, ErrorType.TIMEOUT]:
        if network_errors_logger.isEnabledFor(logging.INFO):
            network_errors_logger.info(LazyLogFormatter(format_message))
    elif error_details.error_type == ErrorType.API:
        if api_errors_logger.isEnabledFor(logging.INFO):
            api_errors_logger.info(LazyLogFormatter(format_message))
    elif error_details.error_type == ErrorType.PAYLOAD:
        if payload_errors_logger.isEnabledFor(logging.INFO):
            payload_errors_logger.info(LazyLogFormatter(format_message))
    else:
        if unknown_errors_logger.isEnabledFor(logging.INFO):
            unknown_errors_logger.info(LazyLogFormatter(format_message))

def log_proxy_error(worker_id: int, stage: str, proxy_config, domain_full: str, error_msg: str, proxy_errors_logger: logging.Logger):
    if not proxy_errors_logger.isEnabledFor(logging.INFO):
        return
    
    def format_message():
        short_domain = _format_short_domain(domain_full)
        short_error = error_msg[:100] + "..." if len(error_msg) > 100 else error_msg
        return f"Worker-{worker_id:02d} | {stage:6s} | {proxy_config.connection_string} | {short_domain} | {short_error}"
    
    proxy_errors_logger.info(LazyLogFormatter(format_message))

if __name__ == "__main__":
    print("=== Enhanced Logging Configuration with 429 Classification ===\n")
    
    print("1. Setting up all loggers:")
    loggers = setup_all_loggers()
    for name, logger in loggers.items():
        handler_count = len(logger.handlers)
        level_name = logging.getLevelName(logger.level)
        print(f"   ✓ {name:25s} → Level: {level_name:8s} → Handlers: {handler_count}")
    
    print(f"\n2. Log files created in '{LOG_DIR}' directory:")
    if LOG_DIR.exists():
        log_files = list(LOG_DIR.glob("*.log"))
        for log_file in sorted(log_files):
            size = log_file.stat().st_size if log_file.exists() else 0
            print(f"   📄 {log_file.name:30s} → Size: {size:6d} bytes")
    else:
        print("   📁 Log directory will be created on first use")
    
    print(f"\n3. Testing enhanced rate limit logging:")
    
    rate_limits_logger = loggers['rate_limits']
    
    try:
        # Test with different limit types
        log_rate_limit(1, "Stage1", "AIz...ABC4", "example.com", 3, "PERSONAL_QUOTA", rate_limits_logger)
        log_rate_limit(2, "Stage2", "AIz...XYZ9", "test.org", 3, "GLOBAL_LIMIT", rate_limits_logger)
        log_rate_limit(3, "Stage1", "AIz...DEF7", "sample.net", 3, "UNKNOWN", rate_limits_logger)
        print("   ✓ Enhanced rate limit logging test successful")
        print("   📊 Sample outputs:")
        print("      Worker-01 | Stage1 | 429 | Type:PERSONAL_QUOTA | Key: AIz...ABC4 | example.com | UNAVAILABLE for 3min")
        print("      Worker-02 | Stage2 | 429 | Type:GLOBAL_LIMIT | Key: AIz...XYZ9 | test.org | UNAVAILABLE for 3min")
        print("      Worker-03 | Stage1 | 429 | Type:UNKNOWN | Key: AIz...DEF7 | sample.net | UNAVAILABLE for 3min")
        
    except Exception as e:
        print(f"   ✗ enhanced rate limit logging test → ERROR: {e}")
    
    print(f"\n=== Enhanced logging ready ===")
    print(f"🔍 NEW: 429 errors now include classification (PERSONAL_QUOTA/GLOBAL_LIMIT)")
    print(f"📊 Updated log_rate_limit() function accepts limit_type parameter")
    print(f"🚀 Ready for enhanced rate limit analysis!")
    print(f"Total loggers configured: {len(loggers)}")