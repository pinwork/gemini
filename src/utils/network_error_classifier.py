#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum
from dataclasses import dataclass
from typing import Optional
from aiohttp_socks import ProxyConnectionError, ProxyTimeoutError, ProxyError
from aiohttp import (
    ClientError, ClientConnectionError, ClientOSError, ClientConnectorError,
    ClientConnectorDNSError, ClientSSLError, ClientConnectorSSLError, 
    ClientConnectorCertificateError, ServerConnectionError, ServerDisconnectedError,
    ServerTimeoutError, ConnectionTimeoutError, SocketTimeoutError,
    ClientResponseError, ClientPayloadError, ClientConnectionResetError
)


class ErrorType(Enum):
    """Типи помилок для класифікації винятків"""
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
    """Детальна інформація про помилку"""
    error_type: ErrorType
    exception_class: str
    error_message: str
    should_retry: bool
    api_key_consumed: bool
    suggested_action: str


def classify_exception(exception: Exception, response_status: Optional[int] = None) -> ErrorDetails:
    """
    Класифікує винятки та HTTP статуси для визначення типу помилки та стратегії обробки
    
    Args:
        exception: Виняток для класифікації (може бути None якщо є response_status)
        response_status: HTTP статус код (опціонально)
        
    Returns:
        ErrorDetails з інформацією про тип помилки та рекомендованою дією
    """
    # Спочатку перевіряємо HTTP статус коди
    if response_status is not None and response_status != 200:
        if response_status == 429:
            return ErrorDetails(
                error_type=ErrorType.API,
                exception_class="HTTPRateLimit",
                error_message=f"Rate limit hit: HTTP {response_status}",
                should_retry=True,
                api_key_consumed=True,
                suggested_action="Freeze API key, retry with different key"
            )
        elif response_status in [401, 403]:
            return ErrorDetails(
                error_type=ErrorType.API,
                exception_class="HTTPAuth",
                error_message=f"Authentication error: HTTP {response_status}",
                should_retry=False,
                api_key_consumed=True,
                suggested_action="Check API key validity, disable if invalid"
            )
        elif response_status >= 500:
            return ErrorDetails(
                error_type=ErrorType.API,
                exception_class="HTTPServerError",
                error_message=f"Server error: HTTP {response_status}",
                should_retry=True,
                api_key_consumed=True,
                suggested_action="Retry with backoff, server issue"
            )
        else:
            return ErrorDetails(
                error_type=ErrorType.API,
                exception_class="HTTPClientError",
                error_message=f"Client error: HTTP {response_status}",
                should_retry=False,
                api_key_consumed=True,
                suggested_action="Check request parameters"
            )
    
    # Якщо немає винятку, повертаємо невідомий тип
    if exception is None:
        return ErrorDetails(
            error_type=ErrorType.UNKNOWN,
            exception_class="NoException",
            error_message="No exception provided",
            should_retry=False,
            api_key_consumed=False,
            suggested_action="Investigation needed"
        )
    
    # Перевіряємо прямі помилки проксі
    if isinstance(exception, (ProxyConnectionError, ProxyTimeoutError, ProxyError)):
        proxy_type = type(exception).__name__
        return ErrorDetails(
            error_type=ErrorType.PROXY,
            exception_class=proxy_type,
            error_message=f"Proxy error: {str(exception)}",
            should_retry=True,
            api_key_consumed=False,
            suggested_action="Try different session ID or proxy"
        )
    
    # Перевіряємо загорнуті помилки проксі (wrapped exceptions)
    if hasattr(exception, '__cause__') and exception.__cause__:
        if isinstance(exception.__cause__, (ProxyConnectionError, ProxyTimeoutError, ProxyError)):
            proxy_type = type(exception.__cause__).__name__
            return ErrorDetails(
                error_type=ErrorType.PROXY,
                exception_class=f"Wrapped{proxy_type}",
                error_message=f"Wrapped proxy error: {str(exception.__cause__)}",
                should_retry=True,
                api_key_consumed=False,
                suggested_action="Try different session ID or proxy"
            )
    
    # DNS помилки
    if isinstance(exception, ClientConnectorDNSError):
        return ErrorDetails(
            error_type=ErrorType.DNS,
            exception_class="DNSResolutionError",
            error_message=f"DNS resolution failed: {str(exception)}",
            should_retry=True,
            api_key_consumed=False,
            suggested_action="Check DNS settings, retry with different proxy"
        )
    
    # SSL помилки
    if isinstance(exception, (ClientSSLError, ClientConnectorSSLError, ClientConnectorCertificateError)):
        ssl_type = type(exception).__name__
        return ErrorDetails(
            error_type=ErrorType.SSL,
            exception_class=ssl_type,
            error_message=f"SSL error: {str(exception)}",
            should_retry=True,
            api_key_consumed=False,
            suggested_action="Check SSL configuration, try different proxy"
        )
    
    # Timeout помилки
    if isinstance(exception, (ServerTimeoutError, ConnectionTimeoutError, SocketTimeoutError)):
        timeout_type = type(exception).__name__
        return ErrorDetails(
            error_type=ErrorType.TIMEOUT,
            exception_class=timeout_type,
            error_message=f"Timeout: {str(exception)}",
            should_retry=True,
            api_key_consumed=False,
            suggested_action="Increase timeout or try different proxy"
        )
    
    # Мережеві помилки
    if isinstance(exception, (ClientConnectorError, ClientConnectionError, ClientOSError,
                             ServerDisconnectedError, ClientConnectionResetError)):
        network_type = type(exception).__name__
        return ErrorDetails(
            error_type=ErrorType.NETWORK,
            exception_class=network_type,
            error_message=f"Network error: {str(exception)}",
            should_retry=True,
            api_key_consumed=False,
            suggested_action="Network issue between proxy and target, retry"
        )
    
    # Payload помилки
    if isinstance(exception, (ClientPayloadError, ClientResponseError)):
        payload_type = type(exception).__name__
        return ErrorDetails(
            error_type=ErrorType.PAYLOAD,
            exception_class=payload_type,
            error_message=f"Payload error: {str(exception)}",
            should_retry=True,
            api_key_consumed=False,
            suggested_action="Response parsing failed, retry request"
        )
    
    # Усі інші винятки - невідомий тип
    return ErrorDetails(
        error_type=ErrorType.UNKNOWN,
        exception_class=type(exception).__name__,
        error_message=f"Unknown error: {str(exception)}",
        should_retry=False,
        api_key_consumed=False,
        suggested_action="Investigation needed, check logs"
    )


def is_proxy_error(exception: Exception) -> bool:
    """
    Перевіряє чи є виняток помилкою проксі
    
    Args:
        exception: Виняток для перевірки
        
    Returns:
        True якщо це помилка проксі
    """
    error_details = classify_exception(exception)
    return error_details.error_type == ErrorType.PROXY


def get_error_summary(exception: Exception, response_status: Optional[int] = None) -> str:
    """
    Повертає короткий опис помилки для логування
    
    Args:
        exception: Виняток для аналізу
        response_status: HTTP статус код (опціонально)
        
    Returns:
        Короткий опис помилки
    """
    error_details = classify_exception(exception, response_status)
    return f"{error_details.error_type.value.upper()}: {error_details.exception_class}"


def should_retry_request(exception: Exception, response_status: Optional[int] = None) -> bool:
    """
    Визначає чи варто повторити запит після помилки
    
    Args:
        exception: Виняток для аналізу
        response_status: HTTP статус код (опціонально)
        
    Returns:
        True якщо запит варто повторити
    """
    error_details = classify_exception(exception, response_status)
    return error_details.should_retry


def was_api_key_consumed(exception: Exception, response_status: Optional[int] = None) -> bool:
    """
    Визначає чи був використаний API ключ при помилці
    
    Args:
        exception: Виняток для аналізу
        response_status: HTTP статус код (опціонально)
        
    Returns:
        True якщо API ключ був використаний
    """
    error_details = classify_exception(exception, response_status)
    return error_details.api_key_consumed


if __name__ == "__main__":
    # Тестування error_handling модуля
    print("=== Error Handling Test Suite ===\n")
    
    # Тест 1: HTTP статус коди
    print("1. HTTP Status Code Classification:")
    test_statuses = [200, 401, 403, 429, 500, 502, 404, 400]
    for status in test_statuses:
        error_details = classify_exception(None, status)
        print(f"   HTTP {status:3d} → {error_details.error_type.value:8s} | "
              f"Retry: {str(error_details.should_retry):5s} | "
              f"Consumed: {str(error_details.api_key_consumed):5s} | "
              f"{error_details.suggested_action}")
    
    # Тест 2: Створення тестових винятків
    print(f"\n2. Exception Type Classification:")
    
    # Симулюємо різні типи винятків
    test_exceptions = [
        (ValueError("Test value error"), "Standard Python exception"),
        (ConnectionError("Test connection error"), "Python connection error"),
        (TimeoutError("Test timeout"), "Python timeout error"),
    ]
    
    # Створюємо mock aiohttp винятки (без реального підключення)
    try:
        import aiohttp
        # Симулюємо створення винятків без реального підключення
        test_exceptions.extend([
            (ClientConnectorDNSError("", OSError("DNS lookup failed")), "DNS resolution error"),
            (ClientConnectorError("", OSError("Connection failed")), "Connection error"),
            (ServerTimeoutError("Request timeout"), "Server timeout"),
            (ClientPayloadError("Payload error"), "Payload parsing error"),
        ])
    except Exception:
        print("   ⚠ Could not create aiohttp exceptions for testing")
    
    for exception, description in test_exceptions:
        try:
            error_details = classify_exception(exception)
            print(f"   {error_details.exception_class:20s} → {error_details.error_type.value:8s} | "
                  f"Retry: {str(error_details.should_retry):5s} | "
                  f"Action: {error_details.suggested_action[:30]}")
        except Exception as e:
            print(f"   {type(exception).__name__:20s} → ERROR | Could not classify: {e}")
    
    # Тест 3: Утиліти функції
    print(f"\n3. Utility Functions:")
    
    test_exception = ValueError("Test exception")
    
    # Тестуємо is_proxy_error
    is_proxy = is_proxy_error(test_exception)
    print(f"   is_proxy_error(ValueError) → {is_proxy}")
    
    # Тестуємо get_error_summary
    summary = get_error_summary(test_exception)
    print(f"   get_error_summary(ValueError) → '{summary}'")
    
    # Тестуємо should_retry_request
    should_retry = should_retry_request(test_exception)
    print(f"   should_retry_request(ValueError) → {should_retry}")
    
    # Тестуємо was_api_key_consumed
    was_consumed = was_api_key_consumed(test_exception)
    print(f"   was_api_key_consumed(ValueError) → {was_consumed}")
    
    # Тест 4: ErrorType та ErrorDetails
    print(f"\n4. Error Types and Details:")
    
    print("   Available ErrorTypes:")
    for error_type in ErrorType:
        print(f"     - {error_type.name:8s} = '{error_type.value}'")
    
    print(f"\n   ErrorDetails structure:")
    sample_details = classify_exception(ValueError("Sample error"))
    print(f"     error_type: {sample_details.error_type}")
    print(f"     exception_class: {sample_details.exception_class}")
    print(f"     error_message: {sample_details.error_message}")
    print(f"     should_retry: {sample_details.should_retry}")
    print(f"     api_key_consumed: {sample_details.api_key_consumed}")
    print(f"     suggested_action: {sample_details.suggested_action}")
    
    # Тест 5: Комбіновані тести
    print(f"\n5. Combined HTTP Status + Exception:")
    combined_tests = [
        (ValueError("Sample error"), 404),
        (ConnectionError("Connection failed"), 500),
        (None, 429),  # Тільки статус код
        (TimeoutError("Timeout"), None)  # Тільки виняток
    ]
    
    for exception, status in combined_tests:
        error_details = classify_exception(exception, status)
        exc_name = type(exception).__name__ if exception else "None"
        status_str = str(status) if status else "None"
        print(f"   {exc_name:15s} + HTTP {status_str:3s} → {error_details.error_type.value:8s} | "
              f"{error_details.exception_class}")
    
    print(f"\n=== Test completed ===")
    print(f"Module loaded successfully with {len([name for name in globals() if callable(globals()[name]) and not name.startswith('_')])} functions")
    print(f"Available ErrorTypes: {len(ErrorType)} types")
    print(f"Main function: classify_exception() with comprehensive error handling")
