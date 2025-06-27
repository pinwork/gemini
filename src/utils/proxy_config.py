#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import random
import ipaddress
from typing import Optional
from aiohttp_socks import ProxyType

# Мапінг протоколів проксі на типи aiohttp-socks
PROXY_PROTOCOL_MAP = {
    'http': ProxyType.HTTP,
    'https': ProxyType.HTTP,
    'socks4': ProxyType.SOCKS4,
    'socks5': ProxyType.SOCKS5
}


class ProxyConfig:
    """
    Конфігурація проксі-сервера з валідацією та утилітами для роботи з session ID
    
    Підтримує HTTP, HTTPS, SOCKS4, SOCKS5 проксі з аутентифікацією та без неї.
    Автоматично генерує нові session ID для ротації проксі сесій.
    """
    
    def __init__(self, protocol: str, ip: str, port: int, 
                 username: Optional[str] = None, password: Optional[str] = None):
        """
        Ініціалізує конфігурацію проксі
        
        Args:
            protocol: Протокол проксі (http, https, socks4, socks5)
            ip: IP адреса або домен проксі-сервера
            port: Порт проксі-сервера (1-65535)
            username: Ім'я користувача для аутентифікації (опціонально)
            password: Пароль для аутентифікації (опціонально)
            
        Raises:
            ValueError: При невалідних параметрах
        """
        self.protocol = protocol.lower()
        self.ip = ip
        self.port = port
        self.username = username if username else None
        self.password = password if password else None
        
        # Валідація параметрів
        if self.protocol not in PROXY_PROTOCOL_MAP:
            raise ValueError(f"Unsupported proxy protocol: {protocol}. Supported: {list(PROXY_PROTOCOL_MAP.keys())}")
        if not (1 <= port <= 65535):
            raise ValueError(f"Invalid port: {port}. Must be between 1 and 65535")
        self._validate_ip_or_domain(ip)
    
    def _validate_ip_or_domain(self, ip: str) -> None:
        """
        Валідує IP адресу або доменне ім'я
        
        Args:
            ip: IP адреса або домен для валідації
            
        Raises:
            ValueError: При невалідній IP адресі або домені
        """
        # Спробуємо спарсити як IPv4 адресу
        try:
            ipaddress.IPv4Address(ip)
            return  # Валідна IPv4 адреса
        except ipaddress.AddressValueError:
            pass
        
        # Спробуємо спарсити як IPv6 адресу
        try:
            ipaddress.IPv6Address(ip)
            return  # Валідна IPv6 адреса
        except ipaddress.AddressValueError:
            pass
        
        # Валідація доменного імені
        domain_pattern = re.compile(
            r'^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$'
        )
        if not domain_pattern.match(ip):
            raise ValueError(f"Invalid IP address or domain: {ip}")
    
    @property
    def proxy_type(self) -> ProxyType:
        """Повертає тип проксі для aiohttp-socks"""
        return PROXY_PROTOCOL_MAP[self.protocol]
    
    @property
    def has_auth(self) -> bool:
        """Перевіряє чи налаштована аутентифікація"""
        return bool(self.username and self.password)
    
    @property
    def connection_string(self) -> str:
        """
        Повертає connection string для логування (з прихованим паролем)
        
        Returns:
            Форматований connection string з замаскованим паролем
        """
        if self.has_auth:
            return f"{self.protocol}://{self.username}:***@{self.ip}:{self.port}"
        return f"{self.protocol}://{self.ip}:{self.port}"
    
    @property
    def full_url(self) -> str:
        """
        Повертає повний URL проксі для підключення (з реальними credentials)
        
        Returns:
            Повний URL проксі включаючи credentials
        """
        if self.has_auth:
            return f"{self.protocol}://{self.username}:{self.password}@{self.ip}:{self.port}"
        return f"{self.protocol}://{self.ip}:{self.port}"
    
    def has_sessid(self) -> bool:
        """
        Перевіряє чи містить username session ID для ротації
        
        Returns:
            True якщо username містить -sessid- pattern
        """
        return "-sessid-" in self.username.lower() if self.username else False
    
    def generate_new_sessid(self) -> 'ProxyConfig':
        """
        Генерує новий ProxyConfig з оновленим session ID
        
        Якщо username містить -sessid-, замінює останні 4 символи на нові випадкові цифри.
        Якщо session ID відсутній, повертає копію поточної конфігурації.
        
        Returns:
            Новий ProxyConfig з оновленим session ID
        """
        if not self.has_sessid():
            return ProxyConfig(
                protocol=self.protocol,
                ip=self.ip,
                port=self.port,
                username=self.username,
                password=self.password
            )
        
        # Генеруємо новий 4-значний суфікс
        new_suffix = ''.join([str(random.randint(0, 9)) for _ in range(4)])
        new_username = self.username[:-4] + new_suffix
        
        return ProxyConfig(
            protocol=self.protocol,
            ip=self.ip,
            port=self.port,
            username=new_username,
            password=self.password
        )
    
    def get_connection_params(self) -> dict:
        """
        Повертає параметри для створення ProxyConnector
        
        Returns:
            Словник з параметрами для aiohttp-socks ProxyConnector
        """
        params = {
            'proxy_type': self.proxy_type,
            'host': self.ip,
            'port': self.port,
        }
        
        if self.has_auth:
            params['username'] = self.username
            params['password'] = self.password
            
        return params
    
    def test_different_ports(self, port_list: list) -> list:
        """
        Створює список ProxyConfig з різними портами для тестування
        
        Args:
            port_list: Список портів для тестування
            
        Returns:
            Список ProxyConfig об'єктів з різними портами
        """
        configs = []
        for port in port_list:
            try:
                config = ProxyConfig(
                    protocol=self.protocol,
                    ip=self.ip,
                    port=port,
                    username=self.username,
                    password=self.password
                )
                configs.append(config)
            except ValueError:
                continue  # Пропускаємо невалідні порти
        return configs
    
    def __str__(self) -> str:
        """Повертає connection string для логування"""
        return self.connection_string
    
    def __repr__(self) -> str:
        """Повертає детальне представлення об'єкта"""
        return (f"ProxyConfig(protocol='{self.protocol}', ip='{self.ip}', "
                f"port={self.port}, has_auth={self.has_auth}, has_sessid={self.has_sessid()})")
    
    def __eq__(self, other) -> bool:
        """Порівняння двох ProxyConfig об'єктів"""
        if not isinstance(other, ProxyConfig):
            return False
        return (self.protocol == other.protocol and 
                self.ip == other.ip and 
                self.port == other.port and
                self.username == other.username and
                self.password == other.password)


def create_proxy_from_url(proxy_url: str) -> ProxyConfig:
    """
    Створює ProxyConfig з URL строки
    
    Args:
        proxy_url: URL проксі у форматі protocol://[username:password@]host:port
        
    Returns:
        ProxyConfig об'єкт
        
    Raises:
        ValueError: При невалідному URL
        
    Examples:
        >>> create_proxy_from_url("http://proxy.example.com:8080")
        >>> create_proxy_from_url("socks5://user:pass@1.2.3.4:1080")
    """
    import urllib.parse
    
    parsed = urllib.parse.urlparse(proxy_url)
    
    if not parsed.scheme:
        raise ValueError(f"Missing protocol in proxy URL: {proxy_url}")
    if not parsed.hostname:
        raise ValueError(f"Missing hostname in proxy URL: {proxy_url}")
    if not parsed.port:
        raise ValueError(f"Missing port in proxy URL: {proxy_url}")
    
    return ProxyConfig(
        protocol=parsed.scheme,
        ip=parsed.hostname,
        port=parsed.port,
        username=parsed.username,
        password=parsed.password
    )


def validate_proxy_list(proxy_configs: list) -> tuple:
    """
    Валідує список ProxyConfig об'єктів
    
    Args:
        proxy_configs: Список ProxyConfig об'єктів для валідації
        
    Returns:
        Кортеж (valid_configs, invalid_configs, error_messages)
    """
    valid_configs = []
    invalid_configs = []
    error_messages = []
    
    for i, config in enumerate(proxy_configs):
        try:
            # Перевіряємо що це дійсно ProxyConfig
            if not isinstance(config, ProxyConfig):
                raise ValueError(f"Item {i} is not a ProxyConfig instance")
            
            # Перевіряємо базові параметри
            if not config.ip or not config.port:
                raise ValueError(f"Missing IP or port in config {i}")
                
            valid_configs.append(config)
            
        except Exception as e:
            invalid_configs.append(config)
            error_messages.append(f"Config {i}: {str(e)}")
    
    return valid_configs, invalid_configs, error_messages


if __name__ == "__main__":
    # Тестування proxy_config модуля
    print("=== Proxy Configuration Test Suite ===\n")
    
    # Тест 1: Створення різних типів проксі
    print("1. Creating Different Proxy Types:")
    
    test_configs = [
        ("HTTP без auth", "http", "proxy1.example.com", 8080, None, None),
        ("HTTPS з auth", "https", "proxy2.example.com", 3128, "user1", "pass1"),
        ("SOCKS4", "socks4", "1.2.3.4", 1080, None, None),
        ("SOCKS5 з sessid", "socks5", "proxy.provider.com", 1080, "user-sessid-1234", "password"),
        ("IPv6 proxy", "http", "2001:db8::1", 8080, None, None),
    ]
    
    configs = []
    for name, protocol, ip, port, username, password in test_configs:
        try:
            config = ProxyConfig(protocol, ip, port, username, password)
            configs.append(config)
            print(f"   ✓ {name:15s} → {config.connection_string}")
        except Exception as e:
            print(f"   ✗ {name:15s} → ERROR: {e}")
    
    # Тест 2: Властивості ProxyConfig
    print(f"\n2. ProxyConfig Properties:")
    if configs:
        config = configs[1]  # HTTPS з auth
        print(f"   Protocol: {config.protocol}")
        print(f"   Proxy Type: {config.proxy_type}")
        print(f"   Has Auth: {config.has_auth}")
        print(f"   Has Session ID: {config.has_sessid()}")
        print(f"   Connection String: {config.connection_string}")
        print(f"   Full URL: {config.full_url}")
        print(f"   Connection Params: {config.get_connection_params()}")
    
    # Тест 3: Session ID генерація
    print(f"\n3. Session ID Generation:")
    sessid_configs = [config for config in configs if config.has_sessid()]
    if sessid_configs:
        original = sessid_configs[0]
        print(f"   Original: {original.username}")
        
        for i in range(3):
            new_config = original.generate_new_sessid()
            print(f"   New #{i+1}:  {new_config.username}")
            original = new_config
    else:
        print("   No configs with session ID found for testing")
    
    # Тест 4: Валідація помилок
    print(f"\n4. Error Validation:")
    error_tests = [
        ("Invalid protocol", "ftp", "1.2.3.4", 8080),
        ("Invalid port", "http", "1.2.3.4", 99999),
        ("Invalid IP", "http", "999.999.999.999", 8080),
        ("Invalid domain", "http", "invalid..domain", 8080),
    ]
    
    for name, protocol, ip, port in error_tests:
        try:
            ProxyConfig(protocol, ip, port)
            print(f"   ✗ {name:20s} → Should have failed!")
        except ValueError as e:
            print(f"   ✓ {name:20s} → Correctly caught: {str(e)[:50]}...")
    
    # Тест 5: Створення з URL
    print(f"\n5. Creating from URL:")
    test_urls = [
        "http://proxy.example.com:8080",
        "socks5://user:pass@1.2.3.4:1080",
        "https://proxy.com:3128",
        "invalid-url-without-protocol",
    ]
    
    for url in test_urls:
        try:
            config = create_proxy_from_url(url)
            print(f"   ✓ {url:35s} → {config}")
        except Exception as e:
            print(f"   ✗ {url:35s} → ERROR: {e}")
    
    # Тест 6: Валідація списку
    print(f"\n6. Proxy List Validation:")
    valid_configs, invalid_configs, errors = validate_proxy_list(configs)
    print(f"   Total configs: {len(configs)}")
    print(f"   Valid configs: {len(valid_configs)}")
    print(f"   Invalid configs: {len(invalid_configs)}")
    if errors:
        for error in errors:
            print(f"   Error: {error}")
    
    # Тест 7: Порти тестування
    print(f"\n7. Different Ports Testing:")
    if configs:
        base_config = configs[0]
        test_ports = [8080, 3128, 8888, 1080]
        port_configs = base_config.test_different_ports(test_ports)
        print(f"   Base: {base_config.ip}:{base_config.port}")
        for port_config in port_configs:
            print(f"   Test: {port_config.ip}:{port_config.port}")
    
    print(f"\n=== Test completed ===")
    print(f"Available protocols: {list(PROXY_PROTOCOL_MAP.keys())}")
    print(f"Module loaded successfully with {len([name for name in globals() if callable(globals()[name]) and not name.startswith('_')])} functions")
    print(f"ProxyConfig class ready for use with {len([attr for attr in dir(ProxyConfig) if not attr.startswith('_')])} public methods/properties")
