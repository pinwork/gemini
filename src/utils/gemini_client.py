#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import aiohttp
import asyncio
import ssl
import certifi
import time
from typing import Dict, Tuple, Optional
from aiohttp_socks import ProxyConnector, ProxyConnectionError, ProxyTimeoutError, ProxyError
from aiohttp import (
    ClientError, ClientConnectionError, ClientOSError, ClientConnectorError,
    ClientConnectorDNSError, ClientSSLError, ClientConnectorSSLError, 
    ClientConnectorCertificateError, ServerConnectionError, ServerDisconnectedError,
    ServerTimeoutError, ConnectionTimeoutError, SocketTimeoutError,
    ClientResponseError, ClientPayloadError, ClientConnectionResetError
)

try:
    from .proxy_config import ProxyConfig
    from .network_error_classifier import classify_exception
    from ..config import ConfigManager
except ImportError:
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent))
    from proxy_config import ProxyConfig
    from network_error_classifier import classify_exception
    # Для standalone тестування
    try:
        from config import ConfigManager
    except ImportError:
        ConfigManager = None

SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

CONNECT_TIMEOUT = 6
SOCK_CONNECT_TIMEOUT = 6
SOCK_READ_TIMEOUT = 240
TOTAL_TIMEOUT = 250
STAGE2_TIMEOUT_SECONDS = 90

DEFAULT_STAGE1_MODEL = "gemini-2.5-flash"
DEFAULT_STAGE2_MODEL = "gemini-2.0-flash"
DEFAULT_START_DELAY_MS = 700

_stage_timing = {
    "stage1": {"last_request_time": 0, "semaphore": None},
    "stage2": {"last_request_time": 0, "semaphore": None}
}


class GeminiAPIError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


def get_max_concurrent_starts() -> int:
    """Отримує MAX_CONCURRENT_STARTS з конфігурації або використовує дефолт"""
    if ConfigManager:
        try:
            return ConfigManager.get_max_concurrent_starts()
        except Exception:
            pass
    return 1  # Дефолтне значення


class GeminiClient:
    
    def __init__(self, 
                 stage1_model: str = DEFAULT_STAGE1_MODEL,
                 stage2_model: str = DEFAULT_STAGE2_MODEL,
                 stage2_retry_model: Optional[str] = None,
                 stage2_schema: Optional[dict] = None,
                 start_delay_ms: int = DEFAULT_START_DELAY_MS):
        self.stage1_model = stage1_model
        self.stage2_model = stage2_model
        self.stage2_retry_model = stage2_retry_model or stage2_model
        self.stage2_schema = stage2_schema or {}
        self.start_delay_ms = start_delay_ms
        
        # Отримуємо MAX_CONCURRENT_STARTS з конфігурації
        max_concurrent_starts = get_max_concurrent_starts()
        
        if _stage_timing["stage1"]["semaphore"] is None:
            _stage_timing["stage1"]["semaphore"] = asyncio.Semaphore(max_concurrent_starts)
        if _stage_timing["stage2"]["semaphore"] is None:
            _stage_timing["stage2"]["semaphore"] = asyncio.Semaphore(max_concurrent_starts)
    
    def format_api_error(self, raw_response: str) -> str:
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
    
    async def _enforce_request_interval(self, stage_name: str) -> None:
        stage_key = stage_name.lower()
        
        if stage_key not in _stage_timing:
            return
        
        async with _stage_timing[stage_key]["semaphore"]:
            current_time = time.time()
            last_time = _stage_timing[stage_key]["last_request_time"]
            time_since_last = current_time - last_time
            
            min_interval = self.start_delay_ms / 1000.0
            sleep_time = max(0, min_interval - time_since_last)
            
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            
            _stage_timing[stage_key]["last_request_time"] = time.time()
    
    async def _make_request(self, 
                           proxy_config: ProxyConfig, 
                           url: str, 
                           payload: dict,
                           stage_name: str,
                           timeout_seconds: Optional[int] = None) -> Tuple[aiohttp.ClientResponse, dict]:
        await self._enforce_request_interval(stage_name)
        
        if timeout_seconds:
            timeout = aiohttp.ClientTimeout(
                total=timeout_seconds + 10,
                connect=CONNECT_TIMEOUT,
                sock_connect=SOCK_CONNECT_TIMEOUT,
                sock_read=timeout_seconds
            )
        else:
            timeout = aiohttp.ClientTimeout(
                total=TOTAL_TIMEOUT,
                connect=CONNECT_TIMEOUT,
                sock_connect=SOCK_CONNECT_TIMEOUT,
                sock_read=SOCK_READ_TIMEOUT
            )
        
        headers = {"Content-Type": "application/json"}
        
        connector_params = proxy_config.get_connection_params()
        connector_params.update({
            'ssl': SSL_CONTEXT,
            'rdns': True
        })
        
        connector = ProxyConnector(**connector_params)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, json=payload, headers=headers, timeout=timeout) as response:
                try:
                    resp_json = await response.json()
                    return response, resp_json
                except aiohttp.ContentTypeError:
                    resp_text = await response.text()
                    if len(resp_text) > 512:
                        resp_text = resp_text[:512] + "...[truncated]"
                    return response, resp_text
    
    def _parse_stage1_response(self, response_data: dict) -> Tuple[str, str]:
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
    
    def _build_stage1_payload(self, domain_full: str, stage1_prompt: str, use_google_search: bool = True) -> dict:
        user_message = f"Analyze website https://{domain_full}\n\n{stage1_prompt}"
        
        tools = [{"urlContext": {}}]
        
        if use_google_search:
            tools.append({"googleSearch": {}})
        
        return {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": user_message}
                    ]
                }
            ],
            "tools": tools,
            "generationConfig": {
                "temperature": 0.3
            }
        }
    
    def _build_stage2_payload(self, domain_full: str, text_content: str, system_prompt: str) -> dict:
        user_message = f"Analyze content review of website {domain_full}: {text_content}"
        
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
                "responseMimeType": "application/json"
            },
            "systemInstruction": {
                "parts": [
                    {"text": system_prompt}
                ]
            }
        }
        
        if self.stage2_schema:
            payload["generationConfig"]["responseSchema"] = self.stage2_schema
        
        return payload
    
    async def analyze_content(self, 
                            domain_full: str, 
                            api_key: str, 
                            proxy_config: ProxyConfig,
                            stage1_prompt: str,
                            use_google_search: bool = True) -> dict:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.stage1_model}:generateContent?key={api_key}"
        payload = self._build_stage1_payload(domain_full, stage1_prompt, use_google_search)
        
        start_time = asyncio.get_event_loop().time()
        
        try:
            response, resp_data = await self._make_request(
                proxy_config, url, payload, "stage1"
            )
            
            end_time = asyncio.get_event_loop().time()
            response_time = end_time - start_time
            
            if response.status == 200:
                if isinstance(resp_data, dict):
                    grounding_status, text_response = self._parse_stage1_response(resp_data)
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
                formatted_error = self.format_api_error(str(resp_data))
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
                    formatted_error = self.format_api_error(json_part)
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
    
    async def analyze_business(self, 
                             domain_full: str, 
                             text_content: str, 
                             api_key: str, 
                             proxy_config: ProxyConfig,
                             system_prompt: str,
                             use_retry_model: bool = False) -> dict:
        model_to_use = self.stage2_retry_model if use_retry_model else self.stage2_model
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_to_use}:generateContent?key={api_key}"
        payload = self._build_stage2_payload(domain_full, text_content, system_prompt)
        
        start_time = asyncio.get_event_loop().time()

        try:
            response, resp_data = await self._make_request(
                proxy_config, url, payload, "stage2", STAGE2_TIMEOUT_SECONDS
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
                    return {
                        "success": False, 
                        "status_code": 200, 
                        "response_time": response_time, 
                        "error": "No candidates in response"
                    }
                
                content = candidates[0].get("content", {})
                parts = content.get("parts", [])
                if not parts:
                    return {
                        "success": False, 
                        "status_code": 200, 
                        "response_time": response_time, 
                        "error": "No parts in content"
                    }
                
                text = parts[0].get("text")
                if not text:
                    return {
                        "success": False, 
                        "status_code": 200, 
                        "response_time": response_time, 
                        "error": "No text in parts"
                    }
                
                try:
                    parsed_result = json.loads(text)
                    return {
                        "success": True, 
                        "status_code": 200, 
                        "response_time": response_time, 
                        "result": parsed_result
                    }
                except json.JSONDecodeError:
                    return {
                        "success": False, 
                        "status_code": 200, 
                        "response_time": response_time, 
                        "error": "Invalid JSON in response"
                    }
                            
            else:
                formatted_error = self.format_api_error(str(resp_data))
                return {
                    "success": False, 
                    "status_code": response.status, 
                    "response_time": response_time, 
                    "error": f"HTTP {response.status}: {formatted_error}"
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
                    formatted_error = self.format_api_error(json_part)
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
    
    async def test_connection(self, api_key: str, proxy_config: ProxyConfig, use_google_search: bool = True) -> dict:
        test_prompt = "Say 'Hello, Gemini API is working!' in exactly these words."
        
        try:
            result = await self.analyze_content(
                "google.com", 
                api_key, 
                proxy_config, 
                test_prompt,
                use_google_search
            )
            
            if result["success"] and "Hello, Gemini API is working!" in result.get("text_response", ""):
                return {"success": True, "message": "API connection successful"}
            else:
                return {"success": False, "message": f"API test failed: {result.get('error', 'Unknown error')}"}
                
        except Exception as e:
            return {"success": False, "message": f"Connection test failed: {str(e)}"}
    
    def get_usage_stats(self) -> dict:
        max_concurrent_starts = get_max_concurrent_starts()
        
        return {
            "stage1_model": self.stage1_model,
            "stage2_model": self.stage2_model,
            "stage2_retry_model": self.stage2_retry_model,
            "has_schema": bool(self.stage2_schema),
            "schema_fields": len(self.stage2_schema.get("properties", {})) if self.stage2_schema else 0,
            "stage1_features": ["urlContext", "googleSearch"],
            "stage2_features": ["JSON_schema", "systemInstruction"],
            "timing_intervals": {
                "start_delay_ms": self.start_delay_ms,
                "max_concurrent_starts": max_concurrent_starts
            }
        }


def create_gemini_client(stage2_schema: Optional[dict] = None, 
                        start_delay_ms: int = DEFAULT_START_DELAY_MS,
                        stage2_retry_model: Optional[str] = None) -> GeminiClient:
    return GeminiClient(
        stage1_model=DEFAULT_STAGE1_MODEL,
        stage2_model=DEFAULT_STAGE2_MODEL,
        stage2_retry_model=stage2_retry_model,
        stage2_schema=stage2_schema,
        start_delay_ms=start_delay_ms
    )


def create_custom_gemini_client(stage1_model: str, 
                               stage2_model: str, 
                               stage2_retry_model: Optional[str] = None,
                               stage2_schema: Optional[dict] = None,
                               start_delay_ms: int = DEFAULT_START_DELAY_MS) -> GeminiClient:
    return GeminiClient(
        stage1_model=stage1_model,
        stage2_model=stage2_model,
        stage2_retry_model=stage2_retry_model,
        stage2_schema=stage2_schema,
        start_delay_ms=start_delay_ms
    )


if __name__ == "__main__":
    import asyncio
    from pathlib import Path
    import json
    import sys
    
    async def test_with_mongo_credentials():
        print("=== Gemini Client Auto Test (MongoDB credentials) ===\n")
        
        try:
            from mongo_operations import get_api_key_and_proxy
            from motor.motor_asyncio import AsyncIOMotorClient
            
            config_path = Path(__file__).parent.parent.parent / "config" / "mongo_config.json"
            with config_path.open("r", encoding="utf-8") as f:
                mongo_config = json.load(f)
            
            api_db_uri = mongo_config["databases"]["main_db"]["uri"]
            client_params = mongo_config["client_params"]
            mongo_client = AsyncIOMotorClient(api_db_uri, **client_params)
            
            print("✓ Connected to MongoDB")
            
            api_key, proxy_config, key_record_id, key_rec = await get_api_key_and_proxy(mongo_client)
            print(f"✓ Got API key: {api_key[:8]}...{api_key[-4:]}")
            print(f"✓ Got proxy: {proxy_config.connection_string}")
            
            mongo_client.close()
            
            return api_key, proxy_config
            
        except Exception as e:
            print(f"❌ MongoDB error: {e}")
            print("💡 Make sure MongoDB is running and config is correct")
            return None, None
    
    async def test_with_manual_credentials():
        print("=== Gemini Client Manual Test (CLI parameters) ===\n")
        
        if len(sys.argv) < 4:
            print("❌ Not enough parameters for manual test")
            print("Usage: python gemini_client.py <API_KEY> <PROXY_PROTOCOL> <PROXY_IP:PORT> [USERNAME:PASSWORD]")
            return None, None
        
        api_key = sys.argv[1]
        proxy_protocol = sys.argv[2]
        proxy_address = sys.argv[3]
        proxy_auth = sys.argv[4] if len(sys.argv) > 4 else None
        
        try:
            if ":" in proxy_address:
                proxy_ip, proxy_port = proxy_address.split(":", 1)
                proxy_port = int(proxy_port)
            else:
                print("❌ Invalid proxy format. Use IP:PORT")
                return None, None
        except ValueError:
            print("❌ Invalid proxy port. Must be integer")
            return None, None
        
        proxy_username = None
        proxy_password = None
        if proxy_auth and ":" in proxy_auth:
            proxy_username, proxy_password = proxy_auth.split(":", 1)
        
        try:
            proxy_config = ProxyConfig(
                protocol=proxy_protocol,
                ip=proxy_ip,
                port=proxy_port,
                username=proxy_username,
                password=proxy_password
            )
            print(f"✓ Manual proxy config: {proxy_config.connection_string}")
            return api_key, proxy_config
        except Exception as e:
            print(f"❌ Proxy config error: {e}")
            return None, None
    
    async def run_gemini_test(api_key, proxy_config):
        try:
            schema_path = Path(__file__).parent.parent.parent / "config" / "stage2_schema.json"
            with schema_path.open("r", encoding="utf-8") as f:
                schema = json.load(f)
            print(f"✓ Schema loaded: {len(schema.get('properties', {}))} fields")
        except Exception as e:
            print(f"⚠ Could not load schema: {e}")
            schema = {}
        
        client = create_gemini_client(schema, start_delay_ms=500)
        print(f"✓ GeminiClient created with {client.start_delay_ms}ms delay")
        
        # Показуємо конфігурований MAX_CONCURRENT_STARTS
        max_concurrent = get_max_concurrent_starts()
        print(f"🔧 Max concurrent starts: {max_concurrent} (from config)")
        
        test_domain = "shopify.com"
        stage1_prompt = "Analyze this website and provide detailed information about its content, purpose, and functionality."
        
        print(f"\n🔍 Testing domain: {test_domain}")
        print("=" * 60)
        
        print("\n📖 STAGE 1 - Content Analysis:")
        print("-" * 40)
        
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{client.stage1_model}:generateContent?key={api_key}"
            payload = client._build_stage1_payload(test_domain, stage1_prompt)
            
            print("🔧 Making raw Stage1 request...")
            start_time = asyncio.get_event_loop().time()
            
            response, resp_data = await client._make_request(
                proxy_config, url, payload, "stage1"
            )
            
            end_time = asyncio.get_event_loop().time()
            response_time = end_time - start_time
            
            print(f"Status: {'✓ SUCCESS' if response.status == 200 else '❌ FAILED'}")
            print(f"Response time: {response_time:.2f}s")
            print(f"Status code: {response.status}")
            
            if response.status == 200 and isinstance(resp_data, dict):
                print(f"\n📄 RAW STAGE1 RESPONSE (Full API Response):")
                print("=" * 50)
                print(json.dumps(resp_data, indent=2, ensure_ascii=False))
                print("=" * 50)
                
                candidates = resp_data.get("candidates", [])
                if candidates:
                    candidate = candidates[0]
                    text_response = candidate.get("content", {}).get("parts", [{}])[0].get("text", "")
                    url_metadata = candidate.get("urlContextMetadata", {}).get("urlMetadata", [])
                    grounding_status = url_metadata[0].get("urlRetrievalStatus", "UNKNOWN") if url_metadata else "NO_URL_METADATA"
                    
                    print(f"🎯 Grounding status: {grounding_status}")
                    
                    if text_response:
                        print(f"\n📄 EXTRACTED TEXT FROM STAGE1:")
                        print("=" * 50)
                        print(text_response)
                        print("=" * 50)
                        
                        print("\n🧠 STAGE 2 - Business Analysis:")
                        print("-" * 40)
                        
                        system_prompt = """You are a website analyzer. Analyze the provided content and return structured business information in JSON format according to the provided schema."""
                        
                        try:
                            url = f"https://generativelanguage.googleapis.com/v1beta/models/{client.stage2_model}:generateContent?key={api_key}"
                            payload = client._build_stage2_payload(test_domain, text_response, system_prompt)
                            
                            print("🔧 Making raw Stage2 request...")
                            start_time = asyncio.get_event_loop().time()
                            
                            response, resp_data = await client._make_request(
                                proxy_config, url, payload, "stage2", STAGE2_TIMEOUT_SECONDS
                            )
                            
                            end_time = asyncio.get_event_loop().time()
                            response_time = end_time - start_time
                            
                            print(f"Status: {'✓ SUCCESS' if response.status == 200 else '❌ FAILED'}")
                            print(f"Response time: {response_time:.2f}s")
                            print(f"Status code: {response.status}")
                            
                            if response.status == 200 and isinstance(resp_data, dict):
                                print(f"\n📊 RAW STAGE2 RESPONSE (Full API Response):")
                                print("=" * 50)
                                print(json.dumps(resp_data, indent=2, ensure_ascii=False))
                                print("=" * 50)
                                
                                candidates = resp_data.get("candidates", [])
                                if candidates:
                                    content = candidates[0].get("content", {})
                                    parts = content.get("parts", [])
                                    if parts:
                                        raw_json_text = parts[0].get("text", "")
                                        print(f"\n🎯 EXTRACTED JSON FROM STAGE2:")
                                        print("=" * 50)
                                        print(raw_json_text)
                                        print("=" * 50)
                                        
                                        try:
                                            parsed_json = json.loads(raw_json_text)
                                            print(f"\n✅ JSON parsing: SUCCESS ({len(parsed_json)} fields)")
                                        except json.JSONDecodeError as e:
                                            print(f"❌ JSON parsing FAILED: {e}")
                                    else:
                                        print(f"❌ No parts in content")
                                else:
                                    print(f"❌ No candidates in response")
                            else:
                                print(f"❌ Stage2 HTTP error: {response.status}")
                                if isinstance(resp_data, str):
                                    print(f"Response: {resp_data[:500]}...")
                            
                        except Exception as e:
                            print(f"❌ Stage2 exception: {e}")
                    else:
                        print(f"❌ No text content in Stage1 response")
                else:
                    print(f"❌ No candidates in Stage1 response")
            else:
                print(f"❌ Stage1 HTTP error: {response.status}")
                if isinstance(resp_data, str):
                    print(f"Response: {resp_data[:500]}...")
                
        except Exception as e:
            print(f"❌ Stage1 exception: {e}")
        
        print(f"\n" + "=" * 60)
        print("🎯 Test completed!")
        print("📝 This test shows RAW responses from Gemini API before any processing")
        print(f"⏱️  Used custom delay: {client.start_delay_ms}ms (configurable)")
        print(f"🔧 Max concurrent starts: {max_concurrent} (configurable)")
    
    async def main_test():
        if len(sys.argv) == 1:
            api_key, proxy_config = await test_with_mongo_credentials()
        else:
            api_key, proxy_config = await test_with_manual_credentials()
        
        if api_key and proxy_config:
            await run_gemini_test(api_key, proxy_config)
        else:
            print("\n💡 Usage options:")
            print("1. Auto mode: python gemini_client.py")
            print("   (uses random API key + proxy from MongoDB)")
            print("2. Manual mode: python gemini_client.py <API_KEY> <PROXY_PROTOCOL> <PROXY_IP:PORT> [USERNAME:PASSWORD]")
            print("   Example: python gemini_client.py AIz... http 1.2.3.4:8080 user:pass")
    
    asyncio.run(main_test())