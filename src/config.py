#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import time
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime
import os

logger = logging.getLogger("config_manager")

class ConfigManager:
    """
    Централізований менеджер конфігурацій з кешуванням та автоматичним оновленням.
    Забезпечує єдину точку доступу до всіх конфігураційних файлів проекту.
    """
    
    _mongo_config: Optional[Dict] = None
    _script_config: Optional[Dict] = None  
    _stage2_schema: Optional[Dict] = None
    
    _file_timestamps: Dict[str, float] = {}
    _last_file_check_time: Dict[str, float] = {}
    _file_check_interval = 30.0
    
    CONFIG_DIR = Path("config")
    MONGO_CONFIG_PATH = CONFIG_DIR / "mongo_config.json"
    SCRIPT_CONFIG_PATH = CONFIG_DIR / "script_control.json"  
    STAGE2_SCHEMA_PATH = CONFIG_DIR / "stage2_schema.json"
    
    DEFAULT_SCRIPT_CONFIG = {
        "enabled": True,
        "workers": {
            "concurrent_workers": 40
        },
        "timing": {
            "start_delay_ms": 700,
            "api_key_wait_time": 60,
            "domain_wait_time": 60,
            "max_concurrent_starts": 1
        },
        "stage_timings": {
            "stage1": {
                "model": "gemini-2.5-flash",
                "cooldown_minutes": 6,
                "api_provider": "gemini"
            },
            "stage2": {
                "model": "gemini-2.5-flash", 
                "retry_model": "gemini-2.5-flash",
                "cooldown_minutes": 6,
                "api_provider": "gemini"
            }
        }
    }
    
    @classmethod
    def _should_check_file(cls, file_path: Path) -> bool:
        """Визначає чи треба перевіряти файл (throttling)"""
        path_str = str(file_path)
        current_time = time.time()
        last_check = cls._last_file_check_time.get(path_str, 0)
        
        if current_time - last_check < cls._file_check_interval:
            return False
            
        cls._last_file_check_time[path_str] = current_time
        return True
    
    @classmethod
    def _check_file_changed(cls, file_path: Path) -> bool:
        """Перевіряє чи змінився файл з останнього завантаження"""
        if not file_path.exists():
            return True
            
        try:
            current_mtime = file_path.stat().st_mtime
            stored_mtime = cls._file_timestamps.get(str(file_path), 0)
            
            if current_mtime != stored_mtime:
                cls._file_timestamps[str(file_path)] = current_mtime
                return True
            return False
        except OSError:
            return True
    
    @classmethod
    def _check_file_changed_throttled(cls, file_path: Path) -> bool:
        """Throttled версія file change checking"""
        if not cls._should_check_file(file_path):
            return False
            
        return cls._check_file_changed(file_path)
    
    @classmethod
    def _load_json_file(cls, file_path: Path, config_name: str) -> Dict:
        """Базова функція завантаження JSON файлу з обробкою помилок"""
        try:
            if not file_path.exists():
                raise FileNotFoundError(f"{config_name} configuration file not found at {file_path}")
            
            with file_path.open("r", encoding="utf-8") as f:
                config = json.load(f)
                
            logger.debug(f"✓ Loaded {config_name} from {file_path}")
            return config
            
        except FileNotFoundError:
            raise
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {config_name} configuration file at {file_path}: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading {config_name} configuration: {e}")
    
    @classmethod
    def _create_default_script_config(cls) -> Dict:
        """Створює дефолтний script_control.json якщо файл не існує"""
        try:
            cls.CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            
            with cls.SCRIPT_CONFIG_PATH.open("w", encoding="utf-8") as f:
                json.dump(cls.DEFAULT_SCRIPT_CONFIG, f, indent=2)
            
            logger.info(f"✓ Created default script_control.json at {cls.SCRIPT_CONFIG_PATH}")
            return cls.DEFAULT_SCRIPT_CONFIG.copy()
            
        except Exception as e:
            logger.warning(f"Could not create default script_control.json: {e}")
            return cls.DEFAULT_SCRIPT_CONFIG.copy()
    
    @classmethod
    def get_mongo_config(cls, force_reload: bool = False) -> Dict:
        """
        Отримує конфігурацію MongoDB з кешуванням та throttling
        
        Args:
            force_reload: Примусове перезавантаження з диска
            
        Returns:
            Dict: MongoDB конфігурація
            
        Raises:
            FileNotFoundError: Якщо файл не знайдено
            ValueError: Якщо невалідний JSON
        """
        if force_reload or cls._mongo_config is None or cls._check_file_changed_throttled(cls.MONGO_CONFIG_PATH):
            cls._mongo_config = cls._load_json_file(cls.MONGO_CONFIG_PATH, "MongoDB")
            cls._validate_mongo_config(cls._mongo_config)
        
        return cls._mongo_config.copy()
    
    @classmethod
    def get_script_config(cls, force_reload: bool = False) -> Dict:
        """
        Отримує конфігурацію скрипта з кешуванням та throttling
        
        Args:
            force_reload: Примусове перезавантаження з диска
            
        Returns:
            Dict: Конфігурація скрипта
        """
        if force_reload or cls._script_config is None or cls._check_file_changed_throttled(cls.SCRIPT_CONFIG_PATH):
            try:
                cls._script_config = cls._load_json_file(cls.SCRIPT_CONFIG_PATH, "Script Control")
            except FileNotFoundError:
                cls._script_config = cls._create_default_script_config()
            
            cls._validate_script_config(cls._script_config)
        
        return cls._script_config.copy()
    
    @classmethod
    def get_stage2_schema(cls, force_reload: bool = False) -> Dict:
        """
        Отримує JSON схему для Stage2 з кешуванням та throttling
        
        Args:
            force_reload: Примусове перезавантаження з диска
            
        Returns:
            Dict: JSON схема для Stage2
            
        Raises:
            FileNotFoundError: Якщо файл не знайдено
            ValueError: Якщо невалідний JSON
        """
        if force_reload or cls._stage2_schema is None or cls._check_file_changed_throttled(cls.STAGE2_SCHEMA_PATH):
            cls._stage2_schema = cls._load_json_file(cls.STAGE2_SCHEMA_PATH, "Stage2 Schema")
            cls._validate_stage2_schema(cls._stage2_schema)
        
        return cls._stage2_schema.copy()
    
    @classmethod
    def get_all_configs(cls, force_reload: bool = False) -> Tuple[Dict, Dict, Dict]:
        """
        Отримує всі конфігурації одночасно
        
        Args:
            force_reload: Примусове перезавантаження всіх конфігів
            
        Returns:
            Tuple[Dict, Dict, Dict]: (mongo_config, script_config, stage2_schema)
        """
        mongo_config = cls.get_mongo_config(force_reload)
        script_config = cls.get_script_config(force_reload)
        stage2_schema = cls.get_stage2_schema(force_reload)
        
        return mongo_config, script_config, stage2_schema
    
    @classmethod
    def reload_all_configs(cls) -> None:
        """Примусово перезавантажує всі конфігурації"""
        cls._mongo_config = None
        cls._script_config = None
        cls._stage2_schema = None
        cls._file_timestamps.clear()
        cls._last_file_check_time.clear()
        
        logger.info("🔄 All configurations reloaded from disk")
    
    @classmethod
    def _validate_mongo_config(cls, config: Dict) -> None:
        """Валідує структуру MongoDB конфігурації"""
        required_keys = ["client_params", "databases"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key '{key}' in MongoDB configuration")
        
        databases = config.get("databases", {})
        if "main_db" not in databases:
            raise ValueError("Missing 'main_db' in MongoDB databases configuration")
        if "api_db" not in databases:
            raise ValueError("Missing 'api_db' in MongoDB databases configuration")
    
    @classmethod
    def _validate_script_config(cls, config: Dict) -> None:
        """Валідує структуру конфігурації скрипта"""
        if "workers" not in config:
            config["workers"] = cls.DEFAULT_SCRIPT_CONFIG["workers"].copy()
        if "timing" not in config:
            config["timing"] = cls.DEFAULT_SCRIPT_CONFIG["timing"].copy()
        if "stage_timings" not in config:
            config["stage_timings"] = cls.DEFAULT_SCRIPT_CONFIG["stage_timings"].copy()
        
        workers_count = config.get("workers", {}).get("concurrent_workers", 40)
        if not isinstance(workers_count, int) or workers_count < 1 or workers_count > 200:
            logger.warning(f"Invalid workers count {workers_count}, using default 40")
            config["workers"]["concurrent_workers"] = 40
    
    @classmethod
    def _validate_stage2_schema(cls, schema: Dict) -> None:
        """Валідує структуру JSON схеми для Stage2"""
        required_keys = ["type", "properties", "required"]
        for key in required_keys:
            if key not in schema:
                raise ValueError(f"Missing required key '{key}' in Stage2 schema")
        
        if schema.get("type") != "object":
            raise ValueError("Stage2 schema must be of type 'object'")
        
        properties_count = len(schema.get("properties", {}))
        if properties_count < 10:
            raise ValueError(f"Stage2 schema has too few properties: {properties_count}")
    
    @classmethod
    def is_script_enabled(cls) -> bool:
        """Швидка перевірка чи ввімкнений скрипт"""
        try:
            config = cls.get_script_config()
            return config.get("enabled", True)
        except Exception as e:
            logger.error(f"Error checking script status: {e}")
            return True
    
    @classmethod
    def get_stage_cooldown(cls, stage: str) -> int:
        """Отримує cooldown для конкретного stage"""
        try:
            config = cls.get_script_config()
            stage_config = config.get("stage_timings", {}).get(stage, {})
            return stage_config.get("cooldown_minutes", 6)
        except Exception:
            return 6
    
    @classmethod
    def get_stage_model(cls, stage: str) -> str:
        """Отримує модель для конкретного stage"""
        try:
            config = cls.get_script_config()
            stage_config = config.get("stage_timings", {}).get(stage, {})
            return stage_config.get("model", "gemini-2.5-flash")
        except Exception:
            return "gemini-2.5-flash"
    
    @classmethod
    def get_stage_retry_model(cls, stage: str) -> Optional[str]:
        """Отримує retry модель для конкретного stage"""
        try:
            config = cls.get_script_config()
            stage_config = config.get("stage_timings", {}).get(stage, {})
            return stage_config.get("retry_model")
        except Exception:
            return None
    
    @classmethod
    def get_db_collections(cls, db_name: str) -> Dict:
        """Отримує колекції для конкретної бази даних"""
        try:
            config = cls.get_mongo_config()
            return config.get("databases", {}).get(db_name, {}).get("collections", {})
        except Exception:
            return {}
    
    @classmethod
    def get_db_uri(cls, db_name: str) -> str:
        """Отримує URI для конкретної бази даних"""
        try:
            config = cls.get_mongo_config()
            return config.get("databases", {}).get(db_name, {}).get("uri", "")
        except Exception:
            return ""
    
    @classmethod
    def get_max_concurrent_starts(cls) -> int:
        """Отримує кількість одночасних стартів з конфігурації"""
        try:
            config = cls.get_script_config()
            return config.get("timing", {}).get("max_concurrent_starts", 1)
        except Exception:
            return 1
    
    @classmethod
    def get_concurrent_workers(cls) -> int:
        """Отримує кількість concurrent workers"""
        try:
            config = cls.get_script_config()
            return config.get("workers", {}).get("concurrent_workers", 40)
        except Exception:
            return 40
    
    @classmethod
    def get_timing_config(cls) -> Dict:
        """Отримує всю timing конфігурацію"""
        try:
            config = cls.get_script_config()
            return config.get("timing", cls.DEFAULT_SCRIPT_CONFIG["timing"])
        except Exception:
            return cls.DEFAULT_SCRIPT_CONFIG["timing"]
    
    @classmethod
    def get_client_params(cls) -> Dict:
        """Отримує параметри MongoDB клієнта"""
        try:
            config = cls.get_mongo_config()
            return config.get("client_params", {})
        except Exception:
            return {}
    
    @classmethod
    def get_config_summary(cls) -> Dict:
        """Отримує короткий summary всіх конфігурацій для логування"""
        try:
            mongo_config = cls.get_mongo_config()
            script_config = cls.get_script_config()
            stage2_schema = cls.get_stage2_schema()
            
            return {
                "script_enabled": script_config.get("enabled", True),
                "concurrent_workers": script_config.get("workers", {}).get("concurrent_workers", 40),
                "stage1_model": cls.get_stage_model("stage1"),
                "stage2_model": cls.get_stage_model("stage2"),
                "stage2_retry_model": cls.get_stage_retry_model("stage2"),
                "stage1_cooldown": cls.get_stage_cooldown("stage1"),
                "stage2_cooldown": cls.get_stage_cooldown("stage2"),
                "schema_fields_count": len(stage2_schema.get("properties", {})),
                "databases_configured": len(mongo_config.get("databases", {})),
                "file_check_interval": cls._file_check_interval
            }
        except Exception as e:
            return {"error": str(e)}


def get_mongo_config() -> Dict:
    """Глобальна функція для отримання MongoDB конфігурації"""
    return ConfigManager.get_mongo_config()

def get_script_config() -> Dict:
    """Глобальна функція для отримання конфігурації скрипта"""
    return ConfigManager.get_script_config()

def get_stage2_schema() -> Dict:
    """Глобальна функція для отримання Stage2 схеми"""
    return ConfigManager.get_stage2_schema()

def is_script_enabled() -> bool:
    """Глобальна функція для перевірки стану скрипта"""
    return ConfigManager.is_script_enabled()

def reload_configs() -> None:
    """Глобальна функція для перезавантаження конфігурацій"""
    ConfigManager.reload_all_configs()


if __name__ == "__main__":
    print("=== Optimized Config Manager Test Suite ===\n")
    
    try:
        print("1. Testing MongoDB Config:")
        mongo_config = ConfigManager.get_mongo_config()
        print(f"   ✓ MongoDB config loaded: {len(mongo_config)} keys")
        print(f"   ✓ Databases: {list(mongo_config.get('databases', {}).keys())}")
        
        print("\n2. Testing Script Config:")
        script_config = ConfigManager.get_script_config()
        print(f"   ✓ Script config loaded: {len(script_config)} keys")
        print(f"   ✓ Enabled: {script_config.get('enabled')}")
        print(f"   ✓ Workers: {script_config.get('workers', {}).get('concurrent_workers')}")
        
        print("\n3. Testing Stage2 Schema:")
        stage2_schema = ConfigManager.get_stage2_schema()
        print(f"   ✓ Stage2 schema loaded: {len(stage2_schema.get('properties', {}))} properties")
        print(f"   ✓ Required fields: {len(stage2_schema.get('required', []))}")
        
        print("\n4. Testing Throttling:")
        import time
        start_time = time.time()
        for i in range(100):
            ConfigManager.get_mongo_config()
        end_time = time.time()
        print(f"   ✓ 100 cached reads took: {(end_time - start_time):.4f}s")
        
        print("\n5. Config Summary:")
        summary = ConfigManager.get_config_summary()
        for key, value in summary.items():
            print(f"   📊 {key}: {value}")
        
        print(f"\n=== All tests passed! ===")
        print(f"🚀 OPTIMIZED ConfigManager with file throttling ({ConfigManager._file_check_interval}s interval)")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()