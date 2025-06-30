#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple
from motor.motor_asyncio import AsyncIOMotorClient

try:
    from ..config import ConfigManager
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent.parent))
    from config import ConfigManager

logger = logging.getLogger("adaptive_delay_manager")

class AdaptiveDelayManager:
    
    @staticmethod
    async def collect_global_stats(mongo_client: AsyncIOMotorClient) -> Tuple[int, int, int]:
        try:
            config = ConfigManager.get_mongo_config()
            api_db_name = config["databases"]["api_db"]["name"]
            api_collection_name = config["databases"]["api_db"]["collections"]["keys"]
            
            api_keys_collection = mongo_client[api_db_name][api_collection_name]
            
            pipeline = [
                {
                    "$match": {
                        "api_status": "active",
                        "api_provider": "gemini"
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_200": {"$sum": "$request_count_200"},
                        "total_429": {"$sum": "$request_count_429"},
                        "key_count": {"$sum": 1}
                    }
                }
            ]
            
            result = await api_keys_collection.aggregate(pipeline).to_list(1)
            
            if result:
                data = result[0]
                return data.get("total_200", 0), data.get("total_429", 0), data.get("key_count", 0)
            else:
                return 0, 0, 0
                
        except Exception as e:
            logger.error(f"Error collecting global stats: {e}")
            return 0, 0, 0
    
    @staticmethod
    def calculate_success_rate(total_200: int, total_429: int) -> float:
        total_requests = total_200 + total_429
        if total_requests == 0:
            return 100.0
        return (total_200 / total_requests) * 100.0
    
    @staticmethod
    async def reset_all_gemini_counters(mongo_client: AsyncIOMotorClient) -> int:
        try:
            config = ConfigManager.get_mongo_config()
            api_db_name = config["databases"]["api_db"]["name"]
            api_collection_name = config["databases"]["api_db"]["collections"]["keys"]
            
            api_keys_collection = mongo_client[api_db_name][api_collection_name]
            
            result = await api_keys_collection.update_many(
                {
                    "api_status": "active",
                    "api_provider": "gemini"
                },
                {
                    "$set": {
                        "request_count_200": 0,
                        "request_count_429": 0,
                        "stats_window_start": datetime.now(timezone.utc)
                    }
                }
            )
            
            return result.modified_count
            
        except Exception as e:
            logger.error(f"Error resetting counters: {e}")
            return 0
    
    @staticmethod
    def update_current_delay(new_delay: int) -> bool:
        try:
            config_path = Path("config/script_control.json")
            
            with config_path.open("r", encoding="utf-8") as f:
                config = json.load(f)
            
            if "adaptive_delay" not in config:
                config["adaptive_delay"] = {}
            
            config["adaptive_delay"]["current_delay_ms"] = new_delay
            config["adaptive_delay"]["last_evaluation"] = datetime.now(timezone.utc).isoformat()
            
            with config_path.open("w", encoding="utf-8") as f:
                json.dump(config, f, indent=2)
            
            ConfigManager.reload_all_configs()
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating delay config: {e}")
            return False
    
    @staticmethod
    async def evaluate_and_adjust(mongo_client: AsyncIOMotorClient, adaptive_logger: logging.Logger) -> None:
        try:
            total_200, total_429, key_count = await AdaptiveDelayManager.collect_global_stats(mongo_client)
            
            success_rate = AdaptiveDelayManager.calculate_success_rate(total_200, total_429)
            
            config = ConfigManager.get_script_config()
            adaptive_config = config.get("adaptive_delay", {})
            
            current_delay = adaptive_config.get("current_delay_ms", 700)
            step_ms = adaptive_config.get("step_ms", 20)
            min_delay = adaptive_config.get("min_delay_ms", 0)
            
            new_delay = max(current_delay - step_ms, min_delay)
            
            adaptive_logger.info(
                f"EVALUATION | Gemini Keys: {key_count} | Total 200: {total_200} | Total 429: {total_429} | "
                f"Success Rate: {success_rate:.1f}% | {current_delay}ms → {new_delay}ms"
            )
            
            AdaptiveDelayManager.update_current_delay(new_delay)
            
            reset_count = await AdaptiveDelayManager.reset_all_gemini_counters(mongo_client)
            
            adaptive_logger.info(f"RESET counters for {reset_count} Gemini keys")
            
        except Exception as e:
            logger.error(f"Error in evaluate_and_adjust: {e}")
    
    @staticmethod
    async def startup_reset(mongo_client: AsyncIOMotorClient, startup_logger: logging.Logger) -> int:
        try:
            reset_count = await AdaptiveDelayManager.reset_all_gemini_counters(mongo_client)
            
            startup_logger.info(f"STARTUP RESET | Gemini Keys: {reset_count} | Counters cleared for clean slate")
            
            return reset_count
            
        except Exception as e:
            logger.error(f"Error in startup reset: {e}")
            return 0
    
    @staticmethod
    def get_current_delay_ms() -> int:
        try:
            config = ConfigManager.get_script_config()
            return config.get("adaptive_delay", {}).get("current_delay_ms", 700)
        except Exception:
            return 700