#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mongo_operations_module.py
----------------------------------------------------------------
• Єдиний репозиторій Mongo-операцій для main.py  
• Параметри БД/колекцій і клієнта читаються тільки з config/mongo_config.json  
• Запущений напряму → smoke-тест: показує лише описані в конфігу колекції
  і JSON першого документа.
"""

from __future__ import annotations

import asyncio, json, logging, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

# --------------------------------------------------------------------------- #
# ProxyConfig – коректний імпорт і в режимі пакета, і в режимі скрипта
try:
    from .proxy_config import ProxyConfig                       # type: ignore
except (ImportError, ValueError):
    ROOT = Path(__file__).resolve().parents[2]
    sys.path.append(ROOT.as_posix())
    from src.utils.proxy_config import ProxyConfig               # noqa: E402

# --------------------------------------------------------------------------- #
# Читання конфіга
CFG_PATH = Path(__file__).resolve().parents[2] / "config" / "mongo_config.json"
CFG = json.loads(CFG_PATH.read_text(encoding="utf-8"))

MAIN_DB = CFG["databases"]["main_db"]
API_DB  = CFG["databases"]["api_db"]
CLIENT_KWARGS = CFG.get("client_params", {})

# --------------------------------------------------------------------------- #
logger = logging.getLogger("mongo_operations")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# --------------------------------------------------------------------------- #
# helper
def _coll(client: AsyncIOMotorClient, db_cfg: dict, key: str):
    return client[db_cfg["name"]][db_cfg["collections"][key]]

# =========================================================================== #
#                               CRUD-ФУНКЦІЇ                                  #
# =========================================================================== #
async def get_domain_for_analysis(
    client: AsyncIOMotorClient,
) -> Optional[Tuple[str, str, str]]:
    c = _coll(client, MAIN_DB, "domain_main")
    doc = await c.find_one_and_update(
        {"status": "processed"},
        {"$set": {"status": "processed_gemini"}, "$inc": {"url_context_try": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        return None
    return doc["target_uri"], doc["domain_full"], str(doc["_id"])


async def revert_domain_status(
    client: AsyncIOMotorClient, domain_id: str, reason: str = ""
) -> None:
    c = _coll(client, MAIN_DB, "domain_main")
    await c.update_one(
        {"_id": ObjectId(domain_id)},
        {"$set": {"status": "processed"}, "$inc": {"url_context_try": -1}},
    )


async def save_gemini_results(
    client: AsyncIOMotorClient, document: dict
) -> None:
    c = _coll(client, MAIN_DB, "gemini")
    await c.replace_one(
        {"domain_full": document["domain_full"]},
        document,
        upsert=True,
    )


async def get_api_key_and_proxy(
    client: AsyncIOMotorClient, cooldown_secs: int = 180
) -> Optional[Tuple[str, ProxyConfig, str, dict]]:
    c = _coll(client, API_DB, "keys")
    now = datetime.now(timezone.utc)
    doc = await c.find_one_and_update(
        {
            "api_status": "active",
            "$or": [
                {"api_last_used_date": None},
                {"api_last_used_date": {"$lt": now - timedelta(seconds=cooldown_secs)}},
            ],
        },
        {"$set": {"api_last_used_date": now}, "$inc": {"request_count_total": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        return None
    return (
        doc["api_key"],
        ProxyConfig.from_mongo(doc["proxy"]),
        str(doc["_id"]),
        doc,
    )


async def finalize_api_key_usage(
    client: AsyncIOMotorClient,
    key_id: str,
    status_code: int | None,
    is_proxy_error: bool,
) -> None:
    c = _coll(client, API_DB, "keys")
    upd: dict = {"$inc": {}}
    if status_code == 200:
        upd["$inc"]["request_count_200"] = 1
        upd["$set"] = {"request_count_429": 0}
    elif status_code == 429:
        upd["$inc"]["request_count_429"] = 1
    if is_proxy_error:
        upd["$inc"]["proxy_error_count"] = 1
    await c.update_one({"_id": ObjectId(key_id)}, upd)


async def update_api_key_ip(
    client: AsyncIOMotorClient, key_id: str, ip: str
) -> bool:
    c = _coll(client, API_DB, "keys")
    try:
        res = await c.update_one(
            {"_id": ObjectId(key_id)},
            {"$addToSet": {"used_ips": ip}, "$set": {"current_ip": ip}},
        )
        return bool(res.modified_count)
    except DuplicateKeyError:
        return False

# =========================================================================== #
#                               SMOKE-ТЕСТ                                    #
# =========================================================================== #
async def _show_db(client: AsyncIOMotorClient, db_cfg: dict) -> None:
    name = db_cfg["name"]
    print(f"\nDB: {name}")
    existing = await client[name].list_collection_names()

    for key, col_name in db_cfg["collections"].items():
        available = col_name in existing
        print(f"  {col_name:25s} available: {str(available).lower()}")
        if not available:
            continue

        doc = await client[name][col_name].find_one()
        if doc is None:
            print("    (collection empty)")
            continue

        print("    sample_document:")
        print(
            json.dumps(doc, default=str, ensure_ascii=False, indent=4)
            .replace("\n", "\n    ")
        )


async def _smoke() -> None:
    client = AsyncIOMotorClient(MAIN_DB["uri"], **CLIENT_KWARGS)
    await _show_db(client, MAIN_DB)
    await _show_db(client, API_DB)
    client.close()

if __name__ == "__main__":
    try:
        asyncio.run(_smoke())
    except OperationFailure as err:
        if err.code == 13:
            print("❌  MongoDB authorization failed. Check credentials.")
        else:
            raise
