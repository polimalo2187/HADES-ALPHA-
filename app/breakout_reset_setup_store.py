from __future__ import annotations

import copy
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

STORE_VERSION = "v1_breakout_reset_state_store"
_MEMORY_LOCK = threading.Lock()
_MEMORY_SETUPS: Dict[str, Dict[str, Any]] = {}
_COLLECTION_NAME = "breakout_reset_setups"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _store_mode() -> str:
    return str(os.getenv("BREAKOUT_RESET_SETUP_STORE", "auto")).strip().lower() or "auto"


def _mongo_collection():
    try:
        from app.database import get_db  # lazy: tests/local imports must not require Mongo env
        return get_db()[_COLLECTION_NAME]
    except Exception as exc:  # pragma: no cover - depends on runtime env
        if _store_mode() == "mongo":
            logger.warning("Mongo breakout setup store unavailable; falling back to memory: %s", exc)
        return None


def _use_mongo() -> bool:
    return _store_mode() in {"auto", "mongo"} and _mongo_collection() is not None


def _copy_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(doc)


def upsert_waiting_setup(setup: Dict[str, Any]) -> Dict[str, Any]:
    payload = _copy_doc(setup)
    setup_id = str(payload.get("setup_id") or "").strip()
    if not setup_id:
        raise ValueError("setup_id is required")
    now = _utcnow()
    payload.setdefault("status", "waiting_reset")
    payload.setdefault("created_at", now)
    payload["updated_at"] = now
    payload.setdefault("schema_version", STORE_VERSION)

    collection = _mongo_collection() if _store_mode() != "memory" else None
    if collection is not None:
        try:
            collection.update_one(
                {"setup_id": setup_id},
                {
                    "$set": payload,
                    "$setOnInsert": {"created_at": payload.get("created_at", now)},
                },
                upsert=True,
            )
            stored = collection.find_one({"setup_id": setup_id}) or payload
            return dict(stored)
        except Exception as exc:  # pragma: no cover
            logger.warning("Could not upsert breakout setup in Mongo; using memory fallback: %s", exc)

    with _MEMORY_LOCK:
        existing = dict(_MEMORY_SETUPS.get(setup_id) or {})
        existing.update(payload)
        existing.setdefault("created_at", now)
        existing["updated_at"] = now
        _MEMORY_SETUPS[setup_id] = existing
        return _copy_doc(existing)


def find_waiting_setups(symbol: str, direction: Optional[str] = None, limit: int = 5) -> List[Dict[str, Any]]:
    symbol_key = str(symbol or "").upper().strip()
    direction_key = str(direction or "").upper().strip()
    query: Dict[str, Any] = {"symbol": symbol_key, "status": "waiting_reset"}
    if direction_key:
        query["direction"] = direction_key

    collection = _mongo_collection() if _store_mode() != "memory" else None
    if collection is not None:
        try:
            cursor = collection.find(query).sort("updated_at", -1).limit(int(limit))
            return [dict(item) for item in cursor]
        except Exception as exc:  # pragma: no cover
            logger.warning("Could not load breakout setups from Mongo; using memory fallback: %s", exc)

    with _MEMORY_LOCK:
        docs = []
        for doc in _MEMORY_SETUPS.values():
            if str(doc.get("symbol") or "").upper().strip() != symbol_key:
                continue
            if str(doc.get("status") or "") != "waiting_reset":
                continue
            if direction_key and str(doc.get("direction") or "").upper().strip() != direction_key:
                continue
            docs.append(_copy_doc(doc))
        docs.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)
        return docs[: int(limit)]


def mark_setup_status(setup_id: str, status: str, reason: Optional[str] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    setup_key = str(setup_id or "").strip()
    if not setup_key:
        return
    now = _utcnow()
    update: Dict[str, Any] = {
        "status": str(status or "unknown"),
        "updated_at": now,
    }
    if reason:
        update["terminal_reason"] = str(reason)
    if extra:
        update.update(dict(extra))

    collection = _mongo_collection() if _store_mode() != "memory" else None
    if collection is not None:
        try:
            collection.update_one({"setup_id": setup_key}, {"$set": update})
            return
        except Exception as exc:  # pragma: no cover
            logger.warning("Could not update breakout setup status in Mongo; using memory fallback: %s", exc)

    with _MEMORY_LOCK:
        if setup_key in _MEMORY_SETUPS:
            _MEMORY_SETUPS[setup_key].update(update)


def clear_memory_store() -> None:
    with _MEMORY_LOCK:
        _MEMORY_SETUPS.clear()
