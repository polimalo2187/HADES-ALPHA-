from __future__ import annotations

import logging
import os
from typing import Iterable, List, Sequence, Tuple

from pymongo import ASCENDING, DESCENDING, IndexModel, MongoClient
from pymongo.errors import OperationFailure, PyMongoError

logger = logging.getLogger(__name__)

# =========================
# CONEXIÓN MONGODB
# =========================

MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

if not MONGODB_URI or not DATABASE_NAME:
    raise RuntimeError("MONGODB_URI o DATABASE_NAME no están definidos")

# MongoClient global seguro para threads
_client = None
_db = None
_indexes_initialized = False


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGODB_URI)
    return _client


def get_db():
    global _db
    if _db is None:
        _db = get_client()[DATABASE_NAME]
    return _db


# =========================
# COLECCIONES PRINCIPALES
# =========================


def users_collection():
    """Usuarios del bot"""
    return get_db()["users"]



def referrals_collection():
    """Historial de referidos válidos"""
    return get_db()["referrals"]



def signals_collection():
    """Señales BASE generadas por el scanner"""
    return get_db()["signals"]



def user_signals_collection():
    """Señales PERSONALIZADAS entregadas a cada usuario"""
    return get_db()["user_signals"]



def signal_results_collection():
    """Resultados de señales para estadísticas"""
    return get_db()["signal_results"]



def watchlists_collection():
    """Watchlists por usuario"""
    return get_db()["watchlists"]


def signal_jobs_collection():
    """Jobs persistidos de despacho rápido de señales"""
    return get_db()["signal_jobs"]


def signal_deliveries_collection():
    """Tracking de entrega de push por usuario y señal"""
    return get_db()["signal_deliveries"]


def stats_snapshots_collection():
    """Snapshots materializados de estadísticas"""
    return get_db()["stats_snapshots"]


def signal_history_collection():
    """Histórico persistente y verificable de señales cerradas"""
    return get_db()["signal_history"]


def subscription_events_collection():
    """Auditoría comercial de activaciones, upgrades, rewards y expiraciones"""
    return get_db()["subscription_events"]


UNIQUE_INDEX_DUPLICATE_QUERIES = {
    "users.user_id": ["user_id"],
    "users.ref_code": ["ref_code"],
    "referrals.referrer_id_referred_id": ["referrer_id", "referred_id"],
    "user_signals.user_id_signal_id": ["user_id", "signal_id"],
    "signal_results.base_signal_id": ["base_signal_id"],
    "watchlists.user_id": ["user_id"],
    "signal_deliveries.signal_id_user_id": ["signal_id", "user_id"],
    "stats_snapshots.key": ["key"],
}


COLLECTION_INDEX_MODELS = {
    "users": [
        IndexModel([("user_id", ASCENDING)], name="user_id_unique", unique=True),
        IndexModel([("ref_code", ASCENDING)], name="ref_code_unique", unique=True, sparse=True),
        IndexModel([("plan", ASCENDING), ("plan_end", ASCENDING)], name="plan_status_idx"),
        IndexModel([("subscription_status", ASCENDING), ("plan_end", ASCENDING)], name="subscription_status_idx"),
        IndexModel([("banned", ASCENDING), ("user_id", ASCENDING)], name="banned_user_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
        IndexModel([("updated_at", DESCENDING)], name="updated_at_idx"),
    ],
    "referrals": [
        IndexModel([("referrer_id", ASCENDING), ("referred_id", ASCENDING)], name="referrer_referred_unique", unique=True),
        IndexModel([("referred_id", ASCENDING)], name="referred_id_idx"),
        IndexModel([("activated_plan", ASCENDING), ("activated_at", DESCENDING)], name="activated_plan_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "signals": [
        IndexModel([("symbol", ASCENDING), ("created_at", DESCENDING)], name="symbol_created_idx"),
        IndexModel([("visibility", ASCENDING), ("telegram_valid_until", DESCENDING)], name="visibility_telegram_valid_idx"),
        IndexModel([("telegram_valid_until", DESCENDING)], name="telegram_valid_until_idx"),
        IndexModel([("evaluation_valid_until", DESCENDING)], name="evaluation_valid_until_idx"),
        IndexModel([("evaluated", ASCENDING), ("valid_until", ASCENDING)], name="evaluated_valid_until_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "user_signals": [
        IndexModel([("user_id", ASCENDING), ("signal_id", ASCENDING)], name="user_signal_unique", unique=True),
        IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)], name="user_created_idx"),
        IndexModel([("user_id", ASCENDING), ("telegram_valid_until", DESCENDING)], name="user_telegram_valid_idx"),
        IndexModel([("symbol", ASCENDING), ("telegram_valid_until", DESCENDING)], name="symbol_telegram_valid_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "signal_results": [
        IndexModel([("base_signal_id", ASCENDING)], name="base_signal_id_unique", unique=True),
        IndexModel([("symbol", ASCENDING), ("evaluated_at", DESCENDING)], name="symbol_evaluated_idx"),
        IndexModel([("result", ASCENDING), ("evaluated_at", DESCENDING)], name="result_evaluated_idx"),
        IndexModel([("plan", ASCENDING), ("evaluated_at", DESCENDING)], name="plan_evaluated_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "watchlists": [
        IndexModel([("user_id", ASCENDING)], name="watchlist_user_unique", unique=True),
        IndexModel([("updated_at", DESCENDING)], name="updated_at_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "signal_jobs": [
        IndexModel([("status", ASCENDING), ("enqueued_at", ASCENDING)], name="status_enqueued_idx"),
        IndexModel([("signal_id", ASCENDING), ("status", ASCENDING)], name="signal_status_idx"),
        IndexModel([("next_retry_at", ASCENDING)], name="next_retry_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "signal_deliveries": [
        IndexModel([("signal_id", ASCENDING), ("user_id", ASCENDING)], name="signal_user_unique", unique=True),
        IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)], name="user_created_idx"),
        IndexModel([("status", ASCENDING), ("updated_at", DESCENDING)], name="status_updated_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "stats_snapshots": [
        IndexModel([("key", ASCENDING)], name="stats_key_unique", unique=True),
        IndexModel([("window_days", ASCENDING), ("updated_at", DESCENDING)], name="window_updated_idx"),
        IndexModel([("computed_at", DESCENDING)], name="computed_at_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "signal_history": [
        IndexModel([("signal_id", ASCENDING)], name="signal_id_unique", unique=True),
        IndexModel([("visibility", ASCENDING), ("signal_created_at", DESCENDING)], name="visibility_created_idx"),
        IndexModel([("result", ASCENDING), ("signal_created_at", DESCENDING)], name="result_created_idx"),
        IndexModel([("setup_group", ASCENDING), ("signal_created_at", DESCENDING)], name="setup_created_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "subscription_events": [
        IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)], name="user_created_idx"),
        IndexModel([("event_type", ASCENDING), ("created_at", DESCENDING)], name="event_created_idx"),
        IndexModel([("plan", ASCENDING), ("created_at", DESCENDING)], name="plan_created_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
}


COLLECTION_GETTERS = {
    "users": users_collection,
    "referrals": referrals_collection,
    "signals": signals_collection,
    "user_signals": user_signals_collection,
    "signal_results": signal_results_collection,
    "watchlists": watchlists_collection,
    "signal_jobs": signal_jobs_collection,
    "signal_deliveries": signal_deliveries_collection,
    "stats_snapshots": stats_snapshots_collection,
    "signal_history": signal_history_collection,
    "subscription_events": subscription_events_collection,
}


def _find_duplicate_groups(collection_name: str, fields: Sequence[str], limit: int = 5) -> List[dict]:
    collection = COLLECTION_GETTERS[collection_name]()
    group_id = {field: f"${field}" for field in fields}
    pipeline = [
        {"$match": {fields[0]: {"$exists": True, "$ne": None}}},
        {"$group": {"_id": group_id, "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit},
    ]
    return list(collection.aggregate(pipeline))



def _safe_create_indexes(collection_name: str, models: Iterable[IndexModel]) -> None:
    collection = COLLECTION_GETTERS[collection_name]()
    for model in models:
        document = model.document
        key_spec = document.get("key")
        if hasattr(key_spec, "items"):
            keys = tuple(field for field, _ in key_spec.items())
        else:
            keys = tuple(field for field, _ in key_spec)
        name = document.get("name") or "unnamed_index"
        is_unique = bool(document.get("unique"))
        duplicates = []

        if is_unique:
            duplicates = _find_duplicate_groups(collection_name, keys)
            if duplicates:
                logger.error(
                    "❌ No se creó índice único %s en %s porque existen duplicados: %s",
                    name,
                    collection_name,
                    duplicates,
                )
                continue

        try:
            collection.create_indexes([model])
            logger.info("✅ Índice listo en %s: %s", collection_name, name)
        except OperationFailure as exc:
            logger.error(
                "❌ Error creando índice %s en %s: %s",
                name,
                collection_name,
                exc,
                exc_info=True,
            )
        except PyMongoError as exc:
            logger.error(
                "❌ Error Mongo creando índice %s en %s: %s",
                name,
                collection_name,
                exc,
                exc_info=True,
            )



def initialize_database() -> None:
    """Inicializa índices críticos de forma idempotente."""
    global _indexes_initialized
    if _indexes_initialized:
        return

    for collection_name, index_models in COLLECTION_INDEX_MODELS.items():
        _safe_create_indexes(collection_name, index_models)

    _indexes_initialized = True
    logger.info("✅ Inicialización de base de datos completada")
