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

MONGODB_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME") or os.getenv("MONGO_DB_NAME")

if not MONGODB_URI or not DATABASE_NAME:
    detected = {
        "MONGODB_URI": bool(os.getenv("MONGODB_URI")),
        "MONGO_URI": bool(os.getenv("MONGO_URI")),
        "DATABASE_NAME": bool(os.getenv("DATABASE_NAME")),
        "MONGO_DB_NAME": bool(os.getenv("MONGO_DB_NAME")),
    }
    raise RuntimeError(
        "MONGODB_URI/MONGO_URI o DATABASE_NAME/MONGO_DB_NAME no están definidos. "
        f"Detectadas: {detected}"
    )

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




def payment_orders_collection():
    """Órdenes de pago automáticas BEP-20"""
    return get_db()["payment_orders"]


def payment_verification_logs_collection():
    """Auditoría de verificaciones on-chain de pagos"""
    return get_db()["payment_verification_logs"]


def audit_logs_collection():
    """Auditoría operacional y de errores críticos"""
    return get_db()["audit_logs"]


def system_health_collection():
    """Estado y heartbeat de componentes internos"""
    return get_db()["system_health"]

UNIQUE_INDEX_DUPLICATE_QUERIES = {
    "users.user_id": ["user_id"],
    "users.ref_code": ["ref_code"],
    "referrals.referrer_id_referred_id": ["referrer_id", "referred_id"],
    "user_signals.user_id_signal_id": ["user_id", "signal_id"],
    "signal_results.base_signal_id": ["base_signal_id"],
    "watchlists.user_id": ["user_id"],
    "signal_deliveries.signal_id_user_id": ["signal_id", "user_id"],
    "stats_snapshots.key": ["key"],
    "payment_orders.order_id": ["order_id"],
    "payment_orders.matched_tx_hash": ["matched_tx_hash"],
    "system_health.component": ["component"],
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
    "payment_orders": [
        IndexModel([("order_id", ASCENDING)], name="order_id_unique", unique=True),
        IndexModel([("user_id", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING)], name="user_status_created_idx"),
        IndexModel([("status", ASCENDING), ("expires_at", ASCENDING)], name="status_expires_idx"),
        IndexModel(
            [("matched_tx_hash", ASCENDING)],
            name="matched_tx_hash_unique",
            unique=True,
            partialFilterExpression={"matched_tx_hash": {"$type": "string"}},
        ),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "payment_verification_logs": [
        IndexModel([("order_id", ASCENDING), ("created_at", DESCENDING)], name="order_created_idx"),
        IndexModel([("tx_hash", ASCENDING)], name="tx_hash_idx", sparse=True),
        IndexModel([("status", ASCENDING), ("created_at", DESCENDING)], name="status_created_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "subscription_events": [
        IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)], name="user_created_idx"),
        IndexModel([("event_type", ASCENDING), ("created_at", DESCENDING)], name="event_created_idx"),
        IndexModel([("plan", ASCENDING), ("created_at", DESCENDING)], name="plan_created_idx"),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "audit_logs": [
        IndexModel([("created_at", DESCENDING)], name="created_at_idx"),
        IndexModel([("event_type", ASCENDING), ("created_at", DESCENDING)], name="event_created_idx"),
        IndexModel([("status", ASCENDING), ("created_at", DESCENDING)], name="status_created_idx"),
        IndexModel([("module", ASCENDING), ("created_at", DESCENDING)], name="module_created_idx"),
        IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)], name="user_created_idx", sparse=True),
        IndexModel([("order_id", ASCENDING), ("created_at", DESCENDING)], name="order_created_idx", sparse=True),
        IndexModel([("signal_id", ASCENDING), ("created_at", DESCENDING)], name="signal_created_idx", sparse=True),
        IndexModel([("schema_version", ASCENDING)], name="schema_version_idx"),
    ],
    "system_health": [
        IndexModel([("component", ASCENDING)], name="component_unique", unique=True),
        IndexModel([("status", ASCENDING), ("updated_at", DESCENDING)], name="status_updated_idx"),
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
    "payment_orders": payment_orders_collection,
    "payment_verification_logs": payment_verification_logs_collection,
    "audit_logs": audit_logs_collection,
    "system_health": system_health_collection,
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





def _reconcile_payment_orders_tx_hash_index() -> None:
    """Corrige el índice único de tx_hash para no bloquear órdenes sin pago asociado."""
    collection = payment_orders_collection()
    index_name = "matched_tx_hash_unique"
    desired_partial = {"matched_tx_hash": {"$type": "string"}}

    try:
        existing_indexes = {item.get("name"): item for item in collection.list_indexes()}
    except PyMongoError as exc:
        logger.error("❌ No se pudieron leer índices de payment_orders: %s", exc, exc_info=True)
        return

    existing = existing_indexes.get(index_name)
    if not existing:
        return

    current_partial = existing.get("partialFilterExpression")
    current_sparse = bool(existing.get("sparse"))
    current_unique = bool(existing.get("unique"))
    if current_unique and current_partial == desired_partial and not current_sparse:
        return

    try:
        cleanup_result = collection.update_many({"matched_tx_hash": None}, {"$unset": {"matched_tx_hash": ""}})
        if int(cleanup_result.modified_count or 0):
            logger.warning(
                "⚠️ payment_orders: se limpiaron %s órdenes con matched_tx_hash=None antes de recrear índice",
                cleanup_result.modified_count,
            )
    except PyMongoError as exc:
        logger.error("❌ No se pudo limpiar matched_tx_hash nulo en payment_orders: %s", exc, exc_info=True)
        return

    try:
        collection.drop_index(index_name)
        logger.warning("⚠️ Índice legacy %s eliminado en payment_orders para recreación segura", index_name)
    except OperationFailure as exc:
        logger.error("❌ No se pudo eliminar índice legacy %s: %s", index_name, exc, exc_info=True)
        return
    except PyMongoError as exc:
        logger.error("❌ Error Mongo eliminando índice legacy %s: %s", index_name, exc, exc_info=True)
        return

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

    _reconcile_payment_orders_tx_hash_index()

    for collection_name, index_models in COLLECTION_INDEX_MODELS.items():
        _safe_create_indexes(collection_name, index_models)

    _indexes_initialized = True
    logger.info("✅ Inicialización de base de datos completada")
