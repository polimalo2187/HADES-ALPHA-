from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from pymongo.errors import PyMongoError

from app.database import audit_logs_collection, system_health_collection
from app.models import new_audit_log, utcnow

logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return str(value)



def compact_context(**context: Any) -> Dict[str, Any]:
    return {str(k): _json_safe(v) for k, v in context.items() if v is not None}



def log_event(event_type: str, *, level: int = logging.INFO, message: Optional[str] = None, **context: Any) -> None:
    payload = compact_context(**context)
    if message:
        payload["message"] = message
    logger.log(level, "%s | %s", event_type, payload)



def record_audit_event(
    *,
    event_type: str,
    status: str = "info",
    module: str = "system",
    user_id: Optional[int] = None,
    admin_id: Optional[int] = None,
    signal_id: Optional[str] = None,
    order_id: Optional[str] = None,
    callback: Optional[str] = None,
    message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        doc = new_audit_log(
            event_type=event_type,
            status=status,
            module=module,
            user_id=user_id,
            admin_id=admin_id,
            signal_id=signal_id,
            order_id=order_id,
            callback=callback,
            message=message,
            metadata=compact_context(**(metadata or {})),
        )
        cleaned = {k: v for k, v in doc.items() if v is not None}
        audit_logs_collection().insert_one(cleaned)
    except PyMongoError as exc:
        logger.error("❌ No se pudo registrar audit_log %s: %s", event_type, exc, exc_info=True)
    except Exception as exc:
        logger.error("❌ Error inesperado registrando audit_log %s: %s", event_type, exc, exc_info=True)



def heartbeat(component: str, *, status: str = "ok", details: Optional[Dict[str, Any]] = None) -> None:
    now = utcnow()
    payload = {
        "component": str(component),
        "status": str(status),
        "details": compact_context(**(details or {})),
        "schema_version": 1,
        "updated_at": now,
    }
    try:
        system_health_collection().update_one(
            {"component": str(component)},
            {
                "$set": payload,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.error("❌ No se pudo actualizar heartbeat de %s: %s", component, exc, exc_info=True)



def get_health_snapshot() -> Dict[str, Dict[str, Any]]:
    snapshot: Dict[str, Dict[str, Any]] = {}
    for row in system_health_collection().find({}, sort=[("component", 1)]):
        snapshot[str(row.get("component") or "unknown")] = {
            "status": row.get("status") or "unknown",
            "updated_at": row.get("updated_at"),
            "details": row.get("details") or {},
        }
    return snapshot
