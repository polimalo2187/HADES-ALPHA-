from __future__ import annotations

import logging
import os
import socket
import threading
import time
from datetime import UTC, datetime
from typing import Any, Callable, Dict, Optional

from pymongo.errors import PyMongoError

from app.database import audit_logs_collection, system_health_collection
from app.models import new_audit_log, utcnow

logger = logging.getLogger(__name__)

_HEARTBEAT_THREADS: Dict[str, threading.Thread] = {}
_HEARTBEAT_LOCK = threading.Lock()

_COMPONENT_STALE_DEFAULTS: Dict[str, int] = {
    "database": 900,
    "miniapp": 180,
    "bot": 180,
    "bot_ui": 180,
    "signal_worker": 180,
    "scanner": 240,
    "scheduler": 900,
    "signal_pipeline": 900,
    "payments": 1800,
    "statistics": 1800,
    "history": 1800,
}

_RUNTIME_REQUIRED_COMPONENTS: Dict[str, list[str]] = {
    "web": ["miniapp", "database"],
    "bot": ["bot", "scanner", "scheduler", "database"],
    "bot_ui": ["bot", "database"],
    "signal_worker": ["signal_worker", "database"],
    "scheduler": ["scheduler", "database"],
}



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
        "details": compact_context(
            hostname=socket.gethostname(),
            pid=os.getpid(),
            **(details or {}),
        ),
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



def start_background_heartbeat(
    component: str,
    *,
    interval_seconds: int = 60,
    status: str = "ok",
    details_provider: Optional[Callable[[], Dict[str, Any]]] = None,
) -> threading.Thread:
    component_key = str(component)
    resolved_interval = max(int(interval_seconds), 15)

    with _HEARTBEAT_LOCK:
        existing = _HEARTBEAT_THREADS.get(component_key)
        if existing and existing.is_alive():
            return existing

        def _runner() -> None:
            while True:
                details: Dict[str, Any] = {}
                try:
                    if details_provider is not None:
                        details = details_provider() or {}
                except Exception as exc:
                    logger.warning("⚠️ details_provider falló para %s: %s", component_key, exc)
                    details = {"details_provider_error": str(exc)}
                heartbeat(component_key, status=status, details=details)
                time.sleep(resolved_interval)

        thread = threading.Thread(
            target=_runner,
            daemon=True,
            name=f"Heartbeat[{component_key}]",
        )
        thread.start()
        _HEARTBEAT_THREADS[component_key] = thread
        return thread



def get_health_snapshot() -> Dict[str, Dict[str, Any]]:
    snapshot: Dict[str, Dict[str, Any]] = {}
    for row in system_health_collection().find({}, sort=[("component", 1)]):
        snapshot[str(row.get("component") or "unknown")] = {
            "status": row.get("status") or "unknown",
            "updated_at": row.get("updated_at"),
            "details": row.get("details") or {},
        }
    return snapshot



def get_required_components_for_role(role: str) -> list[str]:
    return list(_RUNTIME_REQUIRED_COMPONENTS.get(str(role), []))



def get_component_stale_after_seconds(component: str) -> int:
    normalized = str(component or "").strip().lower()
    specific_env = f"HEALTH_{normalized.upper()}_STALE_AFTER_SECONDS"
    raw_specific = os.getenv(specific_env, "").strip()
    if raw_specific:
        try:
            return max(int(raw_specific), 30)
        except Exception:
            pass

    raw_default = os.getenv("HEALTH_DEFAULT_STALE_AFTER_SECONDS", "").strip()
    if raw_default:
        try:
            return max(int(raw_default), 30)
        except Exception:
            pass

    return _COMPONENT_STALE_DEFAULTS.get(normalized, 300)



def _normalize_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None



def assess_component(component: str, row: Optional[Dict[str, Any]], *, now: Optional[datetime] = None) -> Dict[str, Any]:
    current_time = now or datetime.now(UTC)
    updated_at = _normalize_timestamp((row or {}).get("updated_at"))
    status = str((row or {}).get("status") or "missing")
    details = dict((row or {}).get("details") or {})
    stale_after_seconds = get_component_stale_after_seconds(component)

    age_seconds: Optional[int] = None
    is_stale = True
    if updated_at is not None:
        age_seconds = max(int((current_time - updated_at).total_seconds()), 0)
        is_stale = age_seconds > stale_after_seconds

    effective_status = status
    if status == "missing":
        effective_status = "missing"
    elif is_stale and status not in {"error", "stopped"}:
        effective_status = "stale"

    return {
        "component": str(component),
        "status": status,
        "effective_status": effective_status,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "age_seconds": age_seconds,
        "stale_after_seconds": stale_after_seconds,
        "is_stale": is_stale,
        "details": compact_context(**details),
    }



def build_health_report(*, role: Optional[str] = None, required_components: Optional[list[str]] = None) -> Dict[str, Any]:
    current_time = datetime.now(UTC)
    snapshot = get_health_snapshot()
    expected_components = list(required_components or get_required_components_for_role(role or ""))

    component_names = sorted(set(snapshot.keys()) | set(expected_components))
    components = {
        name: assess_component(name, snapshot.get(name), now=current_time)
        for name in component_names
    }

    overall_status = "ok"
    missing_required = [
        name for name in expected_components
        if components.get(name, {}).get("status") == "missing"
    ]

    if missing_required:
        overall_status = "degraded"

    for row in components.values():
        effective_status = row["effective_status"]
        if effective_status in {"error", "stopped"}:
            overall_status = "error"
            break
        if effective_status in {"degraded", "stale", "missing"} and overall_status == "ok":
            overall_status = "degraded"

    return {
        "ok": overall_status == "ok",
        "overall_status": overall_status,
        "runtime_role": role,
        "generated_at": current_time.isoformat(),
        "required_components": expected_components,
        "missing_required_components": missing_required,
        "components": components,
    }



def build_runtime_health_report(role: str) -> Dict[str, Any]:
    return build_health_report(role=role, required_components=get_required_components_for_role(role))
