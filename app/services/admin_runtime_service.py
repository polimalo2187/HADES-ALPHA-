from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from app.config import is_payment_configuration_ready
from app.database import audit_logs_collection, payment_orders_collection, signals_collection, users_collection
from app.observability import build_runtime_health_report
from app.models import utcnow

_RUNTIME_ROLES = ["web", "bot", "signal_worker", "scheduler"]
_ALLOWED_AUDIT_STATUSES = {"info", "ok", "success", "warning", "error"}


def _utcnow() -> datetime:
    return utcnow()


def _serialize_datetime(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _safe_count(collection, filter_query: Dict[str, Any]) -> int:
    try:
        return int(collection.count_documents(filter_query))
    except Exception:
        return 0


def _clamp_limit(limit: int) -> int:
    try:
        parsed = int(limit)
    except Exception:
        return 25
    return max(1, min(parsed, 100))



def get_admin_runtime_health_matrix() -> Dict[str, Any]:
    runtimes: Dict[str, Any] = {}
    overall_status = "ok"

    for role in _RUNTIME_ROLES:
        report = build_runtime_health_report(role)
        runtimes[role] = report
        role_status = str(report.get("overall_status") or "unknown")
        if role_status == "error":
            overall_status = "error"
        elif role_status != "ok" and overall_status == "ok":
            overall_status = "degraded"

    return {
        "ok": overall_status == "ok",
        "overall_status": overall_status,
        "generated_at": _serialize_datetime(_utcnow()),
        "runtimes": runtimes,
    }



def get_admin_operational_overview() -> Dict[str, Any]:
    now = _utcnow()
    last_24h = now - timedelta(hours=24)
    users = users_collection()
    signals = signals_collection()
    orders = payment_orders_collection()
    audits = audit_logs_collection()

    runtime_health = get_admin_runtime_health_matrix()

    overview = {
        "generated_at": _serialize_datetime(now),
        "runtime": runtime_health,
        "users": {
            "total": _safe_count(users, {}),
            "banned": _safe_count(users, {"banned": True}),
            "active_paid": _safe_count(users, {"subscription_status": "active"}),
        },
        "signals": {
            "created_last_24h": _safe_count(signals, {"created_at": {"$gte": last_24h}}),
            "pending_evaluation": _safe_count(signals, {"evaluated": {"$ne": True}}),
        },
        "payments": {
            "configuration_ready": is_payment_configuration_ready(),
            "pending_orders": _safe_count(orders, {"status": "pending"}),
            "awaiting_confirmation": _safe_count(orders, {"status": "awaiting_confirmation"}),
            "paid_last_24h": _safe_count(orders, {"status": "paid", "paid_at": {"$gte": last_24h}}),
        },
        "audit": {
            "errors_last_24h": _safe_count(audits, {"status": "error", "created_at": {"$gte": last_24h}}),
            "warnings_last_24h": _safe_count(audits, {"status": "warning", "created_at": {"$gte": last_24h}}),
        },
    }
    return overview



def list_recent_audit_events(*, limit: int = 25, status: Optional[str] = None, module: Optional[str] = None) -> Dict[str, Any]:
    query: Dict[str, Any] = {}
    normalized_status = (status or "").strip().lower()
    if normalized_status:
        if normalized_status not in _ALLOWED_AUDIT_STATUSES:
            raise ValueError("status inválido")
        query["status"] = normalized_status

    normalized_module = (module or "").strip().lower()
    if normalized_module:
        query["module"] = normalized_module

    clamped_limit = _clamp_limit(limit)
    items: List[Dict[str, Any]] = []

    for row in audit_logs_collection().find(query, sort=[("created_at", -1)], limit=clamped_limit):
        items.append({
            "created_at": _serialize_datetime(row.get("created_at")),
            "event_type": row.get("event_type"),
            "status": row.get("status"),
            "module": row.get("module"),
            "user_id": row.get("user_id"),
            "admin_id": row.get("admin_id"),
            "signal_id": row.get("signal_id"),
            "order_id": row.get("order_id"),
            "message": row.get("message"),
            "metadata": row.get("metadata") or {},
        })

    return {
        "items": items,
        "limit": clamped_limit,
        "filters": {
            "status": normalized_status or None,
            "module": normalized_module or None,
        },
    }
