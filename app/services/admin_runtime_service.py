from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable
from typing import Any, Dict, List, Optional

from app.config import is_payment_configuration_ready
from app.database import audit_logs_collection, payment_orders_collection, signals_collection, users_collection
from app.observability import build_runtime_health_report
from app.models import utcnow
from app.plans import normalize_plan, plan_status

_RUNTIME_ROLES = ["web", "bot", "signal_worker", "scheduler"]
_ALLOWED_AUDIT_STATUSES = {"info", "ok", "success", "warning", "error"}
_INCIDENT_AUDIT_STATUSES = {"warning", "error"}
_INCIDENT_RUNTIME_STATUSES = {"degraded", "stale", "missing", "error", "stopped"}


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





def _build_user_overview(users_collection_handle) -> Dict[str, Any]:
    summary = {
        "total": 0,
        "banned": 0,
        "active_paid": 0,
        "free": 0,
        "plus_active": 0,
        "premium_active": 0,
        "trialing": 0,
        "expired_free": 0,
    }

    try:
        cursor = users_collection_handle.find({}, {
            "user_id": 1,
            "plan": 1,
            "plan_end": 1,
            "trial_end": 1,
            "subscription_status": 1,
            "banned": 1,
        })
    except Exception:
        return summary

    for user in cursor:
        summary["total"] += 1
        if bool(user.get("banned")):
            summary["banned"] += 1
            continue

        effective = plan_status(user)
        plan_value = normalize_plan(effective.get("plan") or user.get("plan"))
        status_value = str(effective.get("status") or user.get("subscription_status") or "free").lower()

        if plan_value == "plus" and status_value == "active":
            summary["plus_active"] += 1
            summary["active_paid"] += 1
            continue

        if plan_value == "premium" and status_value == "active":
            summary["premium_active"] += 1
            summary["active_paid"] += 1
            continue

        summary["free"] += 1
        if status_value == "trial":
            summary["trialing"] += 1
        elif status_value == "expired":
            summary["expired_free"] += 1

    return summary
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

    user_overview = _build_user_overview(users)

    overview = {
        "generated_at": _serialize_datetime(now),
        "runtime": runtime_health,
        "users": {
            **user_overview,
            "current_mix": {
                "free": user_overview.get("free", 0),
                "plus": user_overview.get("plus_active", 0),
                "premium": user_overview.get("premium_active", 0),
            },
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



def _iter_runtime_incidents(runtime_payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    generated_at = runtime_payload.get("generated_at")
    for role, report in (runtime_payload.get("runtimes") or {}).items():
        components = (report.get("components") or {})
        for component_name, component in components.items():
            effective_status = str(component.get("effective_status") or component.get("status") or "unknown")
            if effective_status not in _INCIDENT_RUNTIME_STATUSES:
                continue
            yield {
                "source": "runtime_health",
                "runtime_role": str(role),
                "component": str(component_name),
                "status": effective_status,
                "severity": "error" if effective_status in {"error", "stopped"} else "warning",
                "created_at": component.get("updated_at") or generated_at,
                "message": f"runtime={role} component={component_name} status={effective_status}",
                "metadata": {
                    "age_seconds": component.get("age_seconds"),
                    "stale_after_seconds": component.get("stale_after_seconds"),
                    "details": component.get("details") or {},
                },
            }


def list_recent_incidents(*, limit: int = 25) -> Dict[str, Any]:
    clamped_limit = _clamp_limit(limit)
    runtime_payload = get_admin_runtime_health_matrix()
    audit_payload = list_recent_audit_events(limit=clamped_limit, status=None, module=None)

    items: List[Dict[str, Any]] = []

    for incident in _iter_runtime_incidents(runtime_payload):
        items.append(incident)

    for item in audit_payload.get("items") or []:
        status = str(item.get("status") or "info").lower()
        if status not in _INCIDENT_AUDIT_STATUSES:
            continue
        items.append({
            "source": "audit",
            "runtime_role": None,
            "component": item.get("module"),
            "status": status,
            "severity": status,
            "created_at": item.get("created_at"),
            "message": item.get("message") or item.get("event_type"),
            "event_type": item.get("event_type"),
            "metadata": item.get("metadata") or {},
        })

    def _sort_key(row: Dict[str, Any]) -> str:
        return str(row.get("created_at") or "")

    items.sort(key=_sort_key, reverse=True)
    items = items[:clamped_limit]

    severity_counts = {
        "error": sum(1 for item in items if item.get("severity") == "error"),
        "warning": sum(1 for item in items if item.get("severity") == "warning"),
    }

    return {
        "items": items,
        "limit": clamped_limit,
        "counts": severity_counts,
        "runtime_overall_status": runtime_payload.get("overall_status"),
        "generated_at": _serialize_datetime(_utcnow()),
    }


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
