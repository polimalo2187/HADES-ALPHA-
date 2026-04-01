from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from app.config import is_admin
from app.database import (
    payment_orders_collection,
    payment_verification_logs_collection,
    signal_deliveries_collection,
    signal_history_collection,
    subscription_events_collection,
    user_signals_collection,
    users_collection,
    watchlists_collection,
)
from app.models import utcnow


_BAN_UNIT_SECONDS = {
    "hours": 3600,
    "days": 86400,
    "weeks": 604800,
}


def _now() -> datetime:
    return utcnow()


def get_user_by_id(user_id: int) -> dict | None:
    return users_collection().find_one({"user_id": int(user_id)})


def can_block_target(admin_user_id: int, target_user_id: int) -> tuple[bool, str | None]:
    if target_user_id == admin_user_id:
        return False, "self"
    if is_admin(target_user_id):
        return False, "admin"
    return True, None


def can_delete_target(admin_user_id: int, target_user_id: int) -> tuple[bool, str | None]:
    return can_block_target(admin_user_id, target_user_id)


def _serialize_datetime(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def resolve_ban_state(user: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not user:
        return {
            "active": False,
            "mode": None,
            "label": "Sin baneo",
            "until": None,
            "expired": False,
            "seconds_left": None,
        }

    banned = bool(user.get("banned"))
    if not banned:
        return {
            "active": False,
            "mode": None,
            "label": "Sin baneo",
            "until": _serialize_datetime(user.get("banned_until")),
            "expired": False,
            "seconds_left": None,
        }

    mode = str(user.get("ban_mode") or "permanent").lower().strip()
    banned_until = user.get("banned_until")
    if mode == "temporary" and isinstance(banned_until, datetime):
        remaining = int((banned_until - _now()).total_seconds())
        if remaining <= 0:
            return {
                "active": False,
                "mode": "temporary",
                "label": "Baneo temporal expirado",
                "until": banned_until.isoformat(),
                "expired": True,
                "seconds_left": 0,
            }
        return {
            "active": True,
            "mode": "temporary",
            "label": "Baneo temporal",
            "until": banned_until.isoformat(),
            "expired": False,
            "seconds_left": remaining,
        }

    return {
        "active": True,
        "mode": "permanent",
        "label": "Baneo permanente",
        "until": None,
        "expired": False,
        "seconds_left": None,
    }


def clear_expired_ban(target_user_id: int) -> bool:
    user = get_user_by_id(int(target_user_id))
    state = resolve_ban_state(user)
    if not state.get("expired"):
        return False
    users_collection().update_one(
        {"user_id": int(target_user_id)},
        {"$set": {"banned": False, "updated_at": _now()}, "$unset": {
            "banned_at": "",
            "banned_by": "",
            "banned_until": "",
            "ban_mode": "",
            "ban_reason": "",
        }},
    )
    return True


def is_effectively_banned(user: Optional[Dict[str, Any]]) -> bool:
    if not user:
        return False
    state = resolve_ban_state(user)
    if state.get("expired"):
        clear_expired_ban(int(user.get("user_id") or 0))
        return False
    return bool(state.get("active"))


def ban_user(target_user_id: int, banned_by: int) -> None:
    apply_permanent_ban(target_user_id=target_user_id, banned_by=banned_by)


def _validate_ban_duration(value: int, unit: str) -> tuple[int, str]:
    try:
        amount = int(value)
    except Exception as exc:
        raise ValueError("invalid_ban_duration") from exc
    if amount <= 0:
        raise ValueError("invalid_ban_duration")
    normalized_unit = str(unit or "days").strip().lower()
    if normalized_unit not in _BAN_UNIT_SECONDS:
        raise ValueError("invalid_ban_unit")
    max_by_unit = {"hours": 24 * 90, "days": 3650, "weeks": 520}
    if amount > max_by_unit[normalized_unit]:
        raise ValueError("ban_duration_too_high")
    return amount, normalized_unit


def apply_temporary_ban(
    *,
    target_user_id: int,
    banned_by: int,
    duration_value: int,
    duration_unit: str = "days",
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    amount, normalized_unit = _validate_ban_duration(duration_value, duration_unit)
    now = _now()
    banned_until = now + timedelta(seconds=amount * _BAN_UNIT_SECONDS[normalized_unit])
    users_collection().update_one(
        {"user_id": int(target_user_id)},
        {"$set": {
            "banned": True,
            "banned_at": now,
            "banned_by": int(banned_by),
            "banned_until": banned_until,
            "ban_mode": "temporary",
            "ban_reason": str(reason or "manual_temporary_ban"),
            "updated_at": now,
        }},
    )
    return {
        "mode": "temporary",
        "duration_value": amount,
        "duration_unit": normalized_unit,
        "banned_until": banned_until.isoformat(),
    }


def apply_permanent_ban(*, target_user_id: int, banned_by: int, reason: Optional[str] = None) -> Dict[str, Any]:
    now = _now()
    users_collection().update_one(
        {"user_id": int(target_user_id)},
        {"$set": {
            "banned": True,
            "banned_at": now,
            "banned_by": int(banned_by),
            "ban_mode": "permanent",
            "ban_reason": str(reason or "manual_permanent_ban"),
            "updated_at": now,
        }, "$unset": {"banned_until": ""}},
    )
    return {"mode": "permanent", "banned_until": None}


def remove_ban(*, target_user_id: int, unbanned_by: int) -> Dict[str, Any]:
    now = _now()
    users_collection().update_one(
        {"user_id": int(target_user_id)},
        {"$set": {"banned": False, "updated_at": now, "unbanned_at": now, "unbanned_by": int(unbanned_by)}, "$unset": {
            "banned_at": "",
            "banned_by": "",
            "banned_until": "",
            "ban_mode": "",
            "ban_reason": "",
        }},
    )
    return {"mode": "cleared", "unbanned_at": now.isoformat()}


def delete_user_data(*, target_user_id: int, deleted_by: int) -> Dict[str, Any]:
    target_user_id = int(target_user_id)
    summary = {
        "deleted_user_docs": users_collection().delete_many({"user_id": target_user_id}).deleted_count,
        "deleted_watchlists": watchlists_collection().delete_many({"user_id": target_user_id}).deleted_count,
        "deleted_user_signals": user_signals_collection().delete_many({"user_id": target_user_id}).deleted_count,
        "deleted_history": signal_history_collection().delete_many({"user_id": target_user_id}).deleted_count,
        "deleted_payment_orders": payment_orders_collection().delete_many({"user_id": target_user_id}).deleted_count,
        "deleted_payment_logs": payment_verification_logs_collection().delete_many({"user_id": target_user_id}).deleted_count,
        "deleted_subscription_events": subscription_events_collection().delete_many({"user_id": target_user_id}).deleted_count,
        "deleted_signal_deliveries": signal_deliveries_collection().delete_many({"user_id": target_user_id}).deleted_count,
        "deleted_by": int(deleted_by),
        "deleted_at": _now().isoformat(),
    }
    return summary


def validate_custom_plan_days(days: int) -> tuple[bool, str | None]:
    if days <= 0:
        return False, "non_positive"
    if days > 3650:
        return False, "too_high"
    return True, None
