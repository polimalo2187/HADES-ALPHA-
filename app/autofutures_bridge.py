from __future__ import annotations

import hashlib
import os
import secrets
from datetime import timedelta
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from app.database import autofutures_link_tokens_collection
from app.models import utcnow
from app.plans import PLAN_PREMIUM, SUBSCRIPTION_STATUS_ACTIVE, normalize_plan, plan_status
from app.services.admin_service import is_effectively_banned


class AutoFuturesBridgeError(ValueError):
    """Error controlado para el puente HADES -> AutoFutures."""


DEFAULT_AUTOFUTURES_URL = "https://hades-autofutures-production-32f9.up.railway.app"


def _first_env_value(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def _autofutures_url() -> str:
    url = (
        _first_env_value(
            "HADES_AUTOFUTURES_FRONTEND_URL",
            "AUTOFUTURES_URL",
            "HADES_AUTOFUTURES_URL",
            "PUBLIC_HADES_AUTOFUTURES_URL",
            "VITE_HADES_AUTOFUTURES_URL",
        )
        or DEFAULT_AUTOFUTURES_URL
    ).rstrip("/")
    if not (url.startswith("https://") or url.startswith("http://")):
        raise AutoFuturesBridgeError("HADES_AUTOFUTURES_FRONTEND_URL inválido")
    return url


def _link_ttl_seconds() -> int:
    try:
        return max(30, min(int(os.getenv("HADES_AUTOFUTURES_LINK_TTL_SECONDS", "60") or "60"), 300))
    except Exception:
        return 60


def _hash_code(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def build_autofutures_entitlement(user: Dict[str, Any]) -> Dict[str, Any]:
    status = plan_status(user)
    expires = status.get("expires")
    premium = (
        normalize_plan(status.get("plan") or user.get("plan")) == PLAN_PREMIUM
        and str(status.get("status") or "").lower() == SUBSCRIPTION_STATUS_ACTIVE
        and bool(expires)
        and expires > utcnow()
        and not is_effectively_banned(user)
    )
    return {
        "ok": bool(premium),
        "plan": normalize_plan(status.get("plan") or user.get("plan")),
        "status": str(status.get("status") or "free"),
        "expires_at": expires,
        "days_left": int(status.get("days_left") or 0),
    }


def create_autofutures_link(user: Dict[str, Any], *, request_id: Optional[str] = None) -> Dict[str, Any]:
    entitlement = build_autofutures_entitlement(user)
    if not entitlement.get("ok"):
        raise AutoFuturesBridgeError("premium_required")

    now = utcnow()
    ttl = _link_ttl_seconds()
    code = secrets.token_urlsafe(36)
    token_hash = _hash_code(code)
    premium_until = entitlement.get("expires_at")

    autofutures_link_tokens_collection().insert_one({
        "token_hash": token_hash,
        "user_id": int(user.get("user_id") or 0),
        "username": user.get("username"),
        "plan": PLAN_PREMIUM,
        "premium_until": premium_until,
        "created_at": now,
        "expires_at": now + timedelta(seconds=ttl),
        "used_at": None,
        "request_id": request_id,
        "schema_version": 1,
    })

    url = f"{_autofutures_url()}/auth/hades/link?{urlencode({'code': code})}"
    return {
        "ok": True,
        "url": url,
        "expires_in_seconds": ttl,
        "expires_at": (now + timedelta(seconds=ttl)).isoformat() + "Z",
        "premium_until": premium_until.isoformat() + "Z" if premium_until else None,
        "days_left": int(entitlement.get("days_left") or 0),
    }


def consume_autofutures_code(code: str, *, request_id: Optional[str] = None) -> Dict[str, Any]:
    raw = str(code or "").strip()
    if not raw:
        raise AutoFuturesBridgeError("code_required")

    token_hash = _hash_code(raw)
    now = utcnow()
    token = autofutures_link_tokens_collection().find_one_and_update(
        {
            "token_hash": token_hash,
            "used_at": None,
            "expires_at": {"$gt": now},
        },
        {
            "$set": {
                "used_at": now,
                "consume_request_id": request_id,
            }
        },
        return_document=True,
    )
    if not token:
        raise AutoFuturesBridgeError("invalid_or_expired_code")

    user_id = int(token.get("user_id") or 0)
    if user_id <= 0:
        raise AutoFuturesBridgeError("invalid_user")

    from app.miniapp.service import get_user_by_id

    user = get_user_by_id(user_id)
    if not user:
        raise AutoFuturesBridgeError("user_not_found")
    if is_effectively_banned(user):
        raise AutoFuturesBridgeError("user_banned")

    entitlement = build_autofutures_entitlement(user)
    if not entitlement.get("ok"):
        raise AutoFuturesBridgeError("premium_required")

    expires_at = entitlement.get("expires_at")
    return {
        "ok": True,
        "user": {
            "hadesUserId": str(user_id),
            "jadeUserId": str(user_id),
            "telegramId": str(user_id),
            "username": user.get("username"),
            "email": user.get("email"),
            "premiumActive": True,
            "premiumUntil": expires_at.isoformat() + "Z" if expires_at else None,
            "daysLeft": int(entitlement.get("days_left") or 0),
            "source": "hades_alpha",
        },
    }
