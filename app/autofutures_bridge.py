from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from app.models import utcnow
from app.plans import PLAN_PREMIUM, SUBSCRIPTION_STATUS_ACTIVE, normalize_plan, plan_status
from app.services.admin_service import is_effectively_banned


class AutoFuturesBridgeError(ValueError):
    """Error controlado para el puente HADES Alpha -> Hades AutoFutures."""


DEFAULT_AUTOFUTURES_URL = "https://hades-autofutures-production-32f9.up.railway.app"


def _first_env_value(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def _normalize_url(url: str) -> str:
    normalized = str(url or "").strip().rstrip("/")
    if not normalized:
        return ""
    if normalized.startswith("http://") or normalized.startswith("https://"):
        return normalized
    return f"https://{normalized}"


def _autofutures_url() -> str:
    url = _normalize_url(
        _first_env_value(
            "HADES_AUTOFUTURES_FRONTEND_URL",
            "AUTOFUTURES_URL",
            "HADES_AUTOFUTURES_URL",
            "PUBLIC_HADES_AUTOFUTURES_URL",
            "VITE_HADES_AUTOFUTURES_URL",
        )
        or DEFAULT_AUTOFUTURES_URL
    )
    if not (url.startswith("https://") or url.startswith("http://")):
        raise AutoFuturesBridgeError("HADES_AUTOFUTURES_FRONTEND_URL inválido")
    return url


def _sso_secret() -> str:
    secret = _first_env_value(
        "HADES_AUTOFUTURES_SSO_SECRET",
        "AUTOFUTURES_SSO_SECRET",
        "HADES_ALPHA_SSO_SECRET",
    )
    if len(secret) < 32:
        raise AutoFuturesBridgeError("HADES_AUTOFUTURES_SSO_SECRET inválido")
    return secret


def _link_ttl_seconds() -> int:
    try:
        return max(30, min(int(os.getenv("HADES_AUTOFUTURES_LINK_TTL_SECONDS", "60") or "60"), 300))
    except Exception:
        return 60


def _base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _sign_jwt(payload: Dict[str, Any], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    encoded_header = _base64url(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    encoded_payload = _base64url(json.dumps(payload, default=_json_default, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signing_input = f"{encoded_header}.{encoded_payload}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{encoded_header}.{encoded_payload}.{_base64url(signature)}"


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
    if is_effectively_banned(user):
        raise AutoFuturesBridgeError("user_banned")

    user_id = int(user.get("user_id") or 0)
    if user_id <= 0:
        raise AutoFuturesBridgeError("invalid_user")

    entitlement = build_autofutures_entitlement(user)
    if not entitlement.get("ok"):
        raise AutoFuturesBridgeError("premium_required")

    now_ts = int(time.time())
    ttl = _link_ttl_seconds()
    expires_ts = now_ts + ttl
    premium_until = entitlement.get("expires_at")

    payload = {
        "iss": "hades-alpha",
        "aud": "hades-autofutures",
        "iat": now_ts,
        "exp": expires_ts,
        "hadesUserId": str(user_id),
        "telegramId": str(user_id),
        "username": user.get("username"),
        "email": user.get("email"),
        "premiumActive": True,
        "premiumUntil": premium_until.isoformat().replace("+00:00", "Z") if isinstance(premium_until, datetime) else None,
        "requestId": request_id,
    }

    token = _sign_jwt(payload, _sso_secret())
    url = f"{_autofutures_url()}/auth/callback?{urlencode({'token': token})}"

    return {
        "ok": True,
        "url": url,
        "expires_in_seconds": ttl,
        "expires_at": datetime.fromtimestamp(expires_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "premium_until": payload["premiumUntil"],
        "days_left": int(entitlement.get("days_left") or 0),
    }
