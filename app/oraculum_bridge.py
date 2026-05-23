from __future__ import annotations

import hashlib
import os
import secrets
from datetime import timedelta
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from app.database import oraculum_link_tokens_collection
from app.models import utcnow
from app.plans import PLAN_PREMIUM, SUBSCRIPTION_STATUS_ACTIVE, normalize_plan, plan_status
from app.services.admin_service import is_effectively_banned


class OraculumBridgeError(ValueError):
    """Error controlado para el puente HADES → Oraculum."""


DEFAULT_ORACULUM_URL = "https://oraculum-production-ede3.up.railway.app"


def _first_env_value(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def _oraculum_url() -> str:
    # ORACULUM_URL sigue siendo la fuente principal.
    # Los aliases permiten configuraciones ya existentes y el fallback evita dejar
    # roto el botón si Railway no inyecta la variable en este servicio.
    url = (
        _first_env_value(
            "ORACULUM_URL",
            "ORACULUM_BASE_URL",
            "PUBLIC_ORACULUM_URL",
            "NEXT_PUBLIC_ORACULUM_URL",
            "VITE_ORACULUM_URL",
        )
        or DEFAULT_ORACULUM_URL
    ).rstrip("/")
    if not (url.startswith("https://") or url.startswith("http://")):
        raise OraculumBridgeError("ORACULUM_URL inválido")
    return url


def _link_ttl_seconds() -> int:
    try:
        return max(30, min(int(os.getenv("ORACULUM_LINK_TTL_SECONDS", "60") or "60"), 300))
    except Exception:
        return 60


def _hash_code(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def build_oraculum_entitlement(user: Dict[str, Any]) -> Dict[str, Any]:
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


def create_oraculum_link(user: Dict[str, Any], *, request_id: Optional[str] = None) -> Dict[str, Any]:
    entitlement = build_oraculum_entitlement(user)
    if not entitlement.get("ok"):
        raise OraculumBridgeError("premium_required")

    now = utcnow()
    ttl = _link_ttl_seconds()
    code = secrets.token_urlsafe(36)
    token_hash = _hash_code(code)
    premium_until = entitlement.get("expires_at")

    oraculum_link_tokens_collection().insert_one({
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

    params = urlencode({"code": code})
    url = f"{_oraculum_url()}/auth/hades/link?{params}"
    return {
        "ok": True,
        "url": url,
        "expires_in_seconds": ttl,
        "expires_at": (now + timedelta(seconds=ttl)).isoformat() + "Z",
        "premium_until": premium_until.isoformat() + "Z" if premium_until else None,
        "days_left": int(entitlement.get("days_left") or 0),
    }
