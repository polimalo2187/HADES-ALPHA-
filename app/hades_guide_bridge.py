from __future__ import annotations

import hashlib
import os
import secrets
from datetime import timedelta
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from app.database import hades_guide_link_tokens_collection
from app.models import utcnow
from app.services.admin_service import is_effectively_banned


class HadesGuideBridgeError(ValueError):
    """Error controlado para el puente HADES → Hades Guide."""


DEFAULT_HADES_GUIDE_URL = "https://hades-gide-production-be52.up.railway.app"


def _first_env_value(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def _hades_guide_url() -> str:
    url = (
        _first_env_value(
            "HADES_GUIDE_URL",
            "GUIDE_URL",
            "PUBLIC_HADES_GUIDE_URL",
            "VITE_HADES_GUIDE_URL",
        )
        or DEFAULT_HADES_GUIDE_URL
    ).rstrip("/")
    if not (url.startswith("https://") or url.startswith("http://")):
        raise HadesGuideBridgeError("HADES_GUIDE_URL inválido")
    return url


def _link_ttl_seconds() -> int:
    try:
        return max(30, min(int(os.getenv("HADES_GUIDE_LINK_TTL_SECONDS", "60") or "60"), 300))
    except Exception:
        return 60


def _hash_code(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def _public_user_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    user_id = int(user.get("user_id") or 0)
    username = user.get("username")
    first_name = user.get("first_name") or user.get("name") or ""
    last_name = user.get("last_name") or ""
    display_name = " ".join(part for part in [str(first_name).strip(), str(last_name).strip()] if part).strip()
    if not display_name:
        display_name = str(username or f"Usuario {user_id}")

    return {
        "jadeUserId": str(user_id),
        "hadesUserId": str(user_id),
        "userId": user_id,
        "username": username,
        "displayName": display_name,
        "firstName": first_name,
        "lastName": last_name,
        "languageCode": user.get("language_code") or user.get("language"),
        "status": "active",
        "source": "hades_alpha",
    }


def create_hades_guide_link(user: Dict[str, Any], *, request_id: Optional[str] = None) -> Dict[str, Any]:
    if is_effectively_banned(user):
        raise HadesGuideBridgeError("user_banned")

    user_id = int(user.get("user_id") or 0)
    if user_id <= 0:
        raise HadesGuideBridgeError("invalid_user")

    now = utcnow()
    ttl = _link_ttl_seconds()
    code = secrets.token_urlsafe(36)
    token_hash = _hash_code(code)
    expires_at = now + timedelta(seconds=ttl)

    hades_guide_link_tokens_collection().insert_one({
        "token_hash": token_hash,
        "user_id": user_id,
        "username": user.get("username"),
        "created_at": now,
        "expires_at": expires_at,
        "used_at": None,
        "request_id": request_id,
        "schema_version": 1,
    })

    url = f"{_hades_guide_url()}/auth/hades/callback?{urlencode({'code': code})}"
    return {
        "ok": True,
        "url": url,
        "expires_in_seconds": ttl,
        "expires_at": expires_at.isoformat() + "Z",
    }


def consume_hades_guide_code(code: str, *, request_id: Optional[str] = None) -> Dict[str, Any]:
    raw = str(code or "").strip()
    if not raw:
        raise HadesGuideBridgeError("code_required")

    token_hash = _hash_code(raw)
    now = utcnow()
    token = hades_guide_link_tokens_collection().find_one_and_update(
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
        raise HadesGuideBridgeError("invalid_or_expired_code")

    user_id = int(token.get("user_id") or 0)
    if user_id <= 0:
        raise HadesGuideBridgeError("invalid_user")

    from app.miniapp.service import get_user_by_id

    user = get_user_by_id(user_id)
    if not user:
        raise HadesGuideBridgeError("user_not_found")
    if is_effectively_banned(user):
        raise HadesGuideBridgeError("user_banned")

    return {
        "ok": True,
        "user": _public_user_payload(user),
    }
