from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict
from urllib.parse import parse_qsl

from app.config import get_mini_app_session_secret, get_mini_app_session_ttl_seconds


class MiniAppAuthError(ValueError):
    pass


def _bot_token() -> str:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise MiniAppAuthError("BOT_TOKEN no configurado")
    return token


def validate_telegram_init_data(init_data: str, *, max_age_seconds: int = 86400) -> Dict[str, Any]:
    raw = (init_data or "").strip()
    if not raw:
        raise MiniAppAuthError("init_data vacío")

    pairs = dict(parse_qsl(raw, keep_blank_values=True))
    provided_hash = pairs.pop("hash", None)
    if not provided_hash:
        raise MiniAppAuthError("hash ausente en init_data")

    data_check_string = "\n".join(f"{key}={pairs[key]}" for key in sorted(pairs.keys()))
    secret_key = hmac.new(b"WebAppData", _bot_token().encode("utf-8"), hashlib.sha256).digest()
    calculated_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calculated_hash, provided_hash):
        raise MiniAppAuthError("firma inválida de Telegram")

    auth_date_raw = pairs.get("auth_date")
    if auth_date_raw:
        try:
            auth_date = int(auth_date_raw)
        except Exception as exc:
            raise MiniAppAuthError("auth_date inválido") from exc
        if auth_date + max_age_seconds < int(time.time()):
            raise MiniAppAuthError("init_data expirado")

    user_payload = pairs.get("user")
    if not user_payload:
        raise MiniAppAuthError("usuario ausente en init_data")

    try:
        user = json.loads(user_payload)
    except Exception as exc:
        raise MiniAppAuthError("payload de usuario inválido") from exc

    return {
        "auth_date": int(pairs.get("auth_date") or 0),
        "query_id": pairs.get("query_id"),
        "user": user,
        "raw": pairs,
    }


def _urlsafe_b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _urlsafe_b64decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("utf-8"))


def issue_session_token(*, user_id: int, username: str | None = None, language: str | None = None) -> str:
    now = int(time.time())
    payload = {
        "uid": int(user_id),
        "usr": username or "",
        "lng": language or "es",
        "iat": now,
        "exp": now + get_mini_app_session_ttl_seconds(),
    }
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    secret = get_mini_app_session_secret().encode("utf-8")
    signature = hmac.new(secret, body, hashlib.sha256).digest()
    return f"{_urlsafe_b64encode(body)}.{_urlsafe_b64encode(signature)}"


def parse_session_token(token: str) -> Dict[str, Any]:
    raw = (token or "").strip()
    if not raw or "." not in raw:
        raise MiniAppAuthError("token de sesión inválido")

    body_b64, signature_b64 = raw.split(".", 1)
    body = _urlsafe_b64decode(body_b64)
    provided_signature = _urlsafe_b64decode(signature_b64)
    secret = get_mini_app_session_secret().encode("utf-8")
    expected_signature = hmac.new(secret, body, hashlib.sha256).digest()

    if not hmac.compare_digest(expected_signature, provided_signature):
        raise MiniAppAuthError("firma de sesión inválida")

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise MiniAppAuthError("payload de sesión inválido") from exc

    if int(payload.get("exp") or 0) < int(time.time()):
        raise MiniAppAuthError("sesión expirada")

    return payload
