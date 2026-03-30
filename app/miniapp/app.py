from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import (
    get_mini_app_cors_origins,
    get_mini_app_dev_user_id,
    get_runtime_role,
    is_mini_app_dev_auth_enabled,
)
from app.database import initialize_database
from app.miniapp.auth import MiniAppAuthError, issue_session_token, parse_session_token, validate_telegram_init_data
from app.miniapp.service import (
    build_bootstrap_payload,
    build_dashboard_payload,
    build_history_payload,
    build_market_payload,
    build_me_payload,
    build_plans_payload,
    build_signals_payload,
    build_watchlist_payload,
    ensure_mini_app_user,
    get_user_by_id,
    serialize_order_public,
)
from app.observability import heartbeat, record_audit_event
from app.payment_service import cancel_payment_order, confirm_payment_order, create_payment_order, get_active_payment_order_for_user

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
INDEX_FILE = STATIC_DIR / "index.html"


class MiniAppAuthRequest(BaseModel):
    init_data: Optional[str] = None
    dev_user_id: Optional[int] = None


class MiniAppPlanSelectionRequest(BaseModel):
    plan: str
    days: int


class MiniAppPaymentActionRequest(BaseModel):
    order_id: str



def _resolve_dev_telegram_user(payload: MiniAppAuthRequest) -> Dict[str, Any]:
    if not is_mini_app_dev_auth_enabled():
        raise MiniAppAuthError("autenticación dev deshabilitada")

    configured_dev_user_id = get_mini_app_dev_user_id()
    requested_dev_user_id = payload.dev_user_id

    if not configured_dev_user_id:
        raise MiniAppAuthError("MINI_APP_DEV_USER_ID no configurado")

    if requested_dev_user_id is not None and int(requested_dev_user_id) != int(configured_dev_user_id):
        raise MiniAppAuthError("dev_user_id no autorizado")

    return {
        "id": int(configured_dev_user_id),
        "username": f"dev_{int(configured_dev_user_id)}",
        "language_code": "es",
    }



def create_mini_app() -> FastAPI:
    app = FastAPI(title="HADES Mini App", version="1.0.1")
    cors_origins = get_mini_app_cors_origins()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=bool(cors_origins),
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Requested-With"],
    )

    @app.on_event("startup")
    async def on_startup() -> None:
        initialize_database()
        heartbeat(
            "miniapp",
            status="ok",
            details={
                "stage": "startup",
                "runtime_role": get_runtime_role(),
                "cors_origins": cors_origins,
                "dev_auth_enabled": is_mini_app_dev_auth_enabled(),
            },
        )

    app.mount("/miniapp/static", StaticFiles(directory=str(STATIC_DIR)), name="miniapp_static")

    def _get_bearer_token(authorization: Optional[str]) -> str:
        raw = (authorization or "").strip()
        if not raw.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="missing_bearer_token")
        return raw.split(" ", 1)[1].strip()

    def get_authenticated_user(authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
        token = _get_bearer_token(authorization)
        try:
            payload = parse_session_token(token)
        except MiniAppAuthError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        user = get_user_by_id(int(payload.get("uid") or 0))
        if not user:
            raise HTTPException(status_code=401, detail="session_user_not_found")
        if user.get("banned"):
            raise HTTPException(status_code=403, detail="user_banned")
        return user

    @app.get("/miniapp")
    async def miniapp_index() -> FileResponse:
        return FileResponse(str(INDEX_FILE))

    @app.get("/miniapp/health")
    async def miniapp_health() -> Dict[str, Any]:
        return {
            "ok": True,
            "service": "miniapp",
            "runtime_role": get_runtime_role(),
            "dev_auth_enabled": is_mini_app_dev_auth_enabled(),
            "cors_origins": cors_origins,
        }

    @app.post("/api/miniapp/auth")
    async def miniapp_auth(payload: MiniAppAuthRequest) -> Dict[str, Any]:
        telegram_user: Dict[str, Any]
        try:
            if payload.init_data:
                parsed = validate_telegram_init_data(payload.init_data)
                telegram_user = dict(parsed.get("user") or {})
            else:
                telegram_user = _resolve_dev_telegram_user(payload)
        except MiniAppAuthError as exc:
            record_audit_event(
                event_type="miniapp_auth_failed",
                status="error",
                module="miniapp",
                message=str(exc),
                metadata={
                    "runtime_role": get_runtime_role(),
                    "dev_auth_enabled": is_mini_app_dev_auth_enabled(),
                    "requested_dev_user_id": payload.dev_user_id,
                },
            )
            raise HTTPException(status_code=401, detail=str(exc)) from exc

        user = ensure_mini_app_user(
            user_id=int(telegram_user.get("id") or 0),
            username=telegram_user.get("username"),
            telegram_language=telegram_user.get("language_code") or "es",
        )
        session_token = issue_session_token(
            user_id=int(user.get("user_id") or 0),
            username=user.get("username"),
            language=user.get("language") or "es",
        )
        record_audit_event(
            event_type="miniapp_auth_succeeded",
            status="ok",
            module="miniapp",
            user_id=int(user.get("user_id") or 0),
            metadata={"auth_source": "telegram_init_data" if payload.init_data else "dev_auth"},
        )
        heartbeat("miniapp", status="ok", details={"stage": "authenticated", "user_id": int(user.get("user_id") or 0)})
        return {
            "ok": True,
            "session_token": session_token,
            "me": build_me_payload(user),
        }

    @app.get("/api/miniapp/bootstrap")
    async def miniapp_bootstrap(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_bootstrap_payload(user)

    @app.get("/api/miniapp/me")
    async def miniapp_me(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_me_payload(user)

    @app.get("/api/miniapp/dashboard")
    async def miniapp_dashboard(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_dashboard_payload(user)

    @app.get("/api/miniapp/signals")
    async def miniapp_signals(limit: int = 20, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return {"items": build_signals_payload(user, limit=limit)}

    @app.get("/api/miniapp/history")
    async def miniapp_history(limit: int = 20, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return {"items": build_history_payload(user, limit=limit)}

    @app.get("/api/miniapp/market")
    async def miniapp_market(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_market_payload()

    @app.get("/api/miniapp/watchlist")
    async def miniapp_watchlist(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return {"items": build_watchlist_payload(user)}

    @app.get("/api/miniapp/plans")
    async def miniapp_plans(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_plans_payload()

    @app.get("/api/miniapp/payment-order")
    async def miniapp_active_payment_order(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return {"order": serialize_order_public(get_active_payment_order_for_user(int(user.get("user_id") or 0)))}

    @app.post("/api/miniapp/payment-order")
    async def miniapp_create_payment_order(payload: MiniAppPlanSelectionRequest, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        try:
            order = create_payment_order(int(user.get("user_id") or 0), payload.plan, int(payload.days))
        except Exception as exc:
            record_audit_event(
                event_type="miniapp_payment_order_failed",
                status="error",
                module="miniapp",
                user_id=int(user.get("user_id") or 0),
                message=str(exc),
                metadata={"plan": payload.plan, "days": payload.days},
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, "order": serialize_order_public(order)}

    @app.post("/api/miniapp/payment-order/confirm")
    async def miniapp_confirm_payment(payload: MiniAppPaymentActionRequest, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        result = confirm_payment_order(payload.order_id, int(user.get("user_id") or 0))
        return result

    @app.post("/api/miniapp/payment-order/cancel")
    async def miniapp_cancel_payment(payload: MiniAppPaymentActionRequest, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        cancelled = cancel_payment_order(payload.order_id, int(user.get("user_id") or 0))
        return {"ok": cancelled}

    return app
