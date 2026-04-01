from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

from bson import ObjectId
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import (
    get_mini_app_cors_origins,
    get_mini_app_dev_user_id,
    get_runtime_role,
    is_admin,
    is_mini_app_dev_auth_enabled,
)
from app.database import initialize_database
from app.miniapp.auth import MiniAppAuthError, issue_session_token, parse_session_token, validate_telegram_init_data
from app.miniapp.service import (
    build_bootstrap_payload,
    build_account_center_payload,
    build_dashboard_payload,
    build_history_payload,
    build_market_payload,
    build_me_payload,
    build_plans_payload,
    build_performance_center_payload,
    build_risk_center_payload,
    build_signals_payload,
    build_watchlist_context,
    build_watchlist_payload,
    build_signal_detail_payload,
    build_radar_symbol_payload,
    build_settings_center_payload,
    ensure_mini_app_user,
    get_user_by_id,
    save_settings_center_payload,
    serialize_order_public,
    build_admin_manual_plan_lookup_payload,
    build_admin_user_lookup_payload,
    apply_admin_manual_plan_activation,
    apply_admin_user_moderation_action,
)
from app.observability import build_runtime_health_report, heartbeat, record_audit_event, start_background_heartbeat
from app.services.admin_runtime_service import (
    get_admin_operational_overview,
    get_admin_runtime_health_matrix,
    list_recent_audit_events,
    list_recent_incidents,
)
from app.services.admin_service import is_effectively_banned
from app.payment_service import cancel_payment_order, confirm_payment_order, create_payment_order, get_active_payment_order_for_user
from app.statistics import reset_statistics
from app.plans import normalize_plan, plan_status
from app.watchlist import add_symbol, normalize_many, remove_symbol, set_symbols, clear_watchlist
from app.risk import RiskConfigurationError, get_exchange_fee_preset, normalize_exchange_name, normalize_entry_mode, save_user_risk_profile

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


class MiniAppWatchlistSymbolRequest(BaseModel):
    symbol: str


class MiniAppWatchlistReplaceRequest(BaseModel):
    symbols: Optional[list[str]] = None
    raw: Optional[str] = None


class MiniAppAdminResetRequest(BaseModel):
    confirm: bool = False


class MiniAppAdminManualPlanActivationRequest(BaseModel):
    user_id: int
    plan: str
    days: int


class MiniAppAdminUserModerationRequest(BaseModel):
    user_id: int
    action: str
    duration_value: Optional[int] = None
    duration_unit: Optional[str] = None
    confirm: bool = False


class MiniAppRiskProfileUpdateRequest(BaseModel):
    capital_usdt: Optional[float] = None
    risk_percent: Optional[float] = None
    exchange: Optional[str] = None
    entry_mode: Optional[str] = None
    fee_percent_per_side: Optional[float] = None
    slippage_percent: Optional[float] = None
    default_leverage: Optional[float] = None
    default_profile: Optional[str] = None


class MiniAppSettingsUpdateRequest(BaseModel):
    language: Optional[str] = None
    push_alerts_enabled: Optional[bool] = None
    push_tiers: Optional[Dict[str, bool]] = None


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



def _resolve_watchlist_plan(user: Dict[str, Any]) -> str:
    status = plan_status(user)
    return normalize_plan(status.get("plan") or user.get("plan"))


def _sanitize_json_payload(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dict):
        return {key: _sanitize_json_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_json_payload(item) for item in value]
    return value


def create_mini_app() -> FastAPI:
    app = FastAPI(title="HADES Mini App", version="1.0.1")
    cors_origins = get_mini_app_cors_origins()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=bool(cors_origins),
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Requested-With", "X-Request-ID"],
        expose_headers=["X-Request-ID"],
    )

    @app.middleware("http")
    async def add_request_context(request: Request, call_next):
        request_id = (request.headers.get("X-Request-ID") or "").strip() or str(uuid4())
        request.state.request_id = request_id
        request.state.user_id = None
        try:
            response = await call_next(request)
        except HTTPException:
            raise
        except Exception as exc:
            user_id = getattr(request.state, "user_id", None)
            heartbeat("miniapp", status="degraded", details={"stage": "request_exception", "request_id": request_id})
            record_audit_event(
                event_type="miniapp_request_unhandled_exception",
                status="error",
                module="miniapp",
                user_id=user_id,
                message=str(exc),
                metadata={
                    "request_id": request_id,
                    "method": request.method,
                    "path": str(request.url.path),
                },
            )
            logger.error("❌ MiniApp request failed | request_id=%s path=%s error=%s", request_id, request.url.path, exc, exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "detail": "internal_server_error",
                    "request_id": request_id,
                },
            )
        response.headers["X-Request-ID"] = request_id
        return response

    @app.on_event("startup")
    async def on_startup() -> None:
        initialize_database()
        start_background_heartbeat(
            "miniapp",
            details_provider=lambda: {
                "stage": "running",
                "runtime_role": get_runtime_role(),
                "cors_origins": cors_origins,
                "dev_auth_enabled": is_mini_app_dev_auth_enabled(),
            },
        )
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
        record_audit_event(
            event_type="miniapp_started",
            status="info",
            module="miniapp",
            message="miniapp_started",
            metadata={
                "runtime_role": get_runtime_role(),
                "cors_origins": cors_origins,
                "dev_auth_enabled": is_mini_app_dev_auth_enabled(),
            },
        )

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        heartbeat("miniapp", status="stopped", details={"stage": "shutdown", "runtime_role": get_runtime_role()})
        record_audit_event(
            event_type="miniapp_stopped",
            status="warning",
            module="miniapp",
            message="miniapp_stopped",
            metadata={"runtime_role": get_runtime_role()},
        )

    app.mount("/miniapp/static", StaticFiles(directory=str(STATIC_DIR)), name="miniapp_static")

    def _get_bearer_token(authorization: Optional[str]) -> str:
        raw = (authorization or "").strip()
        if not raw.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="missing_bearer_token")
        return raw.split(" ", 1)[1].strip()

    def get_authenticated_user(request: Request, authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
        token = _get_bearer_token(authorization)
        try:
            payload = parse_session_token(token)
        except MiniAppAuthError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        user = get_user_by_id(int(payload.get("uid") or 0))
        if not user:
            raise HTTPException(status_code=401, detail="session_user_not_found")
        if is_effectively_banned(user):
            raise HTTPException(status_code=403, detail="user_banned")
        request.state.user_id = int(user.get("user_id") or 0)
        return user

    def get_authenticated_admin_user(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        user_id = int(user.get("user_id") or 0)
        if not is_admin(user_id):
            raise HTTPException(status_code=403, detail="admin_required")
        return user

    @app.get("/miniapp")
    async def miniapp_index() -> FileResponse:
        return FileResponse(str(INDEX_FILE))

    @app.get("/miniapp/health/live")
    async def miniapp_liveness() -> Dict[str, Any]:
        return {
            "ok": True,
            "service": "miniapp",
            "runtime_role": get_runtime_role(),
        }

    @app.get("/miniapp/health/ready")
    async def miniapp_readiness() -> JSONResponse:
        report = build_runtime_health_report(get_runtime_role())
        status_code = 200 if report.get("ok") else 503
        return JSONResponse(status_code=status_code, content=report)

    @app.get("/miniapp/health")
    async def miniapp_health() -> Dict[str, Any]:
        report = build_runtime_health_report(get_runtime_role())
        report.update({
            "service": "miniapp",
            "dev_auth_enabled": is_mini_app_dev_auth_enabled(),
            "cors_origins": cors_origins,
        })
        return report

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

    @app.get("/api/miniapp/account")
    async def miniapp_account(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_account_center_payload(user)

    @app.get("/api/miniapp/settings")
    async def miniapp_settings(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_settings_center_payload(user)

    @app.post("/api/miniapp/settings")
    async def miniapp_settings_update(payload: MiniAppSettingsUpdateRequest, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        try:
            return save_settings_center_payload(int(user.get("user_id") or 0), payload.model_dump(exclude_none=True))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/miniapp/signals")
    async def miniapp_signals(limit: int = 20, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return {"items": build_signals_payload(user, limit=limit)}


    @app.get("/api/miniapp/signals/{signal_id}")
    async def miniapp_signal_detail(signal_id: str, profile: str = "moderado", user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        payload = build_signal_detail_payload(user, signal_id, profile_name=profile)
        if not payload:
            raise HTTPException(status_code=404, detail="signal_not_found")
        return payload

    @app.get("/api/miniapp/history")
    async def miniapp_history(limit: int = 20, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return {"items": build_history_payload(user, limit=limit)}

    @app.get("/api/miniapp/market")
    async def miniapp_market(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_market_payload(user)

    @app.get("/api/miniapp/radar/{symbol}")
    async def miniapp_radar_detail(symbol: str, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        payload = build_radar_symbol_payload(user, symbol)
        if not payload:
            raise HTTPException(status_code=404, detail="radar_symbol_not_found")
        return payload

    @app.get("/api/miniapp/watchlist")
    async def miniapp_watchlist(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        return build_watchlist_context(user)

    @app.post("/api/miniapp/watchlist/add")
    async def miniapp_watchlist_add(payload: MiniAppWatchlistSymbolRequest, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        ok, message = add_symbol(int(user.get("user_id") or 0), payload.symbol, plan=_resolve_watchlist_plan(user))
        if not ok:
            record_audit_event(
                event_type="miniapp_watchlist_add_failed",
                status="warning",
                module="miniapp",
                user_id=int(user.get("user_id") or 0),
                message=message,
                metadata={"symbol": payload.symbol},
            )
            raise HTTPException(status_code=400, detail=message)
        record_audit_event(
            event_type="miniapp_watchlist_added",
            status="ok",
            module="miniapp",
            user_id=int(user.get("user_id") or 0),
            message=message,
            metadata={"symbol": payload.symbol},
        )
        return {"ok": True, "message": message, **build_watchlist_context(get_user_by_id(int(user.get("user_id") or 0)) or user)}

    @app.post("/api/miniapp/watchlist/remove")
    async def miniapp_watchlist_remove(payload: MiniAppWatchlistSymbolRequest, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        ok, message = remove_symbol(int(user.get("user_id") or 0), payload.symbol)
        if not ok:
            raise HTTPException(status_code=400, detail=message)
        record_audit_event(
            event_type="miniapp_watchlist_removed",
            status="ok",
            module="miniapp",
            user_id=int(user.get("user_id") or 0),
            message=message,
            metadata={"symbol": payload.symbol},
        )
        return {"ok": True, "message": message, **build_watchlist_context(get_user_by_id(int(user.get("user_id") or 0)) or user)}

    @app.post("/api/miniapp/watchlist/replace")
    async def miniapp_watchlist_replace(payload: MiniAppWatchlistReplaceRequest, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        symbols = payload.symbols or normalize_many(payload.raw or "")
        ok, message = set_symbols(int(user.get("user_id") or 0), symbols, plan=_resolve_watchlist_plan(user))
        if not ok:
            record_audit_event(
                event_type="miniapp_watchlist_replace_failed",
                status="warning",
                module="miniapp",
                user_id=int(user.get("user_id") or 0),
                message=message,
                metadata={"symbols_count": len(symbols)},
            )
            raise HTTPException(status_code=400, detail=message)
        record_audit_event(
            event_type="miniapp_watchlist_replaced",
            status="ok",
            module="miniapp",
            user_id=int(user.get("user_id") or 0),
            message=message,
            metadata={"symbols_count": len(symbols)},
        )
        return {"ok": True, "message": message, **build_watchlist_context(get_user_by_id(int(user.get("user_id") or 0)) or user)}

    @app.post("/api/miniapp/watchlist/clear")
    async def miniapp_watchlist_clear(user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        ok, message = clear_watchlist(int(user.get("user_id") or 0))
        if not ok:
            raise HTTPException(status_code=400, detail=message)
        record_audit_event(
            event_type="miniapp_watchlist_cleared",
            status="ok",
            module="miniapp",
            user_id=int(user.get("user_id") or 0),
            message=message,
        )
        return {"ok": True, "message": message, **build_watchlist_context(get_user_by_id(int(user.get("user_id") or 0)) or user)}

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
        if isinstance(result, dict) and result.get("order") is not None:
            result = dict(result)
            result["order"] = serialize_order_public(result.get("order"))
        return _sanitize_json_payload(result)

    @app.post("/api/miniapp/payment-order/cancel")
    async def miniapp_cancel_payment(payload: MiniAppPaymentActionRequest, user: Dict[str, Any] = Depends(get_authenticated_user)) -> Dict[str, Any]:
        cancelled = cancel_payment_order(payload.order_id, int(user.get("user_id") or 0))
        return {"ok": cancelled}

    @app.get("/api/miniapp/performance")
    async def miniapp_performance_center(
        days: int = 30,
        user: Dict[str, Any] = Depends(get_authenticated_user),
    ) -> Dict[str, Any]:
        if int(days) not in {7, 30, 3650}:
            raise HTTPException(status_code=400, detail="unsupported_performance_window")
        return build_performance_center_payload(user, focus_days=int(days))

    @app.get("/api/miniapp/risk")
    async def miniapp_risk_center(
        signal_id: Optional[str] = None,
        profile: Optional[str] = None,
        leverage: Optional[float] = None,
        user: Dict[str, Any] = Depends(get_authenticated_user),
    ) -> Dict[str, Any]:
        return build_risk_center_payload(
            user,
            signal_id=signal_id,
            profile_name=profile,
            override_leverage=leverage,
        )

    @app.post("/api/miniapp/risk/profile")
    async def miniapp_update_risk_profile(
        payload: MiniAppRiskProfileUpdateRequest,
        user: Dict[str, Any] = Depends(get_authenticated_user),
    ) -> Dict[str, Any]:
        raw_patch = payload.model_dump(exclude_none=True)
        if not raw_patch:
            raise HTTPException(status_code=400, detail="empty_risk_patch")

        user_id = int(user.get("user_id") or 0)
        effective_plan = normalize_plan(plan_status(user).get("plan") or user.get("plan"))
        patch = dict(raw_patch)

        current_exchange = normalize_exchange_name(patch.get("exchange") or "") if patch.get("exchange") is not None else None
        current_entry_mode = normalize_entry_mode(patch.get("entry_mode") or "") if patch.get("entry_mode") is not None else None
        if current_exchange or current_entry_mode:
            current_payload = build_risk_center_payload(user)
            current_profile = current_payload.get("profile") or {}
            preset = get_exchange_fee_preset(
                current_exchange or current_profile.get("exchange"),
                current_entry_mode or current_profile.get("entry_mode"),
            )
            patch["exchange"] = preset["exchange"]
            patch["entry_mode"] = preset["entry_mode"]
            if "fee_percent_per_side" not in patch:
                patch["fee_percent_per_side"] = preset["fee_percent_per_side"]
            if "slippage_percent" not in patch:
                patch["slippage_percent"] = preset["slippage_percent"]

        if effective_plan == "free":
            patch["default_profile"] = "moderado"
        elif "default_profile" in patch and patch["default_profile"] is not None:
            patch["default_profile"] = str(patch["default_profile"]).strip().lower()

        try:
            save_user_risk_profile(user_id, patch)
        except RiskConfigurationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        record_audit_event(
            event_type="miniapp_risk_profile_updated",
            status="ok",
            module="miniapp",
            user_id=user_id,
            metadata={"fields": sorted(list(patch.keys()))},
        )
        return build_risk_center_payload(user)

    @app.get("/api/miniapp/admin/user-lookup")
    async def miniapp_admin_user_lookup(
        user_id: int,
        admin_user: Dict[str, Any] = Depends(get_authenticated_admin_user),
    ) -> Dict[str, Any]:
        try:
            payload = build_admin_user_lookup_payload(int(user_id), admin_user_id=int(admin_user.get("user_id") or 0))
        except ValueError as exc:
            message = str(exc)
            if message == "user_not_found":
                raise HTTPException(status_code=404, detail=message) from exc
            raise HTTPException(status_code=400, detail=message) from exc
        payload["requested_by"] = int(admin_user.get("user_id") or 0)
        return _sanitize_json_payload(payload)

    @app.post("/api/miniapp/admin/manual-plan-activation")
    async def miniapp_admin_manual_plan_activation(
        payload: MiniAppAdminManualPlanActivationRequest,
        admin_user: Dict[str, Any] = Depends(get_authenticated_admin_user),
    ) -> Dict[str, Any]:
        admin_id = int(admin_user.get("user_id") or 0)
        try:
            result = apply_admin_manual_plan_activation(
                admin_user_id=admin_id,
                target_user_id=int(payload.user_id),
                plan=payload.plan,
                days=int(payload.days),
            )
        except ValueError as exc:
            message = str(exc)
            status_code = 404 if message == "user_not_found" else 400
            raise HTTPException(status_code=status_code, detail=message) from exc

        record_audit_event(
            event_type="miniapp_admin_manual_plan_activation",
            status="ok",
            module="miniapp_admin",
            user_id=int(payload.user_id),
            admin_id=admin_id,
            message="miniapp_admin_manual_plan_activation",
            metadata={
                "target_user_id": int(payload.user_id),
                "plan": str(result.get("activation", {}).get("plan") or payload.plan),
                "days": int(result.get("activation", {}).get("days") or payload.days),
                "before_plan": result.get("before", {}).get("plan"),
                "after_plan": result.get("target", {}).get("plan"),
            },
        )
        return _sanitize_json_payload(result)


    @app.post("/api/miniapp/admin/user-moderation")
    async def miniapp_admin_user_moderation(
        payload: MiniAppAdminUserModerationRequest,
        admin_user: Dict[str, Any] = Depends(get_authenticated_admin_user),
    ) -> Dict[str, Any]:
        admin_id = int(admin_user.get("user_id") or 0)
        normalized_action = str(payload.action or "").strip().lower()
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="confirm_required")
        try:
            result = apply_admin_user_moderation_action(
                admin_user_id=admin_id,
                target_user_id=int(payload.user_id),
                action=normalized_action,
                duration_value=payload.duration_value,
                duration_unit=payload.duration_unit,
            )
        except ValueError as exc:
            message = str(exc)
            status_code = 404 if message == "user_not_found" else 400
            raise HTTPException(status_code=status_code, detail=message) from exc

        metadata = {
            "target_user_id": int(payload.user_id),
            "action": normalized_action,
        }
        if payload.duration_value is not None:
            metadata["duration_value"] = int(payload.duration_value)
        if payload.duration_unit is not None:
            metadata["duration_unit"] = str(payload.duration_unit)

        record_audit_event(
            event_type=f"miniapp_admin_user_{normalized_action}",
            status="warning" if normalized_action in {"ban_temporary", "ban_permanent", "delete"} else "ok",
            module="miniapp_admin",
            user_id=int(payload.user_id),
            admin_id=admin_id,
            message=f"miniapp_admin_user_{normalized_action}",
            metadata=_sanitize_json_payload(metadata),
        )
        return _sanitize_json_payload(result)

    @app.get("/api/miniapp/admin/overview")
    async def miniapp_admin_overview(admin_user: Dict[str, Any] = Depends(get_authenticated_admin_user)) -> Dict[str, Any]:
        payload = get_admin_operational_overview()
        payload["requested_by"] = int(admin_user.get("user_id") or 0)
        return payload

    @app.get("/api/miniapp/admin/health")
    async def miniapp_admin_health(admin_user: Dict[str, Any] = Depends(get_authenticated_admin_user)) -> Dict[str, Any]:
        payload = get_admin_runtime_health_matrix()
        payload["requested_by"] = int(admin_user.get("user_id") or 0)
        return payload

    @app.get("/api/miniapp/admin/audit")
    async def miniapp_admin_audit(
        limit: int = 25,
        status: Optional[str] = None,
        module: Optional[str] = None,
        admin_user: Dict[str, Any] = Depends(get_authenticated_admin_user),
    ) -> Dict[str, Any]:
        try:
            payload = list_recent_audit_events(limit=limit, status=status, module=module)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        payload["requested_by"] = int(admin_user.get("user_id") or 0)
        return payload

    @app.get("/api/miniapp/admin/incidents")
    async def miniapp_admin_incidents(
        limit: int = 25,
        admin_user: Dict[str, Any] = Depends(get_authenticated_admin_user),
    ) -> Dict[str, Any]:
        payload = list_recent_incidents(limit=limit)
        payload["requested_by"] = int(admin_user.get("user_id") or 0)
        return payload

    @app.post("/api/miniapp/admin/reset-results")
    async def miniapp_admin_reset_results(
        payload: MiniAppAdminResetRequest,
        admin_user: Dict[str, Any] = Depends(get_authenticated_admin_user),
    ) -> Dict[str, Any]:
        admin_id = int(admin_user.get("user_id") or 0)
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="confirm_required")
        summary = reset_statistics(preserve_signals=False)
        record_audit_event(
            event_type="miniapp_admin_results_reset",
            status="warning",
            module="miniapp_admin",
            user_id=admin_id,
            admin_id=admin_id,
            message="miniapp_admin_results_reset",
            metadata=_sanitize_json_payload(summary),
        )
        return {
            "ok": True,
            "requested_by": admin_id,
            "summary": _sanitize_json_payload(summary),
        }

    return app
