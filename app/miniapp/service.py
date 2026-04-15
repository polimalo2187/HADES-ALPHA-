from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import time
from math import isfinite, log10
from typing import Any, Dict, Iterable, List, Optional

from app.binance_api import get_futures_24h_tickers, get_open_interest, get_premium_index, get_radar_opportunities
from app.services.market_data_service import get_funding_rate_pct_map, get_open_interest_map
from app.config import get_bot_username, get_payment_configuration_status, get_payment_min_confirmations, is_admin
from app.i18n import normalize_language
from app.database import payment_orders_collection, subscription_events_collection, users_collection, user_signals_collection, watchlists_collection
from app.history_service import get_history_entries_for_user
from app.market import get_market_state_snapshot
from app.payment_service import get_active_payment_order_for_user
from app.referrals import get_referral_link, get_referral_reward_rules, get_user_referral_stats
from app.plans import (
    PLAN_FREE,
    PLAN_PLUS,
    PLAN_PREMIUM,
    get_plan_catalog,
    get_plan_name,
    grant_plan_entitlement,
    normalize_plan,
    plan_status,
    validate_entitlement_days,
)
from app.statistics import build_performance_window, get_materialized_window, get_performance_snapshot
from app.user_service import get_or_create_user
from app.models import utcnow
from app.watchlist import get_watchlist, get_watchlist_limit_for_plan
from app.signals import get_signal_analysis_for_user, get_signal_tracking_for_user, get_user_signal_by_signal_id
from app.risk import (
    ENTRY_MODE_LABELS,
    RiskConfigurationError,
    SignalProfileError,
    SignalRiskError,
    build_risk_preview_from_user_signal,
    get_exchange_fee_preset,
    get_risk_profile_label,
    get_user_risk_profile,
    normalize_risk_profile,
    ensure_risk_profile_ready,
)
from app.services.admin_service import (
    apply_permanent_ban,
    apply_temporary_ban,
    can_block_target,
    can_delete_target,
    clear_expired_ban,
    delete_user_data,
    remove_ban,
    resolve_ban_state,
)


_RADAR_SCAN_CACHE: Dict[str, tuple[float, Dict[str, Any]]] = {}
_RADAR_SCAN_TTL_SECONDS = 120
_MARKET_PAYLOAD_CACHE: Dict[str, tuple[float, Dict[str, Any]]] = {}
_MARKET_PAYLOAD_TTL_SECONDS = 20
_RADAR_DETAIL_CACHE: Dict[str, tuple[float, Dict[str, Any]]] = {}
_RADAR_DETAIL_TTL_SECONDS = 20


def _cache_get_payload(cache: Dict[str, tuple[float, Dict[str, Any]]], key: str) -> Optional[Dict[str, Any]]:
    item = cache.get(key)
    if not item:
        return None
    expires_at, payload = item
    if time.time() >= expires_at:
        cache.pop(key, None)
        return None
    return deepcopy(payload)


def _cache_set_payload(cache: Dict[str, tuple[float, Dict[str, Any]]], key: str, payload: Dict[str, Any], ttl_seconds: int) -> None:
    cache[key] = (time.time() + max(1, int(ttl_seconds)), deepcopy(payload))


def _cache_get_radar_scan(symbol: str) -> Optional[Dict[str, Any]]:
    key = str(symbol or '').upper().strip()
    item = _RADAR_SCAN_CACHE.get(key)
    if not item:
        return None
    expires_at, payload = item
    if time.time() >= expires_at:
        _RADAR_SCAN_CACHE.pop(key, None)
        return None
    return deepcopy(payload)


def _cache_set_radar_scan(symbol: str, payload: Dict[str, Any]) -> None:
    _RADAR_SCAN_CACHE[str(symbol or '').upper().strip()] = (time.time() + _RADAR_SCAN_TTL_SECONDS, deepcopy(payload))

def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _label_subscription_status(value: Any) -> str:
    normalized = str(value or "free").lower().strip()
    mapping = {
        "free": "Free",
        "trial": "Trial",
        "active": "Activo",
        "expired": "Expirado",
        "banned": "Bloqueado",
    }
    return mapping.get(normalized, normalized.title())


def _settings_language_options() -> List[Dict[str, str]]:
    return [
        {"value": "es", "label": "Español"},
        {"value": "en", "label": "English"},
    ]


def _effective_plan_for_preferences(user: Dict[str, Any]) -> str:
    status = plan_status(user)
    return normalize_plan(status.get("plan") or user.get("plan"))


def _accessible_push_tiers(plan_value: Optional[str]) -> List[str]:
    normalized = normalize_plan(plan_value)
    if normalized == "premium":
        return ["free", "plus", "premium"]
    if normalized == "plus":
        return ["free", "plus"]
    return ["free"]


def _push_tier_label(tier: str) -> str:
    return {
        "free": "Free",
        "plus": "Plus",
        "premium": "Premium",
    }.get(str(tier or "").lower(), str(tier or "—").upper())


def _load_user_settings(user: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw = (user or {}).get("miniapp_settings")
    return dict(raw) if isinstance(raw, dict) else {}


def _serialize_push_preferences(user: Dict[str, Any]) -> Dict[str, Any]:
    effective_plan = _effective_plan_for_preferences(user)
    accessible_tiers = _accessible_push_tiers(effective_plan)
    settings = _load_user_settings(user)
    push_settings = settings.get("push_alerts") if isinstance(settings.get("push_alerts"), dict) else {}
    stored_tiers = push_settings.get("tiers") if isinstance(push_settings.get("tiers"), dict) else {}
    enabled = bool(push_settings.get("enabled", True))

    tiers: List[Dict[str, Any]] = []
    selected_tiers: List[str] = []
    for tier in ["free", "plus", "premium"]:
        available = tier in accessible_tiers
        selected = bool(stored_tiers.get(tier, available)) if available else False
        if selected:
            selected_tiers.append(tier)
        tiers.append({
            "key": tier,
            "label": _push_tier_label(tier),
            "available": available,
            "selected": selected,
            "locked_reason": None if available else f"Disponible desde {_push_tier_label(tier)}",
        })

    if not enabled:
        summary = "Push silenciado. No recibirás avisos hasta volver a activarlos."
    elif selected_tiers:
        summary = f"Recibirás pushes para: {' / '.join(_push_tier_label(item) for item in selected_tiers)}."
    else:
        summary = "No hay niveles seleccionados. No recibirás avisos hasta activar al menos uno."

    return {
        "enabled": enabled,
        "tiers": tiers,
        "selected_tiers": selected_tiers,
        "available_tiers": accessible_tiers,
        "summary": summary,
        "plan_scope_label": get_plan_name(effective_plan),
    }


def build_settings_center_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    me = build_me_payload(user)
    push_preferences = _serialize_push_preferences(user)
    return {
        "overview": {
            **me,
            "effective_plan": _effective_plan_for_preferences(user),
        },
        "language": {
            "current": normalize_language(user.get("language") or "es"),
            "options": _settings_language_options(),
        },
        "push_alerts": push_preferences,
        "support": {
            "summary": "Configura idioma y qué niveles de señal quieres recibir como push en Telegram.",
            "note": "Los pushes siguen siendo avisos simples y el detalle completo vive dentro de la MiniApp.",
        },
        "generated_at": utcnow().isoformat(),
    }


def save_settings_center_payload(user_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
    user = get_user_by_id(int(user_id))
    if not user:
        raise ValueError("user_not_found")

    effective_plan = _effective_plan_for_preferences(user)
    accessible_tiers = set(_accessible_push_tiers(effective_plan))
    update_doc: Dict[str, Any] = {"updated_at": utcnow(), "last_activity": utcnow()}

    if "language" in patch and patch.get("language") is not None:
        update_doc["language"] = normalize_language(patch.get("language"))

    if "push_alerts_enabled" in patch and patch.get("push_alerts_enabled") is not None:
        update_doc["miniapp_settings.push_alerts.enabled"] = bool(patch.get("push_alerts_enabled"))

    if "push_tiers" in patch and isinstance(patch.get("push_tiers"), dict):
        requested_tiers = patch.get("push_tiers") or {}
        for tier in ["free", "plus", "premium"]:
            allowed = tier in accessible_tiers
            selected = bool(requested_tiers.get(tier, allowed)) if allowed else False
            update_doc[f"miniapp_settings.push_alerts.tiers.{tier}"] = selected

    if len(update_doc) <= 2:
        return build_settings_center_payload(user)

    users_collection().update_one({"user_id": int(user_id)}, {"$set": update_doc})
    refreshed = get_user_by_id(int(user_id)) or user
    return build_settings_center_payload(refreshed)


def _admin_manual_free_allowed(user: Dict[str, Any]) -> bool:
    status = plan_status(user)
    effective_plan = normalize_plan(status.get("plan") or user.get("plan"))
    subscription_status = str(status.get("status") or "free").lower().strip()
    return effective_plan == PLAN_FREE and subscription_status in {"free", "expired"}


def _serialize_admin_target_user(user: Dict[str, Any]) -> Dict[str, Any]:
    clear_expired_ban(int(user.get("user_id") or 0))
    refreshed_user = get_user_by_id(int(user.get("user_id") or 0)) or user
    status = plan_status(refreshed_user)
    effective_plan = normalize_plan(status.get("plan") or refreshed_user.get("plan"))
    ban_state = resolve_ban_state(refreshed_user)
    return {
        "user_id": int(refreshed_user.get("user_id") or 0),
        "username": refreshed_user.get("username"),
        "language": normalize_language(refreshed_user.get("language") or "es"),
        "is_admin": bool(is_admin(int(refreshed_user.get("user_id") or 0))),
        "banned": bool(ban_state.get("active")),
        "ban_active": bool(ban_state.get("active")),
        "ban_mode": ban_state.get("mode"),
        "ban_label": ban_state.get("label"),
        "ban_until": ban_state.get("until"),
        "plan": effective_plan,
        "plan_name": get_plan_name(effective_plan),
        "subscription_status": str(status.get("status") or "free"),
        "subscription_status_label": _label_subscription_status(status.get("status") or "free"),
        "days_left": int(status.get("days_left") or 0),
        "expires_at": _iso(status.get("expires")),
        "trial_end": _iso(refreshed_user.get("trial_end")),
        "plan_end": _iso(refreshed_user.get("plan_end")),
        "free_manual_allowed": _admin_manual_free_allowed(refreshed_user),
    }


def _admin_manual_plan_options(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    free_allowed = _admin_manual_free_allowed(user)
    options: List[Dict[str, Any]] = []
    for key in [PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM]:
        available = True
        disabled_reason = None
        if key == PLAN_FREE and not free_allowed:
            available = False
            disabled_reason = "Free manual solo aplica a usuarios Free cuyo trial ya expiró."
        options.append({
            "key": key,
            "label": get_plan_name(key),
            "available": available,
            "disabled_reason": disabled_reason,
        })
    return options




def _admin_moderation_actions(user: Dict[str, Any], *, admin_user_id: int) -> Dict[str, Any]:
    target_user_id = int(user.get("user_id") or 0)
    can_block, block_reason = can_block_target(int(admin_user_id), target_user_id)
    can_delete, delete_reason = can_delete_target(int(admin_user_id), target_user_id)
    state = resolve_ban_state(user)
    return {
        "can_temporary_ban": bool(can_block and not state.get("active")),
        "can_permanent_ban": bool(can_block and not state.get("active")),
        "can_unban": bool(can_block and state.get("active")),
        "can_delete": bool(can_delete),
        "block_restriction": block_reason,
        "delete_restriction": delete_reason,
        "supported_temp_units": ["hours", "days", "weeks"],
    }


def build_admin_user_lookup_payload(target_user_id: int, *, admin_user_id: int) -> Dict[str, Any]:
    user = get_user_by_id(int(target_user_id))
    if not user:
        raise ValueError("user_not_found")
    return {
        **build_admin_manual_plan_lookup_payload(int(target_user_id)),
        "moderation": _admin_moderation_actions(user, admin_user_id=int(admin_user_id)),
    }


def apply_admin_user_moderation_action(
    *,
    admin_user_id: int,
    target_user_id: int,
    action: str,
    duration_value: Optional[int] = None,
    duration_unit: Optional[str] = None,
) -> Dict[str, Any]:
    user = get_user_by_id(int(target_user_id))
    if not user:
        raise ValueError("user_not_found")

    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"ban_temporary", "ban_permanent", "unban", "delete"}:
        raise ValueError("unsupported_action")

    before = _serialize_admin_target_user(user)
    if normalized_action in {"ban_temporary", "ban_permanent", "unban"}:
        ok, reason = can_block_target(int(admin_user_id), int(target_user_id))
        if not ok:
            raise ValueError("cannot_target_self" if reason == "self" else "cannot_target_admin")
    if normalized_action == "delete":
        ok, reason = can_delete_target(int(admin_user_id), int(target_user_id))
        if not ok:
            raise ValueError("cannot_target_self" if reason == "self" else "cannot_target_admin")

    action_summary: Dict[str, Any]
    if normalized_action == "ban_temporary":
        if duration_value is None:
            raise ValueError("ban_duration_required")
        action_summary = apply_temporary_ban(
            target_user_id=int(target_user_id),
            banned_by=int(admin_user_id),
            duration_value=int(duration_value),
            duration_unit=str(duration_unit or "days"),
        )
        refreshed = get_user_by_id(int(target_user_id))
        if not refreshed:
            raise ValueError("user_not_found")
        after = _serialize_admin_target_user(refreshed)
    elif normalized_action == "ban_permanent":
        action_summary = apply_permanent_ban(target_user_id=int(target_user_id), banned_by=int(admin_user_id))
        refreshed = get_user_by_id(int(target_user_id))
        if not refreshed:
            raise ValueError("user_not_found")
        after = _serialize_admin_target_user(refreshed)
    elif normalized_action == "unban":
        action_summary = remove_ban(target_user_id=int(target_user_id), unbanned_by=int(admin_user_id))
        refreshed = get_user_by_id(int(target_user_id))
        if not refreshed:
            raise ValueError("user_not_found")
        after = _serialize_admin_target_user(refreshed)
    else:
        action_summary = delete_user_data(target_user_id=int(target_user_id), deleted_by=int(admin_user_id))
        after = None

    latest_user = get_user_by_id(int(target_user_id)) if normalized_action != "delete" else None
    plan_options = _admin_manual_plan_options(latest_user) if latest_user else []
    moderation = _admin_moderation_actions(latest_user, admin_user_id=int(admin_user_id)) if latest_user else {
        "can_temporary_ban": False,
        "can_permanent_ban": False,
        "can_unban": False,
        "can_delete": False,
        "supported_temp_units": ["hours", "days", "weeks"],
    }
    return {
        "ok": True,
        "requested_by": int(admin_user_id),
        "action": normalized_action,
        "before": before,
        "target": after,
        "action_summary": action_summary,
        "plan_options": plan_options,
        "moderation": moderation,
        "generated_at": utcnow().isoformat(),
    }

def build_admin_manual_plan_lookup_payload(target_user_id: int) -> Dict[str, Any]:
    user = get_user_by_id(int(target_user_id))
    if not user:
        raise ValueError("user_not_found")
    target = _serialize_admin_target_user(user)
    return {
        "target": target,
        "plan_options": _admin_manual_plan_options(user),
        "rules": {
            "free_manual_summary": "Free manual solo aplica a usuarios Free con el trial vencido.",
            "plus_premium_summary": "Plus y Premium permiten activación manual por la cantidad exacta de días que defina el admin.",
        },
        "generated_at": utcnow().isoformat(),
    }


def apply_admin_manual_plan_activation(
    *,
    admin_user_id: int,
    target_user_id: int,
    plan: str,
    days: int,
) -> Dict[str, Any]:
    user = get_user_by_id(int(target_user_id))
    if not user:
        raise ValueError("user_not_found")

    plan_value = normalize_plan(plan)
    if plan_value not in {PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM}:
        raise ValueError("unsupported_plan")

    day_value = validate_entitlement_days(days)

    if plan_value == PLAN_FREE and not _admin_manual_free_allowed(user):
        raise ValueError("free_manual_requires_expired_free")

    before = _serialize_admin_target_user(user)
    success = grant_plan_entitlement(
        int(target_user_id),
        target_plan=plan_value,
        days=day_value,
        source="miniapp_admin_manual",
        reason="manual_activation",
        metadata={
            "admin_id": int(admin_user_id),
            "manual_days": int(day_value),
            "target_plan": plan_value,
        },
    )
    if not success:
        raise ValueError("activation_failed")

    refreshed = get_user_by_id(int(target_user_id))
    if not refreshed:
        raise ValueError("user_not_found")

    after = _serialize_admin_target_user(refreshed)
    return {
        "ok": True,
        "requested_by": int(admin_user_id),
        "activation": {
            "plan": plan_value,
            "plan_name": get_plan_name(plan_value),
            "days": int(day_value),
        },
        "before": before,
        "target": after,
        "plan_options": _admin_manual_plan_options(refreshed),
        "generated_at": utcnow().isoformat(),
    }


def _label_order_status(value: Any) -> str:
    normalized = str(value or "awaiting_payment").lower().strip()
    mapping = {
        "awaiting_payment": "Esperando pago",
        "verification_in_progress": "Verificando",
        "paid_unconfirmed": "Pago sin confirmar",
        "completed": "Completado",
        "cancelled": "Cancelado",
        "expired": "Expirada",
    }
    return mapping.get(normalized, normalized.replace("_", " ").title())


def _minutes_until(value: Any) -> Optional[int]:
    if not isinstance(value, datetime):
        return None
    delta = int((value - utcnow()).total_seconds())
    return max(delta // 60, 0)


def _time_left_label(minutes: Optional[int]) -> str:
    if minutes is None:
        return "Sin horario"
    if minutes <= 0:
        return "Expirada"
    if minutes < 60:
        return f"{minutes} min"
    hours, rem = divmod(int(minutes), 60)
    if hours < 24:
        return f"{hours} h {rem} min" if rem else f"{hours} h"
    days, rem_hours = divmod(hours, 24)
    return f"{days} d {rem_hours} h" if rem_hours else f"{days} d"


def _billing_steps_for_order(order: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    steps = [
        {"key": "created", "label": "Orden", "state": "done" if order else "upcoming"},
        {"key": "fund", "label": "Enviar monto", "state": "upcoming"},
        {"key": "verify", "label": "Verificación", "state": "upcoming"},
        {"key": "activate", "label": "Activación", "state": "upcoming"},
    ]
    if not order:
        return steps
    status = str(order.get("status") or "awaiting_payment").lower().strip()
    if status == "awaiting_payment":
        steps[1]["state"] = "current"
    elif status == "verification_in_progress":
        steps[1]["state"] = "done"
        steps[2]["state"] = "current"
    elif status == "paid_unconfirmed":
        steps[1]["state"] = "done"
        steps[2]["state"] = "current"
    elif status == "completed":
        for item in steps[1:]:
            item["state"] = "done"
        steps[3]["state"] = "done"
    elif status in {"cancelled", "expired"}:
        steps[1]["state"] = "blocked"
    return steps


def _build_billing_focus(*, payment_config_ready: bool, active_order: Optional[Dict[str, Any]], billing_summary: Dict[str, Any], subscription: Dict[str, Any], payment_config_status: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    required_confirmations = int(get_payment_min_confirmations() or 3)
    base = {
        "state": "idle",
        "tone": "neutral",
        "title": "Centro de billing listo",
        "headline": "Sin orden abierta por ahora",
        "message": "Puedes renovar o hacer upgrade desde los planes disponibles.",
        "can_create_order": bool(payment_config_ready),
        "required_confirmations": required_confirmations,
        "steps": _billing_steps_for_order(active_order),
        "primary_cta": "Generar orden",
        "hint": None,
    }
    if not payment_config_ready:
        status = payment_config_status or get_payment_configuration_status()
        checks = list(status.get("checks") or [])
        missing_keys = [str(item) for item in (status.get("missing_keys") or []) if item]
        missing_labels = [
            str(item.get("label") or item.get("key") or "").strip()
            for item in checks
            if not item.get("value_present")
        ]
        base.update({
            "state": "config_missing",
            "tone": "warning",
            "title": "Pagos no disponibles",
            "headline": "La configuración de cobro está incompleta",
            "message": "No generes órdenes hasta que la configuración BEP-20 esté lista.",
            "can_create_order": False,
            "primary_cta": "Soporte",
            "hint": "Revisa wallet, contrato y RPC antes de cobrar.",
            "config_checks": checks,
            "missing_keys": missing_keys,
            "missing_labels": missing_labels,
        })
        return base

    if active_order:
        status = str(active_order.get("status") or "awaiting_payment").lower().strip()
        confirmations = int(active_order.get("confirmations") or 0)
        minutes_left = _minutes_until(active_order.get("expires_at"))
        time_left = _time_left_label(minutes_left)
        plan_name = active_order.get("plan_name") or get_plan_name(active_order.get("plan"))
        headline = f"{plan_name} · {int(active_order.get('days') or 0)} días · {active_order.get('amount_usdt')} USDT"
        base.update({
            "state": status,
            "headline": headline,
            "expires_in_minutes": minutes_left,
            "time_left_label": time_left,
            "steps": _billing_steps_for_order(active_order),
            "confirmations": confirmations,
        })
        if status == "awaiting_payment":
            base.update({
                "tone": "accent",
                "title": "Orden abierta y lista para pago",
                "message": f"Envía el monto exacto por BEP-20 y luego confirma dentro de la MiniApp. Tiempo restante: {time_left}.",
                "primary_cta": "Confirmar pago",
                "hint": "Si generas otra orden distinta, esta se reemplaza.",
            })
        elif status == "verification_in_progress":
            base.update({
                "tone": "warning",
                "title": "Verificación en curso",
                "message": "Ya hay una verificación corriendo. No envíes otro pago ni generes otra orden hasta que termine.",
                "primary_cta": "Esperando verificación",
                "hint": "Si tocaste dos veces, la segunda solicitud no debería duplicar nada.",
            })
        elif status == "paid_unconfirmed":
            missing = max(required_confirmations - confirmations, 0)
            base.update({
                "tone": "positive",
                "title": "Pago detectado, esperando confirmaciones",
                "message": f"La red detectó tu pago. Confirmaciones actuales: {confirmations}/{required_confirmations}. Faltan {missing}.",
                "primary_cta": "Revisar de nuevo",
                "hint": "No reenvíes fondos. Solo espera más confirmaciones y vuelve a confirmar luego.",
            })
        return base

    open_orders = int((billing_summary or {}).get("open") or 0)
    days_left = int((subscription or {}).get("days_left") or 0)
    if open_orders > 0:
        base.update({
            "state": "open_without_payload",
            "tone": "warning",
            "title": "Hay órdenes abiertas",
            "headline": f"Órdenes abiertas: {open_orders}",
            "message": "Refresca la cuenta o revisa el estado antes de crear otra orden.",
            "primary_cta": "Refrescar cuenta",
        })
    elif days_left <= 3:
        base.update({
            "state": "renew_soon",
            "tone": "warning",
            "title": "Renovación recomendada",
            "headline": f"Tu acceso vence en {days_left} días",
            "message": "Conviene dejar la renovación lista antes del vencimiento para no perder continuidad.",
            "primary_cta": "Renovar",
        })
    else:
        current_plan = get_plan_name((subscription or {}).get("plan"))
        base.update({
            "headline": f"Plan actual: {current_plan}",
            "message": "Puedes renovar tu plan o hacer upgrade desde los bloques de Plus y Premium.",
        })
    return base




def _plan_features(plan: str) -> list[str]:
    plan_value = normalize_plan(plan)
    if plan_value == "premium":
        return [
            "Señales Free + Plus + Premium",
            "Historial completo y premium",
            "Radar y mercado ampliado",
            "Seguimiento y análisis avanzado",
        ]
    if plan_value == "plus":
        return [
            "Señales Free + Plus",
            "Historial y watchlist pro",
            "Mercado y radar ampliado",
            "Cobertura operativa mejorada",
        ]
    return ["Acceso básico"]


def serialize_order_public(order: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not order:
        return None
    base_price = order.get("base_price_usdt")
    amount = order.get("amount_usdt")
    unique_delta = None
    try:
        if base_price is not None and amount is not None:
            unique_delta = round(float(amount) - float(base_price), 3)
    except Exception:
        unique_delta = None
    plan = normalize_plan(order.get("plan"))
    expires_at = order.get("expires_at")
    expires_in_minutes = _minutes_until(expires_at)
    status = str(order.get("status") or "awaiting_payment").lower().strip()
    return {
        "order_id": order.get("order_id"),
        "plan": plan,
        "plan_name": get_plan_name(plan),
        "days": order.get("days"),
        "base_price_usdt": base_price,
        "amount_usdt": amount,
        "amount_unique_delta": unique_delta,
        "network": order.get("network"),
        "token_symbol": order.get("token_symbol"),
        "deposit_address": order.get("deposit_address"),
        "status": status,
        "status_label": _label_order_status(status),
        "last_verification_reason": order.get("last_verification_reason"),
        "confirmations": int(order.get("confirmations") or 0),
        "expires_at": _iso(expires_at),
        "expires_in_minutes": expires_in_minutes,
        "time_left_label": _time_left_label(expires_in_minutes),
        "is_open": status in {"awaiting_payment", "verification_in_progress", "paid_unconfirmed"},
        "steps": _billing_steps_for_order(order),
        "created_at": _iso(order.get("created_at")),
        "updated_at": _iso(order.get("updated_at")),
        "confirmed_at": _iso(order.get("confirmed_at")),
    }


def _label_subscription_event(value: Any) -> str:
    normalized = str(value or "update").lower().strip()
    mapping = {
        "purchase": "Compra aplicada",
        "manual_activation": "Activación manual",
        "referral_reward": "Recompensa por referido",
        "extend": "Extensión",
        "expired": "Expiración",
    }
    return mapping.get(normalized, normalized.replace("_", " ").title())


def _serialize_subscription_event(doc: Dict[str, Any]) -> Dict[str, Any]:
    plan = normalize_plan(doc.get("plan") or doc.get("after_plan"))
    before_plan = normalize_plan(doc.get("before_plan"))
    after_plan = normalize_plan(doc.get("after_plan") or doc.get("plan"))
    return {
        "event_type": str(doc.get("event_type") or "update"),
        "event_label": _label_subscription_event(doc.get("event_type")),
        "plan": plan,
        "plan_name": get_plan_name(plan),
        "days": int(doc.get("days") or 0),
        "source": doc.get("source"),
        "before_plan": before_plan,
        "before_plan_name": get_plan_name(before_plan),
        "after_plan": after_plan,
        "after_plan_name": get_plan_name(after_plan),
        "created_at": _iso(doc.get("created_at")),
        "metadata": dict(doc.get("metadata") or {}),
    }


def _serialize_referral_reward(doc: Dict[str, Any]) -> Dict[str, Any]:
    activated_plan = normalize_plan(doc.get("activated_plan"))
    reward_plan = normalize_plan(doc.get("reward_plan_applied") or activated_plan)
    return {
        "referred_id": int(doc.get("referred_id") or 0),
        "activated_plan": activated_plan,
        "activated_plan_name": get_plan_name(activated_plan),
        "activated_days": int(doc.get("activated_days") or 0),
        "reward_plan": reward_plan,
        "reward_plan_name": get_plan_name(reward_plan),
        "reward_days": int(doc.get("reward_days_applied") or 0),
        "created_at": _iso(doc.get("created_at") or doc.get("activated_at")),
    }


def _load_recent_payment_orders(user_id: int, *, limit: int = 6) -> List[Dict[str, Any]]:
    return list(
        payment_orders_collection()
        .find({"user_id": int(user_id)})
        .sort("created_at", -1)
        .limit(max(1, int(limit)))
    )


def _load_payment_order_summary(user_id: int) -> Dict[str, int]:
    coll = payment_orders_collection()
    user_id = int(user_id)
    return {
        "open": int(coll.count_documents({"user_id": user_id, "status": {"$in": ["awaiting_payment", "verification_in_progress", "paid_unconfirmed"]}})),
        "completed": int(coll.count_documents({"user_id": user_id, "status": "completed"})),
        "expired": int(coll.count_documents({"user_id": user_id, "status": "expired"})),
        "cancelled": int(coll.count_documents({"user_id": user_id, "status": "cancelled"})),
        "total": int(coll.count_documents({"user_id": user_id})),
    }


def _load_recent_subscription_events(user_id: int, *, limit: int = 6) -> List[Dict[str, Any]]:
    return list(
        subscription_events_collection()
        .find({"user_id": int(user_id)})
        .sort("created_at", -1)
        .limit(max(1, int(limit)))
    )


def _load_recent_referral_rewards(user_id: int, *, limit: int = 5) -> List[Dict[str, Any]]:
    from app.database import referrals_collection
    return list(
        referrals_collection()
        .find({"referrer_id": int(user_id)})
        .sort("created_at", -1)
        .limit(max(1, int(limit)))
    )


def _serialize_signal(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "signal_id": str(doc.get("signal_id") or doc.get("_id") or ""),
        "symbol": doc.get("symbol"),
        "direction": str(doc.get("direction") or "").upper(),
        "visibility": doc.get("visibility"),
        "score": doc.get("normalized_score", doc.get("score")),
        "setup_group": doc.get("setup_group"),
        "entry_price": doc.get("entry_price"),
        "status": doc.get("status") or doc.get("result") or "active",
        "resolution": doc.get("resolution"),
        "created_at": _iso(doc.get("created_at") or doc.get("signal_created_at")),
        "telegram_valid_until": _iso(doc.get("telegram_valid_until")),
        "result": doc.get("result"),
    }


def _history_expiry_type(doc: Dict[str, Any]) -> Optional[str]:
    resolution = str(doc.get("resolution") or "").lower().strip()
    expiry_type = str(doc.get("expiry_type") or "").lower().strip()
    entry_touched = doc.get("entry_touched")

    if resolution == "expired_no_fill":
        return "no_fill"
    if resolution == "expired_after_entry":
        return "after_entry_no_followthrough"
    if expiry_type:
        return expiry_type
    if str(doc.get("result") or "").lower().strip() == "expired":
        if entry_touched is False:
            return "no_fill"
        if entry_touched is True:
            return "after_entry_no_followthrough"
    return None



def _history_expiry_label(doc: Dict[str, Any]) -> Optional[str]:
    expiry_type = _history_expiry_type(doc)
    if expiry_type == "no_fill":
        return "Expirada: no llegó al entry"
    if expiry_type == "after_entry_no_followthrough":
        return "Expirada: tocó entry pero no desarrolló"
    return None



def _serialize_history(doc: Dict[str, Any]) -> Dict[str, Any]:
    expiry_type = _history_expiry_type(doc)
    return {
        "signal_id": str(doc.get("signal_id") or doc.get("_id") or ""),
        "symbol": doc.get("symbol"),
        "direction": str(doc.get("direction") or "").upper(),
        "visibility": doc.get("visibility"),
        "score": doc.get("normalized_score", doc.get("score")),
        "setup_group": doc.get("setup_group"),
        "result": doc.get("result") or "unknown",
        "resolution": doc.get("resolution"),
        "entry_price": doc.get("entry_price"),
        "r_multiple": doc.get("r_multiple"),
        "resolution_minutes": doc.get("resolution_minutes"),
        "entry_touched": doc.get("entry_touched"),
        "entry_touched_at": _iso(doc.get("entry_touched_at")),
        "expiry_type": expiry_type,
        "expiry_label": _history_expiry_label(doc),
        "tp1_progress_max_pct": doc.get("tp1_progress_max_pct"),
        "max_favorable_excursion_r": doc.get("max_favorable_excursion_r"),
        "max_adverse_excursion_r": doc.get("max_adverse_excursion_r"),
        "signal_created_at": _iso(doc.get("signal_created_at") or doc.get("created_at")),
        "evaluated_at": _iso(doc.get("evaluated_at")),
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return default
    if not isfinite(parsed):
        return default
    return parsed


def _watchlist_range_bias(position_pct: Optional[float]) -> str:
    if position_pct is None:
        return "Sin rango"
    if position_pct >= 80.0:
        return "Cerca del máximo 24h"
    if position_pct <= 20.0:
        return "Cerca del mínimo 24h"
    return "Zona media 24h"


def _watchlist_volatility_label(range_pct: float) -> str:
    if range_pct >= 12.0:
        return "Expansivo"
    if range_pct >= 6.0:
        return "Activo"
    if range_pct >= 3.0:
        return "Moderado"
    return "Calmo"


def _watchlist_priority_label(score: float) -> str:
    if score >= 85.0:
        return "Máxima"
    if score >= 70.0:
        return "Alta"
    if score >= 55.0:
        return "Media"
    if score >= 40.0:
        return "Vigilancia"
    return "Baja"


def _watchlist_proximity_label(score: float, *, has_active_signal: bool = False) -> str:
    if has_active_signal:
        return "Setup activo"
    if score >= 85.0:
        return "Muy alta"
    if score >= 70.0:
        return "Alta"
    if score >= 55.0:
        return "Media"
    if score >= 40.0:
        return "Temprana"
    return "Baja"


def _watchlist_activity_score(quote_volume: float, trade_count: int) -> float:
    volume_score = 0.0
    if quote_volume > 0:
        volume_score = max(0.0, min(100.0, (log10(quote_volume + 1.0) - 5.0) * 24.0))
    trade_score = 0.0
    if trade_count > 0:
        trade_score = max(0.0, min(100.0, (log10(float(trade_count) + 1.0) - 2.0) * 34.0))
    return (0.7 * volume_score) + (0.3 * trade_score)


def _watchlist_extreme_score(position_pct: Optional[float]) -> float:
    if position_pct is None:
        return 0.0
    return max(0.0, min(100.0, abs(float(position_pct) - 50.0) * 2.0))


def _watchlist_signal_score(doc: Optional[Dict[str, Any]]) -> float:
    if not doc:
        return 0.0
    return max(0.0, min(100.0, _safe_float(doc.get("normalized_score", doc.get("score")), 0.0)))


def _serialize_watchlist_signal(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not doc:
        return None
    visibility = normalize_plan(doc.get("visibility"))
    return {
        "signal_id": str(doc.get("signal_id") or doc.get("_id") or ""),
        "direction": str(doc.get("direction") or "").upper(),
        "visibility": visibility,
        "visibility_name": get_plan_name(visibility),
        "score": round(_watchlist_signal_score(doc), 1),
        "setup_group": doc.get("setup_group"),
        "status": doc.get("status") or doc.get("result") or "active",
        "result": doc.get("result"),
        "resolution": doc.get("resolution"),
        "created_at": _iso(doc.get("created_at") or doc.get("signal_created_at")),
    }


def _is_active_signal_doc(doc: Dict[str, Any], *, now_utc: Optional[datetime] = None) -> bool:
    now_value = now_utc or datetime.utcnow()
    result = str(doc.get("result") or "").lower().strip()
    resolution = str(doc.get("resolution") or "").lower().strip()
    if result in {"won", "lost", "expired"}:
        return False
    if resolution in {"tp1", "tp2", "sl", "expired_clean"}:
        return False

    status = str(doc.get("status") or "").lower().strip()
    if status in {"active", "pending", "open"}:
        return True

    valid_until = doc.get("telegram_valid_until")
    if isinstance(valid_until, datetime):
        if valid_until.tzinfo is not None:
            valid_until = valid_until.astimezone(timezone.utc).replace(tzinfo=None)
        return valid_until >= now_value
    return False


def _load_watchlist_signal_context(user_id: int, symbols: Iterable[str]) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    ordered_symbols = [str(symbol).upper() for symbol in symbols if symbol]
    if not user_id or not ordered_symbols:
        return {}, {}

    lookup = set(ordered_symbols)
    limit = max(20, len(ordered_symbols) * 6)
    docs = _safe_call(
        lambda: list(
            user_signals_collection()
            .find({"user_id": user_id, "symbol": {"$in": list(lookup)}})
            .sort("created_at", -1)
            .limit(limit)
        ),
        [],
    )

    latest_by_symbol: Dict[str, Dict[str, Any]] = {}
    active_by_symbol: Dict[str, Dict[str, Any]] = {}
    now_value = datetime.utcnow()
    for doc in docs:
        symbol = str(doc.get("symbol") or "").upper()
        if not symbol or symbol not in lookup:
            continue
        if symbol not in latest_by_symbol:
            latest_by_symbol[symbol] = doc
        if symbol not in active_by_symbol and _is_active_signal_doc(doc, now_utc=now_value):
            active_by_symbol[symbol] = doc
    return latest_by_symbol, active_by_symbol


def _watchlist_action_label(
    direction: Optional[str],
    position_pct: Optional[float],
    proximity_score: float,
    *,
    has_active_signal: bool = False,
) -> str:
    if has_active_signal:
        return "Ya tienes una señal activa en seguimiento"

    direction_value = str(direction or "").upper().strip()
    if direction_value not in {"LONG", "SHORT"}:
        if proximity_score >= 55.0:
            return "Vigilar confirmación operativa"
        return "Sin gatillo claro todavía"

    if direction_value == "LONG":
        if position_pct is not None and position_pct >= 70.0:
            return "Vigilar continuación long"
        if position_pct is not None and position_pct <= 35.0:
            return "Vigilar pullback long"
        if proximity_score >= 55.0:
            return "Vigilar confirmación long"
        return "Long en observación"

    if position_pct is not None and position_pct <= 30.0:
        return "Vigilar continuación short"
    if position_pct is not None and position_pct >= 65.0:
        return "Vigilar pullback short"
    if proximity_score >= 55.0:
        return "Vigilar confirmación short"
    return "Short en observación"


def _watchlist_priority_reasons(
    *,
    radar_score: float,
    range_score: float,
    change_score: float,
    activity_score: float,
    extreme_score: float,
    signal_score: float,
    has_active_signal: bool,
    missing_market_data: bool,
) -> List[str]:
    reasons: List[str] = []
    if has_active_signal:
        reasons.append("Señal activa ya visible en tu flujo")
    if radar_score >= 70.0:
        reasons.append("Radar caliente y bien rankeado")
    if signal_score >= 70.0:
        reasons.append("Señal reciente con score alto")
    if range_score >= 55.0:
        reasons.append("Rango intradía expandido")
    if change_score >= 45.0:
        reasons.append("Movimiento 24h con desplazamiento útil")
    if activity_score >= 55.0:
        reasons.append("Volumen y actividad sostienen la lectura")
    if extreme_score >= 60.0:
        reasons.append("Cotiza cerca de un extremo del rango")
    if missing_market_data:
        reasons.append("Sin datos frescos de Binance ahora mismo")
    if not reasons:
        reasons.append("En observación, sin gatillo operativo claro")
    return reasons[:3]


def _radar_priority_label(score: float) -> str:
    if score >= 85.0:
        return "Máxima"
    if score >= 72.0:
        return "Alta"
    if score >= 58.0:
        return "Media"
    if score >= 45.0:
        return "Vigilancia"
    return "Exploración"


def _radar_proximity_label(score: float, *, has_active_signal: bool = False) -> str:
    if has_active_signal:
        return "Activa"
    if score >= 88.0:
        return "Inmediata"
    if score >= 74.0:
        return "Cercana"
    if score >= 58.0:
        return "Preparando"
    return "Temprana"


def _radar_window_label(score: float, range_pct_24h: float, *, has_active_signal: bool = False) -> str:
    if has_active_signal:
        return "Seguimiento activo"
    if score >= 88.0 or (score >= 78.0 and range_pct_24h >= 6.0):
        return "Ventana inmediata"
    if score >= 70.0:
        return "Intradía cercano"
    if score >= 55.0:
        return "Preparando setup"
    return "Exploración"


def _radar_conviction_label(score: float, activity_score: float, range_score: float, *, has_active_signal: bool = False) -> str:
    if has_active_signal:
        return "Seguimiento"
    combined = (0.58 * score) + (0.24 * activity_score) + (0.18 * range_score)
    if combined >= 78.0:
        return "Alta"
    if combined >= 60.0:
        return "Media"
    return "Baja"


def _radar_priority_rank(label: str) -> int:
    mapping = {
        "Máxima": 5,
        "Alta": 4,
        "Media": 3,
        "Vigilancia": 2,
        "Exploración": 1,
    }
    return mapping.get(str(label or ""), 0)


def _radar_proximity_rank(label: str) -> int:
    mapping = {
        "Activa": 5,
        "Inmediata": 4,
        "Cercana": 3,
        "Preparando": 2,
        "Temprana": 1,
    }
    return mapping.get(str(label or ""), 0)


def _radar_signal_context_label(*, has_active_signal: bool, latest_signal: Optional[Dict[str, Any]]) -> str:
    if has_active_signal:
        return "Activa"
    if latest_signal:
        return "Reciente"
    return "Sin señal"


def _preferred_side_matches(direction: Optional[str], preferred_side: Optional[str]) -> bool:
    direction_value = str(direction or "").upper().strip()
    preferred_value = str(preferred_side or "").upper().strip()
    if direction_value == "LONG":
        return "LONG" in preferred_value
    if direction_value == "SHORT":
        return "SHORT" in preferred_value
    return False


def _radar_alignment_label(
    direction: Optional[str],
    market_bias: Optional[str],
    preferred_side: Optional[str],
) -> str:
    bias_value = str(market_bias or "").lower().strip()
    if _preferred_side_matches(direction, preferred_side):
        if (direction == "LONG" and bias_value.startswith("alcista")) or (direction == "SHORT" and bias_value.startswith("bajista")):
            return "A favor"
        return "Con flujo"
    if "neutral" in bias_value or "leve" in bias_value or not bias_value:
        return "Selectivo"
    return "Contratendencia"


def _radar_alignment_rank(label: str) -> int:
    mapping = {
        "A favor": 4,
        "Con flujo": 3,
        "Selectivo": 2,
        "Contratendencia": 1,
    }
    return mapping.get(str(label or ""), 0)


def _radar_execution_state_label(
    *,
    has_active_signal: bool,
    proximity_score: float,
    priority_score: float,
    radar_score: float,
    signal_score: float,
) -> str:
    if has_active_signal:
        return "Seguimiento"
    if proximity_score >= 86.0 and priority_score >= 74.0:
        return "Ejecutable"
    if proximity_score >= 72.0 or priority_score >= 68.0 or signal_score >= 70.0:
        return "Preparación"
    if radar_score >= 52.0:
        return "Observación"
    return "Exploración"


def _radar_execution_rank(label: str) -> int:
    mapping = {
        "Seguimiento": 5,
        "Ejecutable": 4,
        "Preparación": 3,
        "Observación": 2,
        "Exploración": 1,
    }
    return mapping.get(str(label or ""), 0)


def _radar_setup_mode_label(direction: Optional[str], range_position_pct: Optional[float], extreme_score: float) -> str:
    position_pct = None if range_position_pct is None else _safe_float(range_position_pct)
    if str(direction or "").upper() == "LONG":
        if position_pct is not None and position_pct >= 72.0:
            return "Continuación"
        if position_pct is not None and position_pct <= 35.0:
            return "Pullback"
    if str(direction or "").upper() == "SHORT":
        if position_pct is not None and position_pct <= 28.0:
            return "Continuación"
        if position_pct is not None and position_pct >= 65.0:
            return "Pullback"
    if extreme_score >= 62.0:
        return "Extremo"
    return "En desarrollo"


def _radar_risk_label(
    *,
    has_active_signal: bool,
    alignment_label: str,
    conviction_label: str,
    volatility_label: Optional[str],
    funding_rate_pct: float,
) -> str:
    if has_active_signal:
        return "Gestionar"
    volatility_value = str(volatility_label or "").lower().strip()
    if alignment_label == "Contratendencia":
        return "Reducido"
    if conviction_label == "Baja" or volatility_value == "expansivo":
        return "Selectivo"
    if abs(funding_rate_pct) >= 0.05:
        return "Cauto"
    return "Normal"


def _radar_operator_note(
    *,
    execution_state_label: str,
    alignment_label: str,
    setup_mode_label: str,
    risk_label: str,
    has_active_signal: bool,
) -> str:
    if has_active_signal:
        return "Gestiona la señal activa; evita duplicar exposición en el mismo símbolo."
    if execution_state_label == "Ejecutable" and alignment_label in {"A favor", "Con flujo"}:
        return f"{setup_mode_label} con ventana útil; busca confirmación sin perseguir precio."
    if execution_state_label == "Preparación":
        return f"Mantén {setup_mode_label.lower()} en primera línea; puede activarse en esta sesión."
    if alignment_label == "Contratendencia":
        return f"Lectura de {setup_mode_label.lower()} en contra del contexto; opera solo si el riesgo es {risk_label.lower()}."
    return "Radar en vigilancia: todavía no compensa forzar entrada."


def _radar_trade_plan(
    *,
    action_label: str,
    execution_state_label: str,
    alignment_label: str,
    risk_label: str,
    signal_context_label: str,
    setup_mode_label: str,
    funding_rate_pct: float,
) -> List[str]:
    steps = [
        f"Setup: {setup_mode_label} · {action_label}",
        f"Estado: {execution_state_label} · Contexto: {alignment_label}",
        f"Riesgo: {risk_label} · Señal: {signal_context_label}",
    ]
    if abs(funding_rate_pct) >= 0.05:
        steps.append("Funding exigente: evita perseguir si el precio ya viene extendido.")
    return steps[:4]


def _radar_reasons(
    *,
    radar_score: float,
    range_score: float,
    change_score: float,
    activity_score: float,
    extreme_score: float,
    signal_score: float,
    has_active_signal: bool,
    funding_rate_pct: float,
    open_interest: float,
    missing_market_data: bool,
) -> List[str]:
    reasons: List[str] = []
    if has_active_signal:
        reasons.append("Ya tienes una señal activa en este símbolo")
    if radar_score >= 82.0:
        reasons.append("Radar con prioridad alta en esta rotación")
    if range_score >= 58.0:
        reasons.append("Expansión intradía suficiente para setup")
    if change_score >= 50.0:
        reasons.append("Desplazamiento 24h con dirección útil")
    if activity_score >= 55.0:
        reasons.append("Volumen y actividad respaldan el movimiento")
    if extreme_score >= 60.0:
        reasons.append("Cotiza cerca de una zona extrema del rango")
    if signal_score >= 70.0:
        reasons.append("Tu historial reciente ya marcó edge aquí")
    if abs(funding_rate_pct) >= 0.03:
        reasons.append("Funding exigente: vigila continuidad y squeeze")
    if open_interest > 0 and open_interest >= 1_000_000:
        reasons.append("Open interest elevado para seguir el flujo")
    if missing_market_data:
        reasons.append("Sin datos frescos de Binance ahora mismo")
    if not reasons:
        reasons.append("En observación, esperando mejor confirmación")
    return reasons[:4]


def _ticker_range_metrics(item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not item:
        return {
            "missing_market_data": True,
            "last_price": 0.0,
            "change_pct": 0.0,
            "quote_volume": 0.0,
            "trade_count": 0,
            "high_24h": 0.0,
            "low_24h": 0.0,
            "price_change_abs": 0.0,
            "range_pct_24h": 0.0,
            "range_position_pct": None,
            "range_bias_label": "Sin datos de Binance",
            "volatility_label": "Sin datos",
        }

    last_price = _safe_float(item.get("lastPrice"))
    change_pct = _safe_float(item.get("priceChangePercent"))
    quote_volume = _safe_float(item.get("quoteVolume"))
    high_24h = _safe_float(item.get("highPrice"), last_price)
    low_24h = _safe_float(item.get("lowPrice"), last_price)
    trade_count = int(_safe_float(item.get("count"), 0.0))
    price_change_abs = _safe_float(item.get("priceChange"))

    if high_24h > 0 and low_24h > 0 and high_24h >= low_24h:
        range_width = max(high_24h - low_24h, 0.0)
        range_pct_24h = (range_width / low_24h * 100.0) if low_24h > 0 else 0.0
        if range_width > 0 and last_price > 0:
            range_position_pct = max(0.0, min(100.0, ((last_price - low_24h) / range_width) * 100.0))
        else:
            range_position_pct = None
    else:
        range_pct_24h = 0.0
        range_position_pct = None

    return {
        "missing_market_data": False,
        "last_price": last_price,
        "change_pct": change_pct,
        "quote_volume": quote_volume,
        "trade_count": trade_count,
        "high_24h": high_24h,
        "low_24h": low_24h,
        "price_change_abs": price_change_abs,
        "range_pct_24h": range_pct_24h,
        "range_position_pct": range_position_pct,
        "range_bias_label": _watchlist_range_bias(range_position_pct),
        "volatility_label": _watchlist_volatility_label(range_pct_24h),
    }


def _serialize_radar(
    user_id: int,
    *,
    limit: int = 6,
    market_snapshot: Optional[Dict[str, Any]] = None,
    fetch_limit: Optional[int] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    radar_rows = _safe_call(get_radar_opportunities, [], limit=max(6, int(fetch_limit or limit))) or []
    if not radar_rows:
        return [], {
            "total": 0,
            "longs": 0,
            "shorts": 0,
            "hot": 0,
            "immediate": 0,
            "active_signals": 0,
        }

    symbols = [str(row.get("symbol") or "").upper() for row in radar_rows if row.get("symbol")]
    funding_rate_map = get_funding_rate_pct_map(symbols, premium_index_fn=get_premium_index)
    open_interest_map = get_open_interest_map(symbols, open_interest_fn=get_open_interest)
    market_snapshot = market_snapshot or {}
    market_bias = market_snapshot.get("bias")
    preferred_side = market_snapshot.get("preferred_side")
    market_environment = market_snapshot.get("environment")
    selected = set(symbols)
    tickers = get_futures_24h_tickers()
    ticker_by_symbol = {
        str(item.get("symbol") or "").upper(): item
        for item in tickers
        if str(item.get("symbol") or "").upper() in selected
    }
    latest_signal_by_symbol, active_signal_by_symbol = _load_watchlist_signal_context(user_id, symbols)

    items: List[Dict[str, Any]] = []
    for row in radar_rows[: max(1, int(limit))]:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue

        ticker_metrics = _ticker_range_metrics(ticker_by_symbol.get(symbol))
        latest_signal = _serialize_watchlist_signal(latest_signal_by_symbol.get(symbol))
        active_signal = _serialize_watchlist_signal(active_signal_by_symbol.get(symbol))
        signal_score = max(
            _watchlist_signal_score(latest_signal_by_symbol.get(symbol)),
            _watchlist_signal_score(active_signal_by_symbol.get(symbol)),
        )

        radar_score = max(0.0, min(100.0, _safe_float(row.get("final_score", row.get("score")), 0.0)))
        base_score = max(0.0, min(100.0, _safe_float(row.get("score"), radar_score)))
        direction = str(row.get("direction") or "").upper().strip() or None
        range_score = max(0.0, min(100.0, ticker_metrics["range_pct_24h"] * 8.0))
        change_score = max(0.0, min(100.0, abs(ticker_metrics["change_pct"]) * 10.0))
        activity_score = _watchlist_activity_score(ticker_metrics["quote_volume"], ticker_metrics["trade_count"])
        extreme_score = _watchlist_extreme_score(ticker_metrics["range_position_pct"])

        setup_priority_score = (
            (0.55 * radar_score)
            + (0.14 * activity_score)
            + (0.11 * range_score)
            + (0.08 * change_score)
            + (0.04 * extreme_score)
            + (0.08 * signal_score)
        )
        setup_proximity_score = (
            (0.50 * radar_score)
            + (0.22 * extreme_score)
            + (0.16 * range_score)
            + (0.12 * change_score)
        )
        if signal_score > 0:
            setup_proximity_score = max(setup_proximity_score, 0.62 * signal_score)
        if active_signal:
            setup_priority_score = max(setup_priority_score, min(100.0, signal_score + 10.0))
            setup_proximity_score = 100.0

        setup_priority_score = max(0.0, min(100.0, setup_priority_score))
        setup_proximity_score = max(0.0, min(100.0, setup_proximity_score))

        funding_rate_pct = float(funding_rate_map.get(symbol) or 0.0)
        open_interest = float(open_interest_map.get(symbol) or 0.0)

        setup_action_label = _watchlist_action_label(
            direction,
            ticker_metrics["range_position_pct"],
            setup_proximity_score,
            has_active_signal=bool(active_signal),
        )
        reasons = _radar_reasons(
            radar_score=radar_score,
            range_score=range_score,
            change_score=change_score,
            activity_score=activity_score,
            extreme_score=extreme_score,
            signal_score=signal_score,
            has_active_signal=bool(active_signal),
            funding_rate_pct=funding_rate_pct,
            open_interest=open_interest,
            missing_market_data=bool(ticker_metrics["missing_market_data"]),
        )

        priority_label = _radar_priority_label(setup_priority_score)
        proximity_label = _radar_proximity_label(setup_proximity_score, has_active_signal=bool(active_signal))
        window_label = _radar_window_label(setup_proximity_score, ticker_metrics["range_pct_24h"], has_active_signal=bool(active_signal))
        conviction_label = _radar_conviction_label(radar_score, activity_score, range_score, has_active_signal=bool(active_signal))
        signal_context_label = _radar_signal_context_label(has_active_signal=bool(active_signal), latest_signal=latest_signal)
        alignment_label = _radar_alignment_label(direction, market_bias, preferred_side)
        execution_state_label = _radar_execution_state_label(
            has_active_signal=bool(active_signal),
            proximity_score=setup_proximity_score,
            priority_score=setup_priority_score,
            radar_score=radar_score,
            signal_score=signal_score,
        )
        setup_mode_label = _radar_setup_mode_label(direction, ticker_metrics["range_position_pct"], extreme_score)
        risk_label = _radar_risk_label(
            has_active_signal=bool(active_signal),
            alignment_label=alignment_label,
            conviction_label=conviction_label,
            volatility_label=ticker_metrics["volatility_label"],
            funding_rate_pct=funding_rate_pct,
        )
        operator_note = _radar_operator_note(
            execution_state_label=execution_state_label,
            alignment_label=alignment_label,
            setup_mode_label=setup_mode_label,
            risk_label=risk_label,
            has_active_signal=bool(active_signal),
        )
        trade_plan = _radar_trade_plan(
            action_label=setup_action_label,
            execution_state_label=execution_state_label,
            alignment_label=alignment_label,
            risk_label=risk_label,
            signal_context_label=signal_context_label,
            setup_mode_label=setup_mode_label,
            funding_rate_pct=funding_rate_pct,
        )
        ranking_score = (
            (0.44 * setup_priority_score)
            + (0.28 * setup_proximity_score)
            + (0.12 * radar_score)
            + (0.08 * activity_score)
            + (0.08 * signal_score)
        )
        if active_signal:
            ranking_score = max(ranking_score, 96.0)

        items.append({
            "symbol": symbol,
            "direction": direction,
            "score": round(base_score, 1),
            "final_score": round(radar_score, 1),
            "priority_label": priority_label,
            "priority_rank": _radar_priority_rank(priority_label),
            "priority_score": round(setup_priority_score, 1),
            "proximity_label": proximity_label,
            "proximity_rank": _radar_proximity_rank(proximity_label),
            "proximity_score": round(setup_proximity_score, 1),
            "window_label": window_label,
            "conviction_label": conviction_label,
            "signal_context_label": signal_context_label,
            "alignment_label": alignment_label,
            "alignment_rank": _radar_alignment_rank(alignment_label),
            "execution_state_label": execution_state_label,
            "execution_rank": _radar_execution_rank(execution_state_label),
            "setup_mode_label": setup_mode_label,
            "risk_label": risk_label,
            "operator_note": operator_note,
            "trade_plan": trade_plan,
            "market_bias": market_bias,
            "market_environment": market_environment,
            "signal_score": round(signal_score, 1),
            "activity_score": round(activity_score, 1),
            "range_score": round(range_score, 1),
            "change_score": round(change_score, 1),
            "extreme_score": round(extreme_score, 1),
            "ranking_score": round(max(0.0, min(100.0, ranking_score)), 1),
            "action_label": setup_action_label,
            "reason_short": reasons[0],
            "reasons": reasons,
            "momentum": row.get("momentum"),
            "last_price": ticker_metrics["last_price"] or _safe_float(row.get("last_price")),
            "change_pct": ticker_metrics["change_pct"] if not ticker_metrics["missing_market_data"] else _safe_float(row.get("change_pct")),
            "quote_volume": ticker_metrics["quote_volume"] or _safe_float(row.get("quote_volume")),
            "trade_count": ticker_metrics["trade_count"] or int(_safe_float(row.get("trades"), 0.0)),
            "range_pct_24h": ticker_metrics["range_pct_24h"],
            "range_position_pct": ticker_metrics["range_position_pct"],
            "range_bias_label": ticker_metrics["range_bias_label"],
            "volatility_label": ticker_metrics["volatility_label"],
            "price_change_abs": ticker_metrics["price_change_abs"],
            "high_24h": ticker_metrics["high_24h"],
            "low_24h": ticker_metrics["low_24h"],
            "funding_rate_pct": funding_rate_pct,
            "open_interest": open_interest,
            "active_signal": active_signal,
            "latest_signal": latest_signal,
            "has_active_signal": bool(active_signal),
        })

    items.sort(
        key=lambda item: (
            0 if item.get("has_active_signal") else 1,
            -_safe_float(item.get("ranking_score"), 0.0),
            -_safe_float(item.get("priority_score"), 0.0),
            -_safe_float(item.get("proximity_score"), 0.0),
            -_safe_float(item.get("final_score"), 0.0),
            -_safe_float(item.get("quote_volume"), 0.0),
            str(item.get("symbol") or ""),
        )
    )

    summary = {
        "total": len(items),
        "longs": sum(1 for item in items if item.get("direction") == "LONG"),
        "shorts": sum(1 for item in items if item.get("direction") == "SHORT"),
        "hot": sum(1 for item in items if _safe_float(item.get("priority_score"), 0.0) >= 75.0),
        "immediate": sum(1 for item in items if item.get("proximity_label") in {"Activa", "Inmediata", "Cercana"}),
        "active_signals": sum(1 for item in items if item.get("has_active_signal")),
        "priority_mix": {
            "maxima": sum(1 for item in items if item.get("priority_label") == "Máxima"),
            "alta": sum(1 for item in items if item.get("priority_label") == "Alta"),
            "media": sum(1 for item in items if item.get("priority_label") == "Media"),
            "vigilancia": sum(1 for item in items if item.get("priority_label") == "Vigilancia"),
            "exploracion": sum(1 for item in items if item.get("priority_label") == "Exploración"),
        },
        "proximity_mix": {
            "activa": sum(1 for item in items if item.get("proximity_label") == "Activa"),
            "inmediata": sum(1 for item in items if item.get("proximity_label") == "Inmediata"),
            "cercana": sum(1 for item in items if item.get("proximity_label") == "Cercana"),
            "preparando": sum(1 for item in items if item.get("proximity_label") == "Preparando"),
            "temprana": sum(1 for item in items if item.get("proximity_label") == "Temprana"),
        },
        "signal_mix": {
            "activa": sum(1 for item in items if item.get("signal_context_label") == "Activa"),
            "reciente": sum(1 for item in items if item.get("signal_context_label") == "Reciente"),
            "sin_senal": sum(1 for item in items if item.get("signal_context_label") == "Sin señal"),
        },
        "execution_mix": {
            "seguimiento": sum(1 for item in items if item.get("execution_state_label") == "Seguimiento"),
            "ejecutable": sum(1 for item in items if item.get("execution_state_label") == "Ejecutable"),
            "preparacion": sum(1 for item in items if item.get("execution_state_label") == "Preparación"),
            "observacion": sum(1 for item in items if item.get("execution_state_label") == "Observación"),
            "exploracion": sum(1 for item in items if item.get("execution_state_label") == "Exploración"),
        },
        "alignment_mix": {
            "a_favor": sum(1 for item in items if item.get("alignment_label") == "A favor"),
            "con_flujo": sum(1 for item in items if item.get("alignment_label") == "Con flujo"),
            "selectivo": sum(1 for item in items if item.get("alignment_label") == "Selectivo"),
            "contratendencia": sum(1 for item in items if item.get("alignment_label") == "Contratendencia"),
        },
        "focus_now": sum(1 for item in items if item.get("execution_state_label") in {"Seguimiento", "Ejecutable"}),
        "aligned_now": sum(1 for item in items if item.get("alignment_label") in {"A favor", "Con flujo"}),
        "sort_default": "ranking",
    }
    return items, summary


def _serialize_watchlist(symbols: Iterable[str], *, user_id: int = 0) -> List[Dict[str, Any]]:
    selected_order = [str(symbol).upper() for symbol in symbols if symbol]
    if not selected_order:
        return []

    selected = set(selected_order)
    tickers = get_futures_24h_tickers()
    ticker_by_symbol: Dict[str, Dict[str, Any]] = {}
    for item in tickers:
        symbol = str(item.get("symbol", "")).upper()
        if symbol and symbol in selected:
            ticker_by_symbol[symbol] = item

    radar_fetch_limit = max(60, len(selected_order) * 12, len(tickers) or 0)
    radar_rows = _safe_call(get_radar_opportunities, [], limit=radar_fetch_limit) or []
    radar_by_symbol = {
        str(item.get("symbol") or "").upper(): item
        for item in radar_rows
        if item.get("symbol")
    }
    latest_signal_by_symbol, active_signal_by_symbol = _load_watchlist_signal_context(user_id, selected_order)

    rows: List[Dict[str, Any]] = []
    for symbol in selected_order:
        item = ticker_by_symbol.get(symbol)
        missing_market_data = item is None
        if not item:
            last_price = 0.0
            change_pct = 0.0
            quote_volume = 0.0
            volume_base = 0.0
            high_24h = 0.0
            low_24h = 0.0
            trade_count = 0
            price_change_abs = 0.0
            range_pct_24h = 0.0
            range_position_pct = None
            range_bias_label = "Sin datos de Binance"
            volatility_label = "Sin datos"
        else:
            last_price = _safe_float(item.get("lastPrice"))
            change_pct = _safe_float(item.get("priceChangePercent"))
            quote_volume = _safe_float(item.get("quoteVolume"))
            volume_base = _safe_float(item.get("volume"))
            high_24h = _safe_float(item.get("highPrice"), last_price)
            low_24h = _safe_float(item.get("lowPrice"), last_price)
            trade_count = int(_safe_float(item.get("count"), 0.0))
            price_change_abs = _safe_float(item.get("priceChange"))

            if high_24h > 0 and low_24h > 0 and high_24h >= low_24h:
                range_width = max(high_24h - low_24h, 0.0)
                range_pct_24h = (range_width / low_24h * 100.0) if low_24h > 0 else 0.0
                if range_width > 0 and last_price > 0:
                    range_position_pct = max(0.0, min(100.0, ((last_price - low_24h) / range_width) * 100.0))
                else:
                    range_position_pct = None
            else:
                range_pct_24h = 0.0
                range_position_pct = None
            range_bias_label = _watchlist_range_bias(range_position_pct)
            volatility_label = _watchlist_volatility_label(range_pct_24h)

        radar_entry = radar_by_symbol.get(symbol) or {}
        radar_score = max(0.0, min(100.0, _safe_float(radar_entry.get("final_score", radar_entry.get("score")), 0.0)))
        radar_direction = str(radar_entry.get("direction") or "").upper().strip() or None
        radar_momentum = radar_entry.get("momentum")

        latest_signal = latest_signal_by_symbol.get(symbol)
        active_signal = active_signal_by_symbol.get(symbol)
        latest_signal_public = _serialize_watchlist_signal(latest_signal)
        active_signal_public = _serialize_watchlist_signal(active_signal)
        signal_score = max(_watchlist_signal_score(latest_signal), _watchlist_signal_score(active_signal))

        range_score = max(0.0, min(100.0, range_pct_24h * 8.0))
        change_score = max(0.0, min(100.0, abs(change_pct) * 10.0))
        activity_score = _watchlist_activity_score(quote_volume, trade_count)
        extreme_score = _watchlist_extreme_score(range_position_pct)

        priority_score = (
            (0.38 * radar_score)
            + (0.16 * range_score)
            + (0.14 * change_score)
            + (0.14 * activity_score)
            + (0.08 * extreme_score)
            + (0.10 * signal_score)
        )
        proximity_score = (
            (0.45 * radar_score)
            + (0.25 * extreme_score)
            + (0.15 * range_score)
            + (0.15 * change_score)
        )
        if signal_score > 0:
            proximity_score = max(proximity_score, (0.6 * signal_score) + (20.0 if active_signal_public else 0.0))
        if active_signal_public:
            priority_score = max(priority_score, min(100.0, signal_score + 12.0))
            proximity_score = 100.0

        priority_score = max(0.0, min(100.0, priority_score))
        proximity_score = max(0.0, min(100.0, proximity_score))
        direction_hint = radar_direction or (active_signal_public or latest_signal_public or {}).get("direction")
        reasons = _watchlist_priority_reasons(
            radar_score=radar_score,
            range_score=range_score,
            change_score=change_score,
            activity_score=activity_score,
            extreme_score=extreme_score,
            signal_score=signal_score,
            has_active_signal=bool(active_signal_public),
            missing_market_data=missing_market_data,
        )

        rows.append({
            "symbol": symbol,
            "last_price": last_price,
            "change_pct": change_pct,
            "quote_volume": quote_volume,
            "volume_base": volume_base,
            "trade_count": trade_count,
            "high_24h": high_24h,
            "low_24h": low_24h,
            "range_pct_24h": range_pct_24h,
            "range_position_pct": range_position_pct,
            "range_bias_label": range_bias_label,
            "volatility_label": volatility_label,
            "price_change_abs": price_change_abs,
            "is_positive": change_pct >= 0,
            "radar_score": round(radar_score, 1),
            "radar_direction": radar_direction,
            "radar_momentum": radar_momentum,
            "setup_priority_score": round(priority_score, 1),
            "setup_priority_label": _watchlist_priority_label(priority_score),
            "setup_proximity_score": round(proximity_score, 1),
            "setup_proximity_label": _watchlist_proximity_label(proximity_score, has_active_signal=bool(active_signal_public)),
            "setup_action_label": _watchlist_action_label(direction_hint, range_position_pct, proximity_score, has_active_signal=bool(active_signal_public)),
            "priority_reasons": reasons,
            "priority_reason_short": reasons[0],
            "priority_driver_label": reasons[0],
            "active_signal": active_signal_public,
            "latest_signal": latest_signal_public,
            "has_active_signal": bool(active_signal_public),
        })

    return rows


def _safe_call(fn, default, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        return default


def _safe_serialize_items(items: List[Dict[str, Any]], serializer) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    for item in items or []:
        try:
            serialized.append(serializer(item))
        except Exception:
            continue
    return serialized


def _safe_serialize_optional(item: Optional[Dict[str, Any]], serializer) -> Optional[Dict[str, Any]]:
    if not item:
        return None
    try:
        return serializer(item)
    except Exception:
        return None


def _empty_summary() -> Dict[str, Any]:
    return {
        "total": 0,
        "resolved": 0,
        "filled_total": 0,
        "won": 0,
        "lost": 0,
        "expired": 0,
        "expired_no_fill": 0,
        "expired_after_entry": 0,
        "tp1": 0,
        "tp2": 0,
        "sl": 0,
        "winrate": 0.0,
        "loss_rate": 0.0,
        "expiry_rate": 0.0,
        "fill_rate": 0.0,
        "no_fill_rate": 0.0,
        "post_fill_expiry_rate": 0.0,
        "after_entry_failure_rate": 0.0,
        "profit_factor": 0.0,
        "expectancy_r": 0.0,
        "max_drawdown_r": 0.0,
    }


def _resolved_count(summary: Any) -> int:
    if not isinstance(summary, dict):
        return 0
    try:
        return int(summary.get("resolved") or 0)
    except Exception:
        return 0


def _select_dashboard_summary(snapshot: Dict[str, Any]) -> tuple[Dict[str, Any], str]:
    summary_7d = snapshot.get("summary_7d") if isinstance(snapshot.get("summary_7d"), dict) else None
    summary_30d = snapshot.get("summary_30d") if isinstance(snapshot.get("summary_30d"), dict) else None

    if _resolved_count(summary_7d) > 0:
        return summary_7d or _empty_summary(), "7D"
    if _resolved_count(summary_30d) > 0:
        return summary_30d or _empty_summary(), "30D"

    total_materialized = _safe_call(lambda: get_materialized_window(3650), None)
    total_payload = total_materialized or (_safe_call(lambda: build_performance_window(3650), None) or {})
    total_summary = total_payload.get("summary") if isinstance(total_payload, dict) else None
    if _resolved_count(total_summary) > 0:
        return total_summary or _empty_summary(), "Total"

    return (summary_7d or summary_30d or _empty_summary()), "7D"



def _tracking_feature_tier(plan: str) -> str:
    plan_value = normalize_plan(plan)
    if plan_value == "premium":
        return "advanced"
    if plan_value == "plus":
        return "full"
    return "basic"


def _human_component_label(raw_label: Any) -> str:
    normalized = str(raw_label or "").strip().lower()
    mapping = {
        "trend_structure": "Estructura de tendencia",
        "adx_strength": "Fuerza ADX",
        "atr_quality": "Calidad ATR",
        "breakout_quality": "Calidad breakout",
        "retest_quality": "Calidad retest",
        "continuation_quality": "Continuación",
        "volume_quality": "Calidad de volumen",
        "entry_freshness": "Frescura de entrada",
        "profile_penalty": "Ajuste por perfil",
        "liquidity_zone": "Liquidity Zone",
        "minimum_sweep": "Minimum Sweep",
        "recovery_close": "Recovery Close",
        "relative_volume": "Relative Volume",
        "confirmation_candle": "Confirmation Candle",
        "ema_reclaim_filter": "Ema Reclaim Filter",
        "htf_context": "HTF Context",
        "barrier_room": "Barrier Room",
        "rr_filter": "RR Filter",
    }
    return mapping.get(normalized, str(raw_label or "—").replace("_", " ").title())


def _coerce_component_score(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return None
    return round(parsed, 2)


def _serialize_score_components(items: Any, *, limit: Optional[int] = None) -> list[Dict[str, Any]]:
    if not isinstance(items, list):
        return []
    rows: list[Dict[str, Any]] = []
    for raw_item in items:
        label = None
        points = None
        if isinstance(raw_item, (list, tuple)) and raw_item:
            label = raw_item[0]
            if len(raw_item) > 1:
                points = raw_item[1]
        elif isinstance(raw_item, dict):
            label = raw_item.get("label") or raw_item.get("name")
            points = raw_item.get("points")
            if points is None:
                for candidate_key in ("score", "value", "raw", "normalized"):
                    if raw_item.get(candidate_key) is not None:
                        points = raw_item.get(candidate_key)
                        break
        else:
            label = raw_item
        score_value = _coerce_component_score(points)
        has_numeric_score = score_value is not None
        row = {
            "label": _human_component_label(label),
            "score": score_value,
            "tone": "positive" if (score_value or 0) >= 0 else "negative",
            "status": "passed" if not has_numeric_score else ("positive" if (score_value or 0) >= 0 else "negative"),
            "status_label": "OK" if not has_numeric_score else None,
            "has_numeric_score": has_numeric_score,
        }
        rows.append(row)
    if limit is not None:
        return rows[:max(0, int(limit))]
    return rows


def _component_extreme(rows: list[Dict[str, Any]], *, pick: str) -> Optional[Dict[str, Any]]:
    eligible = [row for row in rows if row.get("score") is not None]
    if not eligible:
        return None
    key_fn = lambda row: float(row.get("score") or 0.0)
    return max(eligible, key=key_fn) if pick == "max" else min(eligible, key=key_fn)


def _build_radar_scanner_snapshot(symbol: str, *, expected_direction: Optional[str] = None) -> Dict[str, Any]:
    symbol_value = str(symbol or '').upper().strip()
    if not symbol_value:
        return {
            "status": "unavailable",
            "label": "Scanner no disponible",
            "summary": "No se recibió un símbolo válido para el scanner.",
            "components": [],
            "profiles": [],
            "direction_alignment": None,
        }

    cached = _cache_get_radar_scan(symbol_value)
    if cached is not None:
        if expected_direction:
            cached["direction_alignment"] = cached.get("direction") == str(expected_direction).upper().strip()
        return cached

    try:
        from app.scanner import get_klines
        from app.strategy import mtf_strategy

        df_1h = get_klines(symbol_value, "1h")
        df_15m = get_klines(symbol_value, "15m")
        df_5m = get_klines(symbol_value, "5m")
        result = mtf_strategy(df_1h=df_1h, df_15m=df_15m, df_5m=df_5m)
    except Exception:
        payload = {
            "status": "unavailable",
            "label": "Scanner no disponible",
            "summary": "No pude evaluar ahora mismo la lógica táctica del scanner para este activo.",
            "components": [],
            "profiles": [],
            "direction_alignment": None,
        }
        _cache_set_radar_scan(symbol_value, payload)
        return payload

    if not result:
        payload = {
            "status": "no_setup",
            "label": "Sin setup confirmado",
            "summary": "El scanner no confirmó un setup limpio en este momento. Mantén el radar como vigilancia, no como ejecución inmediata.",
            "components": [],
            "profiles": [],
            "direction_alignment": None,
        }
        _cache_set_radar_scan(symbol_value, payload)
        return payload

    direction = str(result.get("direction") or "").upper().strip() or None
    components = _serialize_score_components(result.get("components"), limit=5)
    strongest = _component_extreme(components, pick="max")
    weakest = _component_extreme(components, pick="min")
    profiles = []
    for profile_key in ("conservador", "moderado", "agresivo"):
        profile_payload = (result.get("profiles") or {}).get(profile_key) or {}
        take_profits = list(profile_payload.get("take_profits") or [])
        profiles.append({
            "profile": profile_key,
            "label": profile_key.title(),
            "entry_price": result.get("entry_price"),
            "stop_loss": profile_payload.get("stop_loss") or result.get("stop_loss"),
            "tp1": take_profits[0] if len(take_profits) > 0 else None,
            "tp2": take_profits[1] if len(take_profits) > 1 else None,
            "leverage": profile_payload.get("leverage"),
        })

    setup_group = result.get("setup_group")
    score = result.get("normalized_score") or result.get("score")
    summary = f"Scanner confirma {direction or '—'} {str(setup_group or 'setup').upper()} con score {round(float(score or 0.0), 1):.1f}."
    payload = {
        "status": "confirmed",
        "label": "Setup confirmado",
        "summary": summary,
        "direction": direction,
        "direction_alignment": (direction == str(expected_direction).upper().strip()) if expected_direction and direction else None,
        "setup_group": setup_group,
        "score": score,
        "raw_score": result.get("raw_score"),
        "atr_pct": result.get("atr_pct"),
        "timeframes": list(result.get("timeframes") or []),
        "score_profile": result.get("score_profile"),
        "score_calibration": result.get("score_calibration"),
        "components": components,
        "strongest_component": strongest,
        "weakest_component": weakest,
        "profiles": profiles,
    }
    _cache_set_radar_scan(symbol_value, payload)
    return payload


def build_radar_symbol_payload(user: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
    symbol_value = str(symbol or '').upper().strip()
    if not symbol_value:
        return None

    user_id = int(user.get("user_id") or 0)
    cache_key = f"radar-detail:{user_id}:{symbol_value}"
    cached = _cache_get_payload(_RADAR_DETAIL_CACHE, cache_key)
    if cached is not None:
        return cached

    status = plan_status(user)
    effective_plan = normalize_plan(status.get("plan") or user.get("plan"))
    market_payload = build_market_payload(user)
    snapshot = {
        "bias": market_payload.get("bias"),
        "preferred_side": market_payload.get("preferred_side"),
        "regime": market_payload.get("regime"),
        "environment": market_payload.get("environment"),
        "recommendation": market_payload.get("recommendation"),
    }
    radar_items = list(market_payload.get("radar") or [])
    radar_summary = market_payload.get("radar_summary") or {"total": 0}
    radar_item = next((item for item in radar_items if str(item.get("symbol") or '').upper() == symbol_value), None)
    if not radar_item:
        fallback_items, fallback_summary = _safe_call(
            lambda: _serialize_radar(user_id, limit=24, fetch_limit=40, market_snapshot=snapshot),
            ([], radar_summary),
        )
        radar_summary = fallback_summary or radar_summary
        radar_item = next((item for item in fallback_items if str(item.get("symbol") or '').upper() == symbol_value), None)
    if not radar_item:
        return None

    scanner = _build_radar_scanner_snapshot(symbol_value, expected_direction=radar_item.get("direction"))
    signal_context = radar_item.get("latest_signal") or radar_item.get("active_signal") or {}
    signal_id = signal_context.get("signal_id")
    signal_detail_available = bool(signal_id)

    tactical_checks: List[str] = [
        f"Estado operativo: {radar_item.get('execution_state_label') or 'Observación'}.",
        f"Alineación: {radar_item.get('alignment_label') or 'Selectivo'}.",
        f"Riesgo táctico: {radar_item.get('risk_label') or 'Normal'}.",
    ]
    if scanner.get("status") == "confirmed":
        tactical_checks.append(scanner.get("summary") or "Scanner con setup confirmado.")
        if scanner.get("direction_alignment") is True:
            tactical_checks.append("Radar y scanner están alineados en dirección.")
        elif scanner.get("direction_alignment") is False:
            tactical_checks.append("Scanner y radar no están alineados: requiere validación extra.")
    elif scanner.get("status") == "no_setup":
        tactical_checks.append("El scanner aún no confirma gatillo; úsalo como vigilancia, no como ejecución ciega.")
    else:
        tactical_checks.append("El scanner no pudo evaluarse ahora mismo; decide con prudencia hasta refrescar.")

    payload = {
        "symbol": symbol_value,
        "viewer_plan": effective_plan,
        "market_context": snapshot,
        "summary": radar_summary,
        "radar": radar_item,
        "scanner": scanner,
        "signal_context": {
            "has_active_signal": bool(radar_item.get("has_active_signal")),
            "label": radar_item.get("signal_context_label"),
            "signal_id": signal_id,
            "signal_detail_available": signal_detail_available,
            "signal": signal_context or None,
        },
        "tactical_checks": tactical_checks,
    }
    _cache_set_payload(_RADAR_DETAIL_CACHE, cache_key, payload, _RADAR_DETAIL_TTL_SECONDS)
    return deepcopy(payload)


def _signal_visibility_rank(plan: Any) -> int:
    value = normalize_plan(plan)
    return {"free": 0, "plus": 1, "premium": 2}.get(value, 0)


def build_signal_detail_payload(user: Dict[str, Any], signal_id: str, *, profile_name: str = "moderado") -> Optional[Dict[str, Any]]:
    user_id = int(user.get("user_id") or 0)
    status = plan_status(user)
    effective_plan = normalize_plan(status.get("plan") or user.get("plan"))
    tier = _tracking_feature_tier(effective_plan)

    selected_profile = str(profile_name or "moderado").strip().lower()
    if selected_profile not in {"conservador", "moderado", "agresivo"}:
        selected_profile = "moderado"
    if tier == "basic":
        selected_profile = "moderado"

    tracking = get_signal_tracking_for_user(user_id, signal_id, profile_name=selected_profile)
    if not tracking:
        return None

    analysis = get_signal_analysis_for_user(user_id, signal_id, profile_name=selected_profile) or {}
    signal_row = _serialize_signal(tracking)
    visibility = normalize_plan(tracking.get("visibility") or signal_row.get("visibility"))
    score_components = _serialize_score_components(analysis.get("components"), limit=6 if tier == "advanced" else 4)
    raw_components = _serialize_score_components(analysis.get("raw_components"), limit=6) if tier == "advanced" else []
    normalized_components = _serialize_score_components(analysis.get("normalized_components"), limit=6) if tier == "advanced" else []
    strongest_component = _component_extreme(score_components, pick="max")
    weakest_component = _component_extreme(score_components, pick="min")
    take_profits = tracking.get("take_profits") or []

    tracking_payload: Dict[str, Any] = {
        "selected_profile": selected_profile,
        "state_label": tracking.get("state_label"),
        "entry_state_label": tracking.get("entry_state_label"),
        "result_label": tracking.get("result_label"),
        "recommendation": tracking.get("recommendation"),
        "current_price": tracking.get("current_price"),
        "entry_price": tracking.get("entry_price"),
        "entry_zone_low": tracking.get("entry_zone_low"),
        "entry_zone_high": tracking.get("entry_zone_high"),
        "stop_loss": tracking.get("stop_loss"),
        "take_profits": take_profits,
        "current_move_pct": tracking.get("current_move_pct"),
        "distance_to_entry_pct": tracking.get("distance_to_entry_pct"),
        "stop_distance_pct": tracking.get("stop_distance_pct"),
        "tp1_distance_pct": tracking.get("tp1_distance_pct"),
        "tp2_distance_pct": analysis.get("selected_tp2_distance_pct"),
        "progress_to_tp1_pct": tracking.get("progress_to_tp1_pct"),
        "in_entry_zone": bool(tracking.get("in_entry_zone")),
        "tp1_hit_now": bool(tracking.get("tp1_hit_now")),
        "tp2_hit_now": bool(tracking.get("tp2_hit_now")),
        "stop_hit_now": bool(tracking.get("stop_hit_now")),
        "is_operable_now": bool(tracking.get("is_operable_now")),
        "created_at": _iso(tracking.get("created_at")),
        "telegram_valid_until": _iso(tracking.get("telegram_valid_until")),
        "evaluation_valid_until": _iso(tracking.get("evaluation_valid_until") or tracking.get("valid_until")),
        "warnings": list((tracking.get("warnings") or [])[:(4 if tier == "advanced" else 2)]),
    }

    analysis_payload: Dict[str, Any] = {
        "setup_group": analysis.get("setup_group"),
        "score": analysis.get("score"),
        "normalized_score": analysis.get("normalized_score"),
        "atr_pct": analysis.get("atr_pct"),
        "timeframes": list(analysis.get("timeframes") or []),
        "strongest_component": strongest_component,
        "weakest_component": weakest_component,
        "components": score_components,
        "selected_stop_distance_pct": analysis.get("selected_stop_distance_pct"),
        "selected_tp1_distance_pct": analysis.get("selected_tp1_distance_pct"),
        "selected_tp2_distance_pct": analysis.get("selected_tp2_distance_pct"),
        "warnings": list((analysis.get("warnings") or [])[:(4 if tier == "advanced" else 2)]),
    }

    if tier in {"full", "advanced"}:
        analysis_payload.update({
            "market_validity_minutes": analysis.get("market_validity_minutes"),
            "leverage": (analysis.get("selected_profile_payload") or {}).get("leverage"),
        })

    if tier == "advanced":
        analysis_payload.update({
            "score_profile": analysis.get("score_profile"),
            "score_calibration": analysis.get("score_calibration"),
            "raw_components": raw_components,
            "normalized_components": normalized_components,
        })

    if tier == "basic":
        upgrade_hint = "Plus desbloquea estructura operativa completa y Premium añade desglose interno del scoring."
    elif tier == "full":
        upgrade_hint = "Premium añade desglose interno del scoring y componentes avanzados."
    else:
        upgrade_hint = None

    return {
        "signal": {
            **signal_row,
            "visibility_rank": _signal_visibility_rank(visibility),
            "created_at": signal_row.get("created_at") or _iso(tracking.get("created_at")),
            "visibility": visibility,
        },
        "viewer_plan": effective_plan,
        "tracking_tier": tier,
        "selected_profile": selected_profile,
        "profile_options": ["moderado"] if tier == "basic" else ["conservador", "moderado", "agresivo"],
        "tracking": tracking_payload,
        "analysis": analysis_payload,
        "upgrade_hint": upgrade_hint,
    }



RISK_EXCHANGE_LABELS: Dict[str, str] = {
    "binance": "Binance",
    "lbank": "LBank",
    "coinw": "CoinW",
    "weex": "WEEX",
    "coinex": "CoinEx",
    "bitunix": "Bitunix",
    "mexc": "MEXC",
    "other": "Otro",
}


def _risk_feature_tier(plan: str) -> str:
    plan_value = normalize_plan(plan)
    return "basic" if plan_value == "free" else "full"


def _risk_profile_options_for_plan(plan: str) -> List[str]:
    return ["moderado"] if _risk_feature_tier(plan) == "basic" else ["conservador", "moderado", "agresivo"]


def _risk_exchange_options() -> List[Dict[str, Any]]:
    order = ["binance", "lbank", "coinw", "weex", "coinex", "bitunix", "mexc", "other"]
    return [
        {"value": value, "label": RISK_EXCHANGE_LABELS.get(value, str(value).upper())}
        for value in order
    ]


def _risk_entry_mode_options() -> List[Dict[str, Any]]:
    order = ["limit_wait", "limit_fast", "limit_unknown"]
    return [
        {"value": value, "label": ENTRY_MODE_LABELS.get(value, value)}
        for value in order
    ]


def _serialize_risk_profile(profile: Optional[Dict[str, Any]], *, effective_plan: str) -> Dict[str, Any]:
    normalized = normalize_risk_profile(profile)
    profile_options = _risk_profile_options_for_plan(effective_plan)
    if normalized.get("default_profile") not in profile_options:
        normalized["default_profile"] = profile_options[0]
    return {
        **normalized,
        "updated_at": _iso(normalized.get("updated_at")),
        "exchange_label": RISK_EXCHANGE_LABELS.get(normalized.get("exchange"), str(normalized.get("exchange") or "—").upper()),
        "entry_mode_label": ENTRY_MODE_LABELS.get(normalized.get("entry_mode"), ENTRY_MODE_LABELS.get("limit_wait")),
        "default_profile_label": get_risk_profile_label(normalized.get("default_profile") or "moderado"),
    }


def _serialize_risk_candidate(doc: Dict[str, Any], *, source: str) -> Dict[str, Any]:
    visibility = normalize_plan(doc.get("visibility"))
    result_value = str(doc.get("result") or "").lower().strip()
    status_value = str(doc.get("status") or result_value or ("active" if source == "live" else "closed")).lower().strip()
    return {
        "signal_id": str(doc.get("signal_id") or doc.get("_id") or ""),
        "source": source,
        "symbol": str(doc.get("symbol") or "").upper(),
        "direction": str(doc.get("direction") or "").upper(),
        "visibility": visibility,
        "visibility_name": get_plan_name(visibility),
        "score": doc.get("normalized_score", doc.get("score")),
        "setup_group": doc.get("setup_group"),
        "entry_price": doc.get("entry_price"),
        "status": status_value,
        "result": result_value or None,
        "created_at": _iso(doc.get("created_at") or doc.get("signal_created_at")),
        "telegram_valid_until": _iso(doc.get("telegram_valid_until")),
        "evaluation_valid_until": _iso(doc.get("evaluation_valid_until") or doc.get("valid_until")),
        "evaluated_at": _iso(doc.get("evaluated_at")),
        "resolution_minutes": doc.get("resolution_minutes"),
    }


def _serialize_risk_preview(calc: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics = calc.get("diagnostics") or {}
    return {
        "signal_id": calc.get("signal_id"),
        "symbol": calc.get("symbol"),
        "direction": calc.get("direction"),
        "visibility": normalize_plan(calc.get("visibility")),
        "visibility_name": get_plan_name(calc.get("visibility")),
        "profile_name": calc.get("profile_name"),
        "profile_label": calc.get("profile_label"),
        "requested_profile_name": calc.get("requested_profile_name"),
        "requested_profile_label": calc.get("requested_profile_label"),
        "profile_fallback_used": bool(calc.get("profile_fallback_used")),
        "profile_resolution_errors": list(calc.get("profile_resolution_errors") or []),
        "entry_price": calc.get("entry_price"),
        "stop_loss": calc.get("stop_loss"),
        "take_profits": list(calc.get("take_profits") or []),
        "capital_usdt": calc.get("capital_usdt"),
        "risk_percent": calc.get("risk_percent"),
        "risk_amount_usdt": calc.get("risk_amount_usdt"),
        "leverage": calc.get("leverage"),
        "signal_leverage_default": calc.get("signal_leverage_default"),
        "signal_leverage_hint": calc.get("signal_leverage_hint"),
        "fee_percent_per_side": calc.get("fee_percent_per_side"),
        "slippage_percent": calc.get("slippage_percent"),
        "exchange": calc.get("exchange"),
        "exchange_label": RISK_EXCHANGE_LABELS.get(calc.get("exchange"), str(calc.get("exchange") or "—").upper()),
        "entry_mode": calc.get("entry_mode"),
        "entry_mode_label": calc.get("entry_mode_label"),
        "position_notional_usdt": calc.get("position_notional_usdt"),
        "required_margin_usdt": calc.get("required_margin_usdt"),
        "quantity_estimate": calc.get("quantity_estimate"),
        "loss_at_stop_usdt": calc.get("loss_at_stop_usdt"),
        "stop_distance_pct": calc.get("stop_distance_pct"),
        "effective_loss_pct": calc.get("effective_loss_pct"),
        "fee_roundtrip_pct": calc.get("fee_roundtrip_pct"),
        "slippage_decimal": calc.get("slippage_decimal"),
        "tp_results": list(calc.get("tp_results") or []),
        "warnings": list(calc.get("warnings") or []),
        "is_operable": bool(calc.get("is_operable")),
        "signal_active_for_entry": bool(calc.get("signal_active_for_entry")),
        "diagnostics": {
            "margin_usage_pct": diagnostics.get("margin_usage_pct"),
            "capital_buffer_usdt": diagnostics.get("capital_buffer_usdt"),
            "tp_count": diagnostics.get("tp_count"),
            "best_rr_net": diagnostics.get("best_rr_net"),
            "risk_band": diagnostics.get("risk_band"),
        },
        "risk_profile": _serialize_risk_profile(calc.get("risk_profile"), effective_plan=normalize_plan(calc.get("visibility") or "free")),
        "created_at": _iso(calc.get("created_at")),
        "telegram_valid_until": _iso(calc.get("telegram_valid_until")),
        "evaluation_valid_until": _iso(calc.get("evaluation_valid_until")),
        "timeframes": list(calc.get("timeframes") or []),
        "score": calc.get("score"),
    }


def build_risk_center_payload(
    user: Dict[str, Any],
    *,
    signal_id: Optional[str] = None,
    profile_name: Optional[str] = None,
    override_leverage: Optional[float] = None,
) -> Dict[str, Any]:
    user_id = int(user.get("user_id") or 0)
    status = plan_status(user)
    effective_plan = normalize_plan(status.get("plan") or user.get("plan"))
    feature_tier = _risk_feature_tier(effective_plan)
    risk_profile = get_user_risk_profile(user_id)
    profile_options = _risk_profile_options_for_plan(effective_plan)
    risk_profile_public = _serialize_risk_profile(risk_profile, effective_plan=effective_plan)

    readiness = {
        "is_ready": True,
        "message": "Perfil listo para calcular riesgo desde señales activas o históricas.",
        "blocking_reason": None,
    }
    try:
        normalize_risk_profile(risk_profile)
        ensure_risk_profile_ready(risk_profile)
    except RiskConfigurationError as exc:
        readiness = {
            "is_ready": False,
            "message": str(exc),
            "blocking_reason": str(exc),
        }

    live_docs = _safe_call(
        lambda: list(
            user_signals_collection()
            .find({"user_id": user_id})
            .sort("created_at", -1)
            .limit(18)
        ),
        [],
    )
    live_items = []
    for doc in live_docs:
        if doc.get("result"):
            continue
        live_items.append(_serialize_risk_candidate(doc, source="live"))
        if len(live_items) >= 8:
            break

    history_docs = _safe_call(lambda: get_history_entries_for_user(user_id, user_plan=user.get("plan"), limit=8), []) or []
    history_items = [_serialize_risk_candidate(doc, source="history") for doc in history_docs]

    selected_signal = None
    preview = None
    preview_error = None
    selected_signal_id = str(signal_id or "").strip() or None
    requested_profile = str(profile_name or risk_profile_public.get("default_profile") or "moderado").strip().lower() or None
    if requested_profile not in profile_options:
        requested_profile = profile_options[0]

    if selected_signal_id:
        user_signal = get_user_signal_by_signal_id(user_id, selected_signal_id)
        if not user_signal:
            preview_error = "No pude encontrar esa señal para calcular riesgo."
        else:
            source = "live" if not user_signal.get("result") else "history"
            selected_signal = _serialize_risk_candidate(user_signal, source=source)
            try:
                preview = _serialize_risk_preview(
                    build_risk_preview_from_user_signal(
                        user_signal,
                        risk_profile=risk_profile,
                        profile_name=requested_profile,
                        override_leverage=override_leverage if override_leverage and float(override_leverage) > 0 else None,
                    )
                )
            except (RiskConfigurationError, SignalProfileError, SignalRiskError) as exc:
                preview_error = str(exc)

    preset_matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for exchange in [item["value"] for item in _risk_exchange_options()]:
        preset_matrix[exchange] = {}
        for entry_mode in [item["value"] for item in _risk_entry_mode_options()]:
            preset_matrix[exchange][entry_mode] = get_exchange_fee_preset(exchange, entry_mode)

    return {
        "overview": {
            "plan": effective_plan,
            "plan_name": get_plan_name(effective_plan),
            "feature_tier": feature_tier,
            "profile_options": profile_options,
        },
        "profile": risk_profile_public,
        "readiness": readiness,
        "catalog": {
            "exchanges": _risk_exchange_options(),
            "entry_modes": _risk_entry_mode_options(),
            "presets": preset_matrix,
        },
        "signals": {
            "live": live_items,
            "history": history_items,
            "selected_signal_id": selected_signal_id,
            "selected_profile": requested_profile,
            "selected_signal": selected_signal,
        },
        "preview": preview,
        "preview_error": preview_error,
        "generated_at": utcnow().isoformat(),
    }


def ensure_mini_app_user(*, user_id: int, username: Optional[str], telegram_language: Optional[str]) -> Dict[str, Any]:
    user, _ = get_or_create_user(
        user_id=int(user_id),
        username=username,
        telegram_language=telegram_language,
    )
    return user


def build_me_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    clear_expired_ban(int(user.get("user_id") or 0))
    refreshed_user = get_user_by_id(int(user.get("user_id") or 0)) or user
    status = plan_status(refreshed_user)
    raw_plan = normalize_plan(refreshed_user.get("plan"))
    effective_plan = normalize_plan(status.get("plan") or raw_plan)
    subscription_status = str(status.get("status") or refreshed_user.get("subscription_status") or "free").lower()
    ban_state = resolve_ban_state(refreshed_user)

    if bool(ban_state.get("active")):
        effective_plan = raw_plan
        subscription_status = "banned"

    plan_for_display = raw_plan if raw_plan != "free" else effective_plan
    expires_at = status.get("expires") or refreshed_user.get("plan_end") or refreshed_user.get("trial_end")

    return {
        "user_id": int(refreshed_user.get("user_id") or 0),
        "username": refreshed_user.get("username"),
        "language": refreshed_user.get("language") or "es",
        "is_admin": bool(is_admin(int(refreshed_user.get("user_id") or 0))),
        "plan": plan_for_display,
        "plan_name": get_plan_name(plan_for_display),
        "subscription_status": subscription_status,
        "subscription_status_label": _label_subscription_status(subscription_status),
        "days_left": int(status.get("days_left") or 0),
        "expires_at": _iso(expires_at),
        "banned": bool(ban_state.get("active")),
        "ref_code": refreshed_user.get("ref_code"),
        "valid_referrals_total": int(refreshed_user.get("valid_referrals_total") or 0),
        "reward_days_total": int(refreshed_user.get("reward_days_total") or 0),
    }




def _finite_metric(value: Any, digits: Optional[int] = None) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(number):
        return None
    return round(number, digits) if digits is not None else number


def _serialize_performance_summary(summary: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = _empty_summary()
    if isinstance(summary, dict):
        base.update(summary)
    profit_factor_raw = base.get("profit_factor")
    return {
        "total": int(base.get("total") or 0),
        "resolved": int(base.get("resolved") or 0),
        "filled_total": int(base.get("filled_total") or 0),
        "won": int(base.get("won") or 0),
        "lost": int(base.get("lost") or 0),
        "expired": int(base.get("expired") or 0),
        "expired_no_fill": int(base.get("expired_no_fill") or 0),
        "expired_after_entry": int(base.get("expired_after_entry") or 0),
        "tp1": int(base.get("tp1") or 0),
        "tp2": int(base.get("tp2") or 0),
        "sl": int(base.get("sl") or 0),
        "winrate": _finite_metric(base.get("winrate"), 2) or 0.0,
        "loss_rate": _finite_metric(base.get("loss_rate"), 2) or 0.0,
        "expiry_rate": _finite_metric(base.get("expiry_rate"), 2) or 0.0,
        "fill_rate": _finite_metric(base.get("fill_rate"), 2) or 0.0,
        "no_fill_rate": _finite_metric(base.get("no_fill_rate"), 2) or 0.0,
        "post_fill_expiry_rate": _finite_metric(base.get("post_fill_expiry_rate"), 2) or 0.0,
        "after_entry_failure_rate": _finite_metric(base.get("after_entry_failure_rate"), 2) or 0.0,
        "gross_profit_r": _finite_metric(base.get("gross_profit_r"), 4) or 0.0,
        "gross_loss_r": _finite_metric(base.get("gross_loss_r"), 4) or 0.0,
        "net_r": _finite_metric(base.get("net_r"), 4) or 0.0,
        "profit_factor": _finite_metric(profit_factor_raw, 2),
        "profit_factor_infinite": bool(profit_factor_raw == float("inf")),
        "expectancy_r": _finite_metric(base.get("expectancy_r"), 4) or 0.0,
        "max_drawdown_r": _finite_metric(base.get("max_drawdown_r"), 4) or 0.0,
        "avg_resolution_minutes": _finite_metric(base.get("avg_resolution_minutes"), 2),
    }


def _serialize_performance_activity(activity: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = activity if isinstance(activity, dict) else {}
    return {
        "signals_total": int(base.get("signals_total") or 0),
        "avg_score": _finite_metric(base.get("avg_score"), 2),
    }


def _serialize_performance_window(payload: Optional[Dict[str, Any]], *, days: int, label: str, materialized: bool = False) -> Dict[str, Any]:
    base = payload if isinstance(payload, dict) else {}
    computed = base.get("computed_for_range") if isinstance(base.get("computed_for_range"), dict) else {}
    return {
        "days": int(days),
        "label": label,
        "materialized": bool(materialized),
        "summary": _serialize_performance_summary(base.get("summary")),
        "activity": _serialize_performance_activity(base.get("activity")),
        "computed_for_range": {
            "from": _iso(computed.get("from")),
            "to": _iso(computed.get("to")),
        },
    }


def _serialize_performance_breakdown_row(name: str, stats: Optional[Dict[str, Any]], activity: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "plan": name,
        "plan_name": get_plan_name(name),
        "summary": _serialize_performance_summary(stats),
        "activity": _serialize_performance_activity(activity),
    }


def _serialize_performance_direction_row(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    item = row if isinstance(row, dict) else {}
    profit_factor_raw = item.get("profit_factor")
    return {
        "direction": str(item.get("direction") or "—").upper(),
        "resolved": int(item.get("resolved") or 0),
        "won": int(item.get("won") or 0),
        "lost": int(item.get("lost") or 0),
        "expired": int(item.get("expired") or 0),
        "expired_no_fill": int(item.get("expired_no_fill") or 0),
        "expired_after_entry": int(item.get("expired_after_entry") or 0),
        "winrate": _finite_metric(item.get("winrate"), 2) or 0.0,
        "profit_factor": _finite_metric(profit_factor_raw, 2),
        "profit_factor_infinite": bool(profit_factor_raw == float("inf")),
        "expectancy_r": _finite_metric(item.get("expectancy_r"), 4) or 0.0,
    }


def _serialize_performance_setup_row(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    item = row if isinstance(row, dict) else {}
    profit_factor_raw = item.get("profit_factor")
    return {
        "setup_group": str(item.get("setup_group") or "—").upper(),
        "resolved": int(item.get("resolved") or 0),
        "won": int(item.get("won") or 0),
        "lost": int(item.get("lost") or 0),
        "expired": int(item.get("expired") or 0),
        "expired_no_fill": int(item.get("expired_no_fill") or 0),
        "expired_after_entry": int(item.get("expired_after_entry") or 0),
        "winrate": _finite_metric(item.get("winrate"), 2) or 0.0,
        "profit_factor": _finite_metric(profit_factor_raw, 2),
        "profit_factor_infinite": bool(profit_factor_raw == float("inf")),
        "expectancy_r": _finite_metric(item.get("expectancy_r"), 4) or 0.0,
    }


def _serialize_performance_symbol_row(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    item = row if isinstance(row, dict) else {}
    profit_factor_raw = item.get("profit_factor")
    return {
        "symbol": str(item.get("symbol") or "—").upper(),
        "resolved": int(item.get("resolved") or 0),
        "won": int(item.get("won") or 0),
        "lost": int(item.get("lost") or 0),
        "expired": int(item.get("expired") or 0),
        "expired_no_fill": int(item.get("expired_no_fill") or 0),
        "expired_after_entry": int(item.get("expired_after_entry") or 0),
        "winrate": _finite_metric(item.get("winrate"), 2) or 0.0,
        "loss_rate": _finite_metric(item.get("loss_rate"), 2) or 0.0,
        "profit_factor": _finite_metric(profit_factor_raw, 2),
        "profit_factor_infinite": bool(profit_factor_raw == float("inf")),
        "expectancy_r": _finite_metric(item.get("expectancy_r"), 4) or 0.0,
    }


def _serialize_performance_score_bucket(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    item = row if isinstance(row, dict) else {}
    return {
        "label": str(item.get("label") or "—"),
        "n": int(item.get("n") or 0),
        "won": int(item.get("won") or 0),
        "lost": int(item.get("lost") or 0),
        "winrate": _finite_metric(item.get("winrate"), 2) or 0.0,
        "net_r": _finite_metric(item.get("net_r"), 4) or 0.0,
    }


def _serialize_performance_diagnostics(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = payload if isinstance(payload, dict) else {}
    profit_factor_raw = base.get("profit_factor")
    return {
        "evaluated_total": int(base.get("evaluated_total") or 0),
        "resolved_total": int(base.get("resolved_total") or 0),
        "won": int(base.get("won") or 0),
        "lost": int(base.get("lost") or 0),
        "expired": int(base.get("expired") or 0),
        "expired_no_fill": int(base.get("expired_no_fill") or 0),
        "expired_after_entry": int(base.get("expired_after_entry") or 0),
        "filled_total": int(base.get("filled_total") or 0),
        "scanner_signals_total": int(base.get("scanner_signals_total") or 0),
        "pending_to_evaluate": int(base.get("pending_to_evaluate") or 0),
        "winrate": _finite_metric(base.get("winrate"), 2) or 0.0,
        "loss_rate": _finite_metric(base.get("loss_rate"), 2) or 0.0,
        "expiry_rate": _finite_metric(base.get("expiry_rate"), 2) or 0.0,
        "fill_rate": _finite_metric(base.get("fill_rate"), 2) or 0.0,
        "no_fill_rate": _finite_metric(base.get("no_fill_rate"), 2) or 0.0,
        "post_fill_expiry_rate": _finite_metric(base.get("post_fill_expiry_rate"), 2) or 0.0,
        "after_entry_failure_rate": _finite_metric(base.get("after_entry_failure_rate"), 2) or 0.0,
        "avg_result_score": _finite_metric(base.get("avg_result_score"), 2),
        "profit_factor": _finite_metric(profit_factor_raw, 2),
        "profit_factor_infinite": bool(profit_factor_raw == float("inf")),
        "expectancy_r": _finite_metric(base.get("expectancy_r"), 4) or 0.0,
        "max_drawdown_r": _finite_metric(base.get("max_drawdown_r"), 4) or 0.0,
        "avg_resolution_minutes": _finite_metric(base.get("avg_resolution_minutes"), 2),
    }


def build_performance_center_payload(user: Dict[str, Any], *, focus_days: int = 30) -> Dict[str, Any]:
    requested_focus = int(focus_days or 30)
    focus_days = requested_focus if requested_focus in {7, 30, 3650} else 30

    snapshot = _safe_call(get_performance_snapshot, {}) or {}
    total_materialized = _safe_call(lambda: get_materialized_window(3650), None)
    total_payload = total_materialized or (_safe_call(lambda: build_performance_window(3650), None) or {})

    windows = [
        _serialize_performance_window(
            {
                "summary": snapshot.get("summary_7d"),
                "activity": snapshot.get("activity_7d"),
            },
            days=7,
            label="7D",
            materialized=bool(snapshot.get("materialized_7d")),
        ),
        _serialize_performance_window(
            {
                "summary": snapshot.get("summary_30d"),
                "activity": snapshot.get("activity_30d"),
            },
            days=30,
            label="30D",
            materialized=bool(snapshot.get("materialized_30d")),
        ),
        _serialize_performance_window(
            total_payload,
            days=3650,
            label="Total",
            materialized=bool(total_materialized),
        ),
    ]

    focus_payload = next((item for item in windows if item["days"] == focus_days), windows[1])
    by_plan = snapshot.get("by_plan_30d") if isinstance(snapshot.get("by_plan_30d"), dict) else {}
    activity_by_plan = snapshot.get("activity_by_plan_30d") if isinstance(snapshot.get("activity_by_plan_30d"), dict) else {}

    return {
        "overview": {
            "focus_days": focus_payload["days"],
            "focus_label": focus_payload["label"],
            "user_plan": normalize_plan(plan_status(user).get("plan") or user.get("plan")),
            "windows": [{"days": item["days"], "label": item["label"], "materialized": item["materialized"]} for item in windows],
            "generated_at": utcnow().isoformat(),
        },
        "windows": windows,
        "focus": focus_payload,
        "plan_breakdown_30d": [
            _serialize_performance_breakdown_row("free", by_plan.get("free"), activity_by_plan.get("free")),
            _serialize_performance_breakdown_row("plus", by_plan.get("plus"), activity_by_plan.get("plus")),
            _serialize_performance_breakdown_row("premium", by_plan.get("premium"), activity_by_plan.get("premium")),
        ],
        "direction_30d": [_serialize_performance_direction_row(row) for row in (snapshot.get("direction_30d") or [])],
        "setup_groups_30d": [_serialize_performance_setup_row(row) for row in (snapshot.get("setup_groups_30d") or [])],
        "weak_symbols_30d": [_serialize_performance_symbol_row(row) for row in (snapshot.get("worst_symbols_30d") or [])],
        "score_buckets_30d": [_serialize_performance_score_bucket(row) for row in ((snapshot.get("by_score_30d") or {}).get("buckets") or [])],
        "diagnostics_30d": _serialize_performance_diagnostics(snapshot.get("diagnostics_30d")),
    }

def build_account_center_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    user_id = int(user.get("user_id") or 0)
    me = build_me_payload(user)
    status = plan_status(user)
    display_plan = normalize_plan(me.get("plan"))
    watchlist_default_limit = get_watchlist_limit_for_plan(display_plan)
    watchlist_default_slots = watchlist_default_limit

    watchlist_meta = _safe_call(
        lambda: build_watchlist_context(user)["meta"],
        {
            "symbols": [],
            "symbols_count": 0,
            "max_symbols": watchlist_default_limit,
            "slots_left": watchlist_default_slots,
            "plan": display_plan,
            "plan_name": get_plan_name(display_plan),
            "can_add_more": True,
        },
    )
    active_order = _safe_call(lambda: get_active_payment_order_for_user(user_id), None)
    recent_orders_raw = _safe_call(lambda: _load_recent_payment_orders(user_id), []) or []
    recent_orders = [serialize_order_public(order) for order in recent_orders_raw]
    billing_summary = _safe_call(lambda: _load_payment_order_summary(user_id), {"open": 0, "completed": 0, "expired": 0, "cancelled": 0, "total": 0})
    latest_completed_at = next((order.get("updated_at") or order.get("created_at") for order in recent_orders if str(order.get("status") or "") == "completed"), None)

    referral_stats = _safe_call(lambda: get_user_referral_stats(user_id), None) or {}
    referral_link = _safe_call(lambda: get_referral_link(user_id), f"https://t.me/share/url?url=HADES")
    referral_rewards = [
        _serialize_referral_reward(doc)
        for doc in (_safe_call(lambda: _load_recent_referral_rewards(user_id), []) or [])
    ]

    subscription_events = [
        _serialize_subscription_event(doc)
        for doc in (_safe_call(lambda: _load_recent_subscription_events(user_id), []) or [])
    ]

    last_purchase_plan = normalize_plan(user.get("last_purchase_plan"))

    subscription_payload = {
        "plan": display_plan,
        "plan_name": get_plan_name(display_plan),
        "status": me.get("subscription_status"),
        "status_label": me.get("subscription_status_label"),
        "days_left": int(me.get("days_left") or 0),
        "expires_at": me.get("expires_at"),
        "plan_started_at": _iso(user.get("plan_started_at")),
        "last_purchase_at": _iso(user.get("last_purchase_at")),
        "last_purchase_plan": last_purchase_plan,
        "last_purchase_plan_name": get_plan_name(last_purchase_plan),
        "last_purchase_days": int(user.get("last_purchase_days") or 0),
        "last_entitlement_source": user.get("last_entitlement_source"),
        "features": _plan_features(display_plan),
        "is_trial": str(me.get("subscription_status") or "") == "trial",
        "is_paid": display_plan in {"plus", "premium"} and str(me.get("subscription_status") or "") == "active",
        "can_upgrade_plus": display_plan == "free",
        "can_upgrade_premium": display_plan in {"free", "plus"},
        "watchlist": watchlist_meta,
    }
    payment_config_status = get_payment_configuration_status()
    payment_config_ready = bool(payment_config_status.get("ready"))
    active_order_public = serialize_order_public(active_order)
    billing_payload = {
        "payment_config_ready": payment_config_ready,
        "payment_config_status": payment_config_status,
        "active_order": active_order_public,
        "recent_orders": recent_orders,
        "summary": billing_summary,
        "latest_completed_at": latest_completed_at,
    }
    billing_payload["focus"] = _build_billing_focus(
        payment_config_ready=payment_config_ready,
        active_order=active_order_public,
        billing_summary=billing_summary,
        subscription=subscription_payload,
        payment_config_status=payment_config_status,
    )

    return {
        "overview": {
            **me,
            "watchlist_symbols": int(watchlist_meta.get("symbols_count") or 0),
            "watchlist_limit": watchlist_meta.get("max_symbols"),
            "watchlist_slots_left": watchlist_meta.get("slots_left"),
        },
        "subscription": subscription_payload,
        "billing": billing_payload,
        "referrals": {
            "ref_code": me.get("ref_code"),
            "referral_link": referral_link,
            "share_text": f"Únete a HADES Alpha con mi enlace: {referral_link}",
            "total_referred": int(referral_stats.get("total_referred") or 0),
            "plus_referred": int(referral_stats.get("plus_referred") or 0),
            "premium_referred": int(referral_stats.get("premium_referred") or 0),
            "current_plus": int(referral_stats.get("current_plus") or 0),
            "current_premium": int(referral_stats.get("current_premium") or 0),
            "valid_referrals_total": int(referral_stats.get("valid_referrals_total") or 0),
            "reward_days_total": int(referral_stats.get("reward_days_total") or 0),
            "reward_rules": list(referral_stats.get("pending_rewards") or get_referral_reward_rules()),
            "recent_rewards": referral_rewards,
        },
        "plans": _safe_call(lambda: build_plans_payload(display_plan), {"plus": [], "premium": []}),
        "timeline": subscription_events,
        "support": {
            "url": "https://chat.whatsapp.com/JXxSGjaKtqRH9c0jTlGv2l?mode=gi_t",
            "label": "Soporte HADES",
        },
        "settings": {
            "language": normalize_language(user.get("language") or "es"),
            "push_alerts": _serialize_push_preferences(user),
        },
        "generated_at": utcnow().isoformat(),
    }


def build_dashboard_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    user_id = int(user.get("user_id") or 0)
    snapshot = _safe_call(get_performance_snapshot, {}) or {}
    summary_7d = _serialize_performance_summary(snapshot.get("summary_7d"))
    summary_30d = _serialize_performance_summary(snapshot.get("summary_30d"))
    home_summary_raw, home_summary_label = _select_dashboard_summary(snapshot)
    home_summary = _serialize_performance_summary(home_summary_raw)

    active_query = {"user_id": user_id, "telegram_valid_until": {"$gte": datetime.utcnow()}}
    active_signals = _safe_call(
        lambda: list(
            user_signals_collection()
            .find(active_query)
            .sort("created_at", -1)
            .limit(6)
        ),
        [],
    )
    latest_signals = _safe_call(
        lambda: list(
            user_signals_collection()
            .find({"user_id": user_id})
            .sort("created_at", -1)
            .limit(30)
        ),
        [],
    )
    recent_history = _safe_call(
        lambda: get_history_entries_for_user(user_id, user_plan=user.get("plan"), limit=5),
        [],
    )
    active_order = _safe_call(lambda: get_active_payment_order_for_user(user_id), None)
    watchlist_doc = _safe_call(lambda: watchlists_collection().find_one({"user_id": user_id}) or {}, {})

    signal_mix = {"free": 0, "plus": 0, "premium": 0}
    active_mix = {"free": 0, "plus": 0, "premium": 0}

    for doc in latest_signals:
        visibility = normalize_plan(doc.get("visibility"))
        if visibility in signal_mix:
            signal_mix[visibility] += 1
    for doc in active_signals:
        visibility = normalize_plan(doc.get("visibility"))
        if visibility in active_mix:
            active_mix[visibility] += 1

    active_count = _safe_call(lambda: int(user_signals_collection().count_documents(active_query)), len(active_signals))

    return {
        "summary_7d": summary_7d,
        "summary_30d": summary_30d,
        "home_summary": home_summary,
        "home_summary_label": home_summary_label,
        "active_signals_count": active_count,
        "recent_signals": _safe_serialize_items(active_signals, _serialize_signal),
        "recent_history": _safe_serialize_items(recent_history, _serialize_history),
        "active_payment_order": _safe_serialize_optional(active_order, serialize_order_public),
        "watchlist_count": len(watchlist_doc.get("symbols") or []),
        "signal_mix": signal_mix,
        "active_mix": active_mix,
    }




def _latest_activity_iso(docs: List[Dict[str, Any]]) -> Optional[str]:
    latest: Optional[datetime] = None
    for doc in docs or []:
        for candidate in (doc.get("updated_at"), doc.get("created_at"), doc.get("signal_created_at")):
            if isinstance(candidate, datetime) and (latest is None or candidate > latest):
                latest = candidate
    return _iso(latest)


def build_live_signals_feed_meta(
    user: Dict[str, Any],
    *,
    active_limit: int = 6,
    signals_limit: int = 20,
) -> Dict[str, Any]:
    user_id = int(user.get("user_id") or 0)
    now = utcnow()
    active_query = {"user_id": user_id, "telegram_valid_until": {"$gte": now}}
    projection = {"updated_at": 1, "created_at": 1, "signal_created_at": 1}

    active_meta_docs = _safe_call(
        lambda: list(
            user_signals_collection()
            .find(active_query, projection)
            .sort("created_at", -1)
            .limit(max(1, int(active_limit)))
        ),
        [],
    )
    recent_meta_docs = _safe_call(
        lambda: list(
            user_signals_collection()
            .find({"user_id": user_id}, projection)
            .sort("created_at", -1)
            .limit(max(1, int(signals_limit)))
        ),
        [],
    )
    active_count = _safe_call(lambda: int(user_signals_collection().count_documents(active_query)), len(active_meta_docs))
    latest_activity = _latest_activity_iso(recent_meta_docs) or _latest_activity_iso(active_meta_docs)
    latest_active_activity = _latest_activity_iso(active_meta_docs)
    feed_version = "|".join([
        str(active_count),
        latest_activity or "",
        latest_active_activity or "",
        str(len(recent_meta_docs)),
    ])

    return {
        "active_signals_count": active_count,
        "latest_signal_activity_at": latest_activity,
        "latest_active_signal_activity_at": latest_active_activity,
        "feed_version": feed_version,
        "generated_at": utcnow().isoformat(),
    }


def build_live_signals_payload(
    user: Dict[str, Any],
    *,
    active_limit: int = 6,
    signals_limit: int = 20,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    user_id = int(user.get("user_id") or 0)
    now = utcnow()
    active_query = {"user_id": user_id, "telegram_valid_until": {"$gte": now}}
    meta = meta or build_live_signals_feed_meta(user, active_limit=active_limit, signals_limit=signals_limit)

    active_docs = _safe_call(
        lambda: list(
            user_signals_collection()
            .find(active_query)
            .sort("created_at", -1)
            .limit(max(1, int(active_limit)))
        ),
        [],
    )
    recent_docs = _safe_call(
        lambda: list(
            user_signals_collection()
            .find({"user_id": user_id})
            .sort("created_at", -1)
            .limit(max(1, int(signals_limit)))
        ),
        [],
    )

    return {
        "active_signals_count": int(meta.get("active_signals_count") or 0),
        "recent_signals": [_serialize_signal(doc) for doc in active_docs],
        "signals": [_serialize_signal(doc) for doc in recent_docs],
        "latest_signal_activity_at": meta.get("latest_signal_activity_at"),
        "latest_active_signal_activity_at": meta.get("latest_active_signal_activity_at"),
        "feed_version": meta.get("feed_version"),
        "generated_at": meta.get("generated_at") or utcnow().isoformat(),
    }


def build_signals_payload(user: Dict[str, Any], *, limit: int = 20) -> List[Dict[str, Any]]:
    user_id = int(user.get("user_id") or 0)
    docs = list(
        user_signals_collection()
        .find({"user_id": user_id})
        .sort("created_at", -1)
        .limit(max(1, int(limit)))
    )
    return [_serialize_signal(doc) for doc in docs]


def build_history_payload(user: Dict[str, Any], *, limit: int = 20) -> List[Dict[str, Any]]:
    docs = get_history_entries_for_user(int(user.get("user_id") or 0), user_plan=user.get("plan"), limit=limit)
    return [_serialize_history(doc) for doc in docs]


def build_market_payload(user: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    user_id = int((user or {}).get("user_id") or 0)
    cache_key = f"market:{user_id}"
    cached = _cache_get_payload(_MARKET_PAYLOAD_CACHE, cache_key)
    if cached is not None:
        return cached

    snapshot = get_market_state_snapshot() or {}
    radar_items, radar_summary = _safe_call(
        lambda: _serialize_radar(user_id, limit=24, market_snapshot=snapshot),
        ([], {"total": 0, "longs": 0, "shorts": 0, "hot": 0, "immediate": 0, "active_signals": 0, "sort_default": "ranking"}),
    )
    snapshot["radar"] = radar_items
    snapshot["radar_summary"] = radar_summary
    snapshot["top_gainers"] = list(snapshot.get("top_gainers") or [])[:5]
    snapshot["top_losers"] = list(snapshot.get("top_losers") or [])[:5]
    snapshot["top_volume"] = list(snapshot.get("top_volume") or [])[:5]
    snapshot["top_open_interest"] = list(snapshot.get("top_open_interest") or [])[:4]
    _cache_set_payload(_MARKET_PAYLOAD_CACHE, cache_key, snapshot, _MARKET_PAYLOAD_TTL_SECONDS)
    return deepcopy(snapshot)


def build_watchlist_context(user: Dict[str, Any]) -> Dict[str, Any]:
    raw_symbols = get_watchlist(int(user.get("user_id") or 0))
    status = plan_status(user)
    plan_value = normalize_plan(status.get("plan") or user.get("plan"))
    max_symbols = get_watchlist_limit_for_plan(plan_value)
    symbols_count = len(raw_symbols)
    slots_left = None if max_symbols is None else max(max_symbols - symbols_count, 0)
    return {
        "items": _serialize_watchlist(raw_symbols, user_id=int(user.get("user_id") or 0)),
        "meta": {
            "symbols": raw_symbols,
            "symbols_count": symbols_count,
            "max_symbols": max_symbols,
            "slots_left": slots_left,
            "plan": plan_value,
            "plan_name": get_plan_name(plan_value),
            "can_add_more": True if max_symbols is None else symbols_count < max_symbols,
        },
    }


def build_watchlist_payload(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    return build_watchlist_context(user)["items"]


def build_plans_payload(current_plan: Optional[str] = None) -> Dict[str, Any]:
    current_value = normalize_plan(current_plan)
    catalog = get_plan_catalog()
    enriched: Dict[str, Any] = {}
    for plan, rows in catalog.items():
        enriched[plan] = []
        for row in rows:
            item = dict(row)
            item["plan_name"] = get_plan_name(plan)
            item["is_current_plan"] = current_value == normalize_plan(plan)
            item["features"] = _plan_features(plan)
            enriched[plan].append(item)
    return enriched


def build_bootstrap_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    me_payload = _safe_call(lambda: build_me_payload(user), {
        "user_id": int(user.get("user_id") or 0),
        "username": user.get("username"),
        "language": user.get("language") or "es",
        "plan": normalize_plan(user.get("plan")),
        "plan_name": get_plan_name(user.get("plan")),
        "subscription_status": str(user.get("subscription_status") or "free").lower(),
        "subscription_status_label": _label_subscription_status(user.get("subscription_status") or "free"),
        "is_admin": bool(is_admin(int(user.get("user_id") or 0))),
        "days_left": 0,
        "expires_at": None,
        "banned": bool(resolve_ban_state(user).get("active")),
        "ref_code": user.get("ref_code"),
        "valid_referrals_total": int(user.get("valid_referrals_total") or 0),
        "reward_days_total": int(user.get("reward_days_total") or 0),
    })
    me_plan = normalize_plan(me_payload.get("plan"))
    watchlist_limit_default = get_watchlist_limit_for_plan(me_plan)
    watchlist_slots_default = watchlist_limit_default

    return {
        "bootstrap_mode": "light",
        "me": me_payload,
        "dashboard": {
            "summary_7d": _empty_summary(),
            "summary_30d": _empty_summary(),
            "home_summary": _empty_summary(),
            "home_summary_label": "7D",
            "recent_signals": [],
            "recent_history": [],
            "active_signals_count": 0,
            "watchlist_count": 0,
            "signal_mix": {"free": 0, "plus": 0, "premium": 0},
            "active_mix": {"free": 0, "plus": 0, "premium": 0},
            "active_payment_order": None,
        },
        "signals": [],
        "history": [],
        "market": {
            "fear_greed": 0,
            "btc_dominance": 0,
            "top_gainers": [],
            "top_losers": [],
            "top_volume": [],
            "top_open_interest": [],
            "radar": [],
            "radar_summary": {"total": 0},
            "radar_context": {
                "bias": "neutral",
                "regime": "neutral",
                "environment": "—",
                "recommendation": "Cargando lectura de mercado...",
            },
            "bias": "—",
            "regime": "—",
            "volatility": "—",
            "environment": "—",
            "recommendation": "Cargando lectura de mercado...",
        },
        "watchlist": [],
        "watchlist_meta": {
            "symbols": [],
            "symbols_count": 0,
            "max_symbols": watchlist_limit_default,
            "slots_left": watchlist_slots_default,
            "plan": me_plan,
            "plan_name": get_plan_name(me_plan),
            "can_add_more": True,
        },
        "plans": {"plus": [], "premium": []},
        "account": {},
        "support_url": "https://chat.whatsapp.com/JXxSGjaKtqRH9c0jTlGv2l?mode=gi_t",
        "bot_username": get_bot_username(),
        "generated_at": utcnow().isoformat(),
    }


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    return users_collection().find_one({"user_id": int(user_id)})
