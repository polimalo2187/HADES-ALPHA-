from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# =========================
# CONSTANTES CONFIGURABLES
# =========================
TRIAL_DAYS = 7
USER_SCHEMA_VERSION = 4
REFERRAL_SCHEMA_VERSION = 3
SIGNAL_SCHEMA_VERSION = 1
USER_SIGNAL_SCHEMA_VERSION = 1
SIGNAL_RESULT_SCHEMA_VERSION = 1
WATCHLIST_SCHEMA_VERSION = 1
SIGNAL_JOB_SCHEMA_VERSION = 1
SIGNAL_DELIVERY_SCHEMA_VERSION = 1
STATS_SNAPSHOT_SCHEMA_VERSION = 4
SIGNAL_HISTORY_SCHEMA_VERSION = 1
SUBSCRIPTION_EVENT_SCHEMA_VERSION = 1
PAYMENT_ORDER_SCHEMA_VERSION = 1
PAYMENT_VERIFICATION_LOG_SCHEMA_VERSION = 1
AUDIT_LOG_SCHEMA_VERSION = 1
HEALTH_STATUS_SCHEMA_VERSION = 1
SCANNER_CYCLE_STAT_SCHEMA_VERSION = 1


def utcnow() -> datetime:
    return datetime.utcnow()


# =========================
# USER MODEL
# =========================
def new_user(
    user_id: int,
    username: Optional[str],
    referred_by: Optional[int] = None,
    language: Optional[str] = "es",
) -> Dict[str, Any]:
    now = utcnow()

    return {
        "user_id": user_id,
        "username": username,
        "plan": "free",
        "trial_end": now + timedelta(days=TRIAL_DAYS),
        "plan_end": None,
        "subscription_status": "trial",
        "plan_started_at": None,
        "last_plan_change_at": now,
        "last_purchase_at": None,
        "last_purchase_plan": None,
        "last_purchase_days": 0,
        "last_entitlement_source": None,
        "queued_plus_seconds": 0,
        "queued_plus_origin": None,
        "ref_code": f"ref_{user_id}",
        "referred_by": referred_by,
        "ref_plus_valid": 0,
        "ref_premium_valid": 0,
        "ref_plus_total": 0,
        "ref_premium_total": 0,
        "valid_referrals_total": 0,
        "reward_days_total": 0,
        "daily_signal_count": 0,
        "daily_signal_date": now.date().isoformat(),
        "last_signal_id": None,
        "last_signal_at": None,
        "language": language or "es",
        "miniapp_settings": {
            "push_alerts": {
                "enabled": True,
                "tiers": {
                    "free": True,
                    "plus": False,
                    "premium": False,
                },
            },
        },
        "onboarding_seen": False,
        "onboarding_completed": False,
        "onboarding_version": 0,
        "banned": False,
        "schema_version": USER_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
        "last_activity": now,
    }



def user_backfill_patch(existing_user: Dict[str, Any], *, user_id: int) -> Dict[str, Any]:
    now = utcnow()
    patch: Dict[str, Any] = {}
    defaults = new_user(user_id=user_id, username=existing_user.get("username"), referred_by=existing_user.get("referred_by"), language=existing_user.get("language") or "es")

    for key, value in defaults.items():
        if key not in existing_user:
            patch[key] = value

    if existing_user.get("schema_version") != USER_SCHEMA_VERSION:
        patch["schema_version"] = USER_SCHEMA_VERSION

    if patch:
        patch["updated_at"] = now
        patch["last_activity"] = existing_user.get("last_activity", now)

    return patch



def update_timestamp(doc: Dict[str, Any]) -> Dict[str, Any]:
    updated_doc = doc.copy()
    updated_doc["updated_at"] = utcnow()
    return updated_doc



def activate_plan(user: Dict[str, Any], plan: str, days: int = 30) -> Dict[str, Any]:
    now = utcnow()

    if user.get("plan_end") and user["plan_end"] > now:
        user["plan_end"] = user["plan_end"] + timedelta(days=days)
    else:
        user["plan_end"] = now + timedelta(days=days)

    user["plan"] = plan
    user["trial_end"] = None
    user["subscription_status"] = "active"
    user["plan_started_at"] = user.get("plan_started_at") or now
    user["last_plan_change_at"] = now
    user["last_purchase_at"] = now
    user["last_purchase_plan"] = plan
    user["last_purchase_days"] = int(days)
    user["schema_version"] = USER_SCHEMA_VERSION
    return update_timestamp(user)



def is_trial_active(user: Dict[str, Any]) -> bool:
    if user.get("trial_end") is None:
        return False
    return user["trial_end"] >= utcnow()



def is_plan_active(user: Dict[str, Any]) -> bool:
    now = utcnow()
    plan_end = user.get("plan_end")
    if plan_end is not None and plan_end >= now:
        return True
    queued_plus_seconds = int(user.get("queued_plus_seconds") or 0)
    return queued_plus_seconds > 0


# =========================
# REFERRAL MODEL
# =========================
def new_referral(
    referrer_id: int,
    referred_id: int,
    activated_plan: str,
    reward_days_applied: int = 7,
    activated_days: int = 30,
    reward_plan_applied: Optional[str] = None,
    purchase_key: Optional[str] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "referrer_id": referrer_id,
        "referred_id": referred_id,
        "activated_plan": activated_plan,
        "activated_days": int(activated_days),
        "activated_at": now,
        "reward_days_applied": int(reward_days_applied),
        "reward_plan_applied": reward_plan_applied or activated_plan,
        "purchase_key": str(purchase_key or f"legacy:{referrer_id}:{referred_id}:{activated_plan}:{activated_days}"),
        "schema_version": REFERRAL_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }




# =========================
# SUBSCRIPTION EVENT MODEL
# =========================
def new_subscription_event(
    user_id: int,
    event_type: str,
    plan: Optional[str],
    days: int,
    source: str,
    before_plan: Optional[str],
    after_plan: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "user_id": int(user_id),
        "event_type": str(event_type),
        "plan": plan,
        "days": int(days),
        "source": str(source),
        "before_plan": before_plan,
        "after_plan": after_plan,
        "metadata": metadata or {},
        "schema_version": SUBSCRIPTION_EVENT_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }




# =========================
# PAYMENT ORDER MODEL
# =========================
def new_payment_order(
    *,
    order_id: str,
    user_id: int,
    plan: str,
    days: int,
    base_price_usdt: float,
    amount_usdt: float,
    network: str,
    token_symbol: str,
    token_contract: str,
    deposit_address: str,
    expires_at: datetime,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "order_id": str(order_id),
        "user_id": int(user_id),
        "plan": str(plan),
        "days": int(days),
        "base_price_usdt": float(base_price_usdt),
        "amount_usdt": float(amount_usdt),
        "network": str(network),
        "token_symbol": str(token_symbol),
        "token_contract": str(token_contract).lower(),
        "deposit_address": str(deposit_address).lower(),
        "declared_sender_address": None,
        "status": "awaiting_payment",
        "verification_attempts": 0,
        "verification_started_at": None,
        "verification_lock_token": None,
        "last_verification_reason": None,
        "matched_from": None,
        "matched_to": None,
        "matched_amount": None,
        "confirmations": 0,
        "confirmed_at": None,
        "expires_at": expires_at,
        "schema_version": PAYMENT_ORDER_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }


# =========================
# PAYMENT VERIFICATION LOG MODEL
# =========================
def new_payment_verification_log(
    *,
    order_id: str,
    user_id: int,
    status: str,
    reason: str,
    tx_hash: Optional[str] = None,
    from_address: Optional[str] = None,
    to_address: Optional[str] = None,
    amount_usdt: Optional[float] = None,
    confirmations: Optional[int] = None,
    raw: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "order_id": str(order_id),
        "user_id": int(user_id),
        "status": str(status),
        "reason": str(reason),
        "tx_hash": tx_hash,
        "from_address": (from_address or "").lower() or None,
        "to_address": (to_address or "").lower() or None,
        "amount_usdt": float(amount_usdt) if amount_usdt is not None else None,
        "confirmations": int(confirmations) if confirmations is not None else None,
        "raw": raw or {},
        "schema_version": PAYMENT_VERIFICATION_LOG_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }


# =========================
# OBSERVABILITY MODELS
# =========================
def new_audit_log(
    *,
    event_type: str,
    status: str,
    module: str,
    user_id: Optional[int] = None,
    admin_id: Optional[int] = None,
    signal_id: Optional[str] = None,
    order_id: Optional[str] = None,
    callback: Optional[str] = None,
    message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "event_type": str(event_type),
        "status": str(status),
        "module": str(module),
        "user_id": int(user_id) if user_id is not None else None,
        "admin_id": int(admin_id) if admin_id is not None else None,
        "signal_id": str(signal_id) if signal_id is not None else None,
        "order_id": str(order_id) if order_id is not None else None,
        "callback": str(callback) if callback is not None else None,
        "message": str(message) if message is not None else None,
        "metadata": metadata or {},
        "schema_version": AUDIT_LOG_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }


def new_health_status(
    *,
    component: str,
    status: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "component": str(component),
        "status": str(status),
        "details": details or {},
        "schema_version": HEALTH_STATUS_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }



def new_scanner_cycle_stat(
    *,
    cycle_number: int,
    status: str,
    cycle_started_at: datetime,
    duration_seconds: float,
    scan_interval_seconds: int,
    bootstrap_mode: bool,
    universe_symbols_total: int,
    active_symbols_total: int,
    attempted_symbols_total: int,
    candidate_pool_total: int,
    selected_signals_total: int,
    rejected_symbols_total: int,
    risk_off_symbols_total: int,
    failure_symbols_total: int,
    failure_samples: Optional[List[str]] = None,
    market_regime: Optional[Dict[str, Any]] = None,
    attempts_by_strategy: Optional[Dict[str, int]] = None,
    candidate_pool_by_strategy: Optional[Dict[str, int]] = None,
    selected_by_strategy: Optional[Dict[str, int]] = None,
    rejected_by_strategy: Optional[Dict[str, int]] = None,
    reject_reasons: Optional[Dict[str, int]] = None,
    reject_reasons_by_strategy: Optional[Dict[str, Dict[str, int]]] = None,
    cache_stats: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "cycle_number": int(cycle_number),
        "status": str(status),
        "cycle_started_at": cycle_started_at,
        "duration_seconds": round(float(duration_seconds), 6),
        "scan_interval_seconds": int(scan_interval_seconds),
        "bootstrap_mode": bool(bootstrap_mode),
        "universe_symbols_total": int(universe_symbols_total),
        "active_symbols_total": int(active_symbols_total),
        "attempted_symbols_total": int(attempted_symbols_total),
        "candidate_pool_total": int(candidate_pool_total),
        "selected_signals_total": int(selected_signals_total),
        "rejected_symbols_total": int(rejected_symbols_total),
        "risk_off_symbols_total": int(risk_off_symbols_total),
        "failure_symbols_total": int(failure_symbols_total),
        "failure_samples": list(failure_samples or []),
        "market_regime": market_regime or {},
        "attempts_by_strategy": attempts_by_strategy or {},
        "candidate_pool_by_strategy": candidate_pool_by_strategy or {},
        "selected_by_strategy": selected_by_strategy or {},
        "rejected_by_strategy": rejected_by_strategy or {},
        "reject_reasons": reject_reasons or {},
        "reject_reasons_by_strategy": reject_reasons_by_strategy or {},
        "cache_stats": cache_stats or {},
        "schema_version": SCANNER_CYCLE_STAT_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }

# =========================
# SIGNAL MODEL
# =========================
def new_signal(
    symbol: str,
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profits: List[float],
    timeframes: List[str],
    visibility: str,
    leverage: Optional[Dict[str, str]] = None,
    components: Optional[List[Any]] = None,
    score: Optional[float] = None,
) -> Dict[str, Any]:
    """Crea un diccionario base de señal listo para MongoDB."""
    now = utcnow()
    return {
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "take_profits": take_profits,
        "timeframes": timeframes,
        "leverage": leverage or {
            "conservador": "5x-10x",
            "moderado": "10x-20x",
            "agresivo": "30x-40x",
        },
        "visibility": visibility,
        "components": components or [],
        "score": score,
        "evaluated": False,
        "schema_version": SIGNAL_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }



def new_user_signal(
    *,
    user_id: int,
    signal_id: str,
    symbol: str,
    direction: str,
    entry_price: float,
    entry_zone: Dict[str, float],
    profiles: Dict[str, Dict[str, Any]],
    leverage_profiles: Dict[str, str],
    timeframes: List[str],
    valid_until: datetime,
    evaluation_valid_until: datetime,
    telegram_valid_until: datetime,
    fingerprint: str,
    visibility: str,
    score: Optional[float] = None,
    normalized_score: Optional[float] = None,
    components: Optional[List[Any]] = None,
    raw_components: Optional[List[Any]] = None,
    normalized_components: Optional[List[Any]] = None,
    setup_group: Optional[str] = None,
    score_profile: Optional[str] = None,
    score_calibration: Optional[str] = None,
    atr_pct: Optional[float] = None,
    market_validity_minutes: Optional[int] = None,
    telegram_visibility_minutes: Optional[int] = None,
    evaluation_scope_version: Optional[str] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "user_id": int(user_id),
        "signal_id": str(signal_id),
        "symbol": symbol,
        "direction": direction,
        "entry_price": float(entry_price),
        "entry_zone": entry_zone,
        "profiles": profiles,
        "leverage_profiles": leverage_profiles,
        "timeframes": timeframes,
        "created_at": now,
        "updated_at": now,
        "valid_until": valid_until,
        "evaluation_valid_until": evaluation_valid_until,
        "telegram_valid_until": telegram_valid_until,
        "fingerprint": fingerprint,
        "visibility": visibility,
        "score": score,
        "normalized_score": normalized_score,
        "components": components or [],
        "raw_components": raw_components or [],
        "normalized_components": normalized_components or [],
        "setup_group": setup_group,
        "score_profile": score_profile,
        "score_calibration": score_calibration,
        "atr_pct": atr_pct,
        "market_validity_minutes": market_validity_minutes,
        "telegram_visibility_minutes": telegram_visibility_minutes,
        "evaluation_scope_version": evaluation_scope_version,
        "evaluated": False,
        "schema_version": USER_SIGNAL_SCHEMA_VERSION,
    }



def new_signal_result(
    *,
    base_signal_id: str,
    signal_id: str,
    user_id: Optional[int],
    symbol: Optional[str],
    direction: Optional[str],
    visibility: Optional[str],
    plan: Optional[str],
    score: Optional[float],
    normalized_score: Optional[float] = None,
    setup_group: Optional[str] = None,
    send_mode: Optional[str] = None,
    strategy_name: Optional[str] = None,
    strategy_version: Optional[str] = None,
    regime_state: Optional[str] = None,
    regime_reason: Optional[str] = None,
    result: str,
    evaluated_profile: str,
    evaluation_scope: str,
    evaluation_scope_version: Optional[str],
    tp_used: Optional[float],
    sl_used: Optional[float],
    entry_price: Optional[float] = None,
    risk_pct: Optional[float] = None,
    reward_pct: Optional[float] = None,
    r_multiple: Optional[float] = None,
    resolution_minutes: Optional[float] = None,
    signal_created_at: Optional[datetime] = None,
    signal_valid_until: Optional[datetime] = None,
    evaluation_valid_until: Optional[datetime] = None,
    telegram_valid_until: Optional[datetime] = None,
    market_validity_minutes: Optional[int] = None,
    entry_touched: Optional[bool] = None,
    entry_touched_at: Optional[datetime] = None,
    expiry_type: Optional[str] = None,
    expiry_reason: Optional[str] = None,
    tp1_progress_max_pct: Optional[float] = None,
    max_favorable_excursion_r: Optional[float] = None,
    max_adverse_excursion_r: Optional[float] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "base_signal_id": str(base_signal_id),
        "signal_id": str(signal_id),
        "user_id": user_id,
        "symbol": symbol,
        "direction": direction,
        "visibility": visibility,
        "plan": plan,
        "score": score,
        "normalized_score": normalized_score,
        "setup_group": setup_group,
        "send_mode": send_mode,
        "strategy_name": strategy_name,
        "strategy_version": strategy_version,
        "regime_state": regime_state,
        "regime_reason": regime_reason,
        "result": result,
        "evaluated_at": now,
        "evaluated_profile": evaluated_profile,
        "evaluation_scope": evaluation_scope,
        "evaluation_scope_version": evaluation_scope_version,
        "tp_used": tp_used,
        "sl_used": sl_used,
        "entry_price": entry_price,
        "risk_pct": risk_pct,
        "reward_pct": reward_pct,
        "r_multiple": r_multiple,
        "resolution_minutes": resolution_minutes,
        "signal_created_at": signal_created_at,
        "signal_valid_until": signal_valid_until,
        "evaluation_valid_until": evaluation_valid_until,
        "telegram_valid_until": telegram_valid_until,
        "market_validity_minutes": market_validity_minutes,
        "entry_touched": entry_touched,
        "entry_touched_at": entry_touched_at,
        "expiry_type": expiry_type,
        "expiry_reason": expiry_reason,
        "tp1_progress_max_pct": tp1_progress_max_pct,
        "max_favorable_excursion_r": max_favorable_excursion_r,
        "max_adverse_excursion_r": max_adverse_excursion_r,
        "schema_version": SIGNAL_RESULT_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }



def new_watchlist(user_id: int, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    now = utcnow()
    return {
        "user_id": int(user_id),
        "symbols": list(symbols or []),
        "schema_version": WATCHLIST_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }


# =========================
# SIGNAL PIPELINE MODELS
# =========================
def new_signal_job(
    *,
    signal_id: str,
    visibility: str,
    status: str = "queued",
    attempt_count: int = 0,
    next_retry_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "signal_id": str(signal_id),
        "visibility": visibility,
        "status": status,
        "attempt_count": int(attempt_count),
        "last_error": None,
        "enqueued_at": now,
        "processing_started_at": None,
        "processing_finished_at": None,
        "next_retry_at": next_retry_at,
        "schema_version": SIGNAL_JOB_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }


def new_signal_delivery(
    *,
    signal_id: str,
    user_id: int,
    visibility: str,
    status: str = "queued",
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "signal_id": str(signal_id),
        "user_id": int(user_id),
        "visibility": visibility,
        "status": status,
        "message_id": None,
        "error": None,
        "created_at": now,
        "updated_at": now,
        "queued_at": now,
        "sent_at": None,
        "schema_version": SIGNAL_DELIVERY_SCHEMA_VERSION,
    }


# =========================
# MATERIALIZED STATS SNAPSHOT MODEL
# =========================
def new_stats_snapshot(
    *,
    key: str,
    window_days: int,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "key": str(key),
        "window_days": int(window_days),
        "payload": payload,
        "computed_at": now,
        "schema_version": STATS_SNAPSHOT_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }


# =========================
# SIGNAL HISTORY MODEL
# =========================
def new_signal_history_record(
    *,
    signal_id: str,
    result_id: Optional[str],
    symbol: Optional[str],
    direction: Optional[str],
    visibility: Optional[str],
    score: Optional[float],
    normalized_score: Optional[float],
    setup_group: Optional[str],
    score_profile: Optional[str],
    score_calibration: Optional[str],
    result: Optional[str],
    entry_price: Optional[float],
    stop_loss: Optional[float],
    tp1: Optional[float],
    tp2: Optional[float],
    risk_pct: Optional[float],
    reward_pct: Optional[float],
    r_multiple: Optional[float],
    resolution_minutes: Optional[float],
    market_validity_minutes: Optional[int],
    signal_created_at: Optional[datetime],
    signal_valid_until: Optional[datetime],
    evaluation_valid_until: Optional[datetime],
    telegram_valid_until: Optional[datetime],
    timeframes: Optional[List[str]] = None,
    entry_touched: Optional[bool] = None,
    entry_touched_at: Optional[datetime] = None,
    expiry_type: Optional[str] = None,
    expiry_reason: Optional[str] = None,
    tp1_progress_max_pct: Optional[float] = None,
    max_favorable_excursion_r: Optional[float] = None,
    max_adverse_excursion_r: Optional[float] = None,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "signal_id": str(signal_id),
        "result_id": str(result_id) if result_id else None,
        "symbol": symbol,
        "direction": direction,
        "visibility": visibility,
        "score": score,
        "normalized_score": normalized_score,
        "setup_group": setup_group,
        "score_profile": score_profile,
        "score_calibration": score_calibration,
        "result": result,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "tp1": tp1,
        "tp2": tp2,
        "risk_pct": risk_pct,
        "reward_pct": reward_pct,
        "r_multiple": r_multiple,
        "resolution_minutes": resolution_minutes,
        "market_validity_minutes": market_validity_minutes,
        "signal_created_at": signal_created_at,
        "signal_valid_until": signal_valid_until,
        "evaluation_valid_until": evaluation_valid_until,
        "telegram_valid_until": telegram_valid_until,
        "timeframes": list(timeframes or []),
        "entry_touched": entry_touched,
        "entry_touched_at": entry_touched_at,
        "expiry_type": expiry_type,
        "expiry_reason": expiry_reason,
        "tp1_progress_max_pct": tp1_progress_max_pct,
        "max_favorable_excursion_r": max_favorable_excursion_r,
        "max_adverse_excursion_r": max_adverse_excursion_r,
        "send_mode": None,
        "strategy_name": None,
        "strategy_version": None,
        "regime_state": None,
        "regime_reason": None,
        "regime_bias": None,
        "router_version": None,
        "schema_version": SIGNAL_HISTORY_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
    }
