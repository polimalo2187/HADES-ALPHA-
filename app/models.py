from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# =========================
# CONSTANTES CONFIGURABLES
# =========================
TRIAL_DAYS = 7
USER_SCHEMA_VERSION = 2
REFERRAL_SCHEMA_VERSION = 1
SIGNAL_SCHEMA_VERSION = 1
USER_SIGNAL_SCHEMA_VERSION = 1
SIGNAL_RESULT_SCHEMA_VERSION = 1
WATCHLIST_SCHEMA_VERSION = 1


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
    user["schema_version"] = USER_SCHEMA_VERSION
    return update_timestamp(user)



def is_trial_active(user: Dict[str, Any]) -> bool:
    if user.get("trial_end") is None:
        return False
    return user["trial_end"] >= utcnow()



def is_plan_active(user: Dict[str, Any]) -> bool:
    if user.get("plan_end") is None:
        return False
    return user["plan_end"] >= utcnow()


# =========================
# REFERRAL MODEL
# =========================
def new_referral(
    referrer_id: int,
    referred_id: int,
    activated_plan: str,
    reward_days_applied: int = 7,
) -> Dict[str, Any]:
    now = utcnow()
    return {
        "referrer_id": referrer_id,
        "referred_id": referred_id,
        "activated_plan": activated_plan,
        "activated_at": now,
        "reward_days_applied": reward_days_applied,
        "schema_version": REFERRAL_SCHEMA_VERSION,
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
    result: str,
    evaluated_profile: str,
    evaluation_scope: str,
    evaluation_scope_version: Optional[str],
    tp_used: Optional[float],
    sl_used: Optional[float],
    signal_created_at: Optional[datetime],
    signal_valid_until: Optional[datetime],
    evaluation_valid_until: Optional[datetime],
    telegram_valid_until: Optional[datetime],
    market_validity_minutes: Optional[int],
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
        "result": result,
        "evaluated_at": now,
        "evaluated_profile": evaluated_profile,
        "evaluation_scope": evaluation_scope,
        "evaluation_scope_version": evaluation_scope_version,
        "tp_used": tp_used,
        "sl_used": sl_used,
        "signal_created_at": signal_created_at,
        "signal_valid_until": signal_valid_until,
        "evaluation_valid_until": evaluation_valid_until,
        "telegram_valid_until": telegram_valid_until,
        "market_validity_minutes": market_validity_minutes,
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
