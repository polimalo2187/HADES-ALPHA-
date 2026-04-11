# app/signals.py

import os
import time
import logging
import secrets
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

import requests
import pytz
from bson import ObjectId

from app.models import new_signal, new_signal_result, new_user_signal
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM, normalize_plan
from app.config import is_admin
from app.database import (
    signals_collection,
    user_signals_collection,
    users_collection,
    signal_results_collection,
    signal_history_collection,
)
from app.history_service import upsert_signal_history_record
from app.services.admin_service import is_effectively_banned

logger = logging.getLogger(__name__)

# ======================================================
# CONFIGURACIÓN GLOBAL
# ======================================================
BINANCE_FUTURES_API = os.getenv("BINANCE_FUTURES_API", "https://fapi.binance.com")
MAX_SIGNALS_PER_QUERY = int(os.getenv("MAX_SIGNALS_PER_QUERY", "10"))
BINANCE_MAX_RETRIES = int(os.getenv("BINANCE_MAX_RETRIES", "3"))
BINANCE_RETRY_DELAY = float(os.getenv("BINANCE_RETRY_DELAY", "1.0"))
USER_TIMEZONE = os.getenv("USER_TIMEZONE", "America/Havana")

LEVERAGE_PROFILES = {
    "conservador": "20x-30x",
    "moderado": "30x-40x",
    "agresivo": "40x-50x",
}

TIMEFRAME_TO_MINUTES = {
    "5M": 5,
    "15M": 15,
    "1H": 60,
}

DEDUP_MINUTES = int(os.getenv("DEDUP_MINUTES", "10"))
TELEGRAM_SIGNAL_COOLDOWN_MINUTES = 15
MIN_SIGNAL_VALIDITY_MINUTES = int(os.getenv("MIN_SIGNAL_VALIDITY_MINUTES", "15"))
MAX_SIGNAL_VALIDITY_MINUTES = int(os.getenv("MAX_SIGNAL_VALIDITY_MINUTES", "45"))
MIN_ENTRY_WAIT_MINUTES = int(os.getenv("MIN_ENTRY_WAIT_MINUTES", str(TELEGRAM_SIGNAL_COOLDOWN_MINUTES)))
MAX_ENTRY_WAIT_MINUTES = int(os.getenv("MAX_ENTRY_WAIT_MINUTES", "60"))
ENTRY_WAIT_BUFFER_MINUTES = int(os.getenv("ENTRY_WAIT_BUFFER_MINUTES", "3"))
MARKET_EVALUATION_VERSION = "v4_pending_entry_activation_window"
ENTRY_ZONE_MIN_PCT = float(os.getenv("ENTRY_ZONE_MIN_PCT", "0.0015"))
ENTRY_ZONE_MAX_PCT = float(os.getenv("ENTRY_ZONE_MAX_PCT", "0.0035"))
ENTRY_ZONE_RISK_FRACTION = float(os.getenv("ENTRY_ZONE_RISK_FRACTION", "0.22"))

# ======================================================
# UTILIDADES
# ======================================================

def _base_validity_by_timeframes(timeframes: List[str]) -> float:
    """
    Base canónica de evaluación del mercado.
    No depende del plan para que FREE / PLUS / PREMIUM no alteren
    artificialmente el outcome estadístico de una misma idea.
    """
    minutes = [TIMEFRAME_TO_MINUTES.get(str(tf).upper(), 0) for tf in (timeframes or [])]
    tf_hint = max(minutes) if minutes else 5

    if tf_hint >= 60:
        return 24.0
    if tf_hint >= 15:
        return 21.0
    return 18.0


def calculate_signal_validity(
    timeframes: List[str],
    *,
    visibility: str = "",
    score: Optional[float] = None,
    entry_price: Optional[float] = None,
    current_price: Optional[float] = None,
    atr_pct: Optional[float] = None,
) -> int:
    """
    Validez canónica del mercado.
    Importante: visibility se conserva solo por compatibilidad de firma,
    pero NO altera la evaluación. El resultado de una señal debe depender
    del setup y del mercado, no del plan que la vea.
    """
    validity = float(_base_validity_by_timeframes(timeframes))

    if (
        entry_price is not None
        and current_price is not None
        and float(entry_price) > 0
        and float(current_price) > 0
    ):
        distance_pct = abs(float(current_price) - float(entry_price)) / float(current_price)

        if distance_pct >= 0.006:
            validity += 8
        elif distance_pct >= 0.004:
            validity += 6
        elif distance_pct >= 0.0025:
            validity += 4
        elif distance_pct >= 0.0015:
            validity += 2

    if score is not None:
        try:
            score = float(score)
            if score >= 95:
                validity += 6
            elif score >= 90:
                validity += 5
            elif score >= 82:
                validity += 3
            elif score >= 76:
                validity += 2
        except Exception:
            pass

    if atr_pct is not None:
        try:
            atr_pct = float(atr_pct)
            if atr_pct >= 0.010:
                validity -= 4
            elif atr_pct >= 0.008:
                validity -= 3
            elif atr_pct <= 0.003:
                validity += 3
            elif atr_pct <= 0.004:
                validity += 2
        except Exception:
            pass

    validity = max(
        MIN_SIGNAL_VALIDITY_MINUTES,
        min(MAX_SIGNAL_VALIDITY_MINUTES, int(round(validity))),
    )
    return validity


def _price_round_digits(value: float) -> int:
    try:
        number = abs(float(value))
    except Exception:
        return 4
    if number == 0:
        return 4
    if number >= 1000:
        return 2
    if number >= 100:
        return 3
    if number >= 1:
        return 4
    if number >= 0.1:
        return 5
    if number >= 0.01:
        return 7
    if number >= 0.001:
        return 8
    if number >= 0.0001:
        return 10
    return 12


def _round_price_dynamic(value: float) -> float:
    return round(float(value), _price_round_digits(value))


def calculate_entry_zone(entry: float, stop_loss: Optional[float] = None, pct: Optional[float] = None):
    entry = float(entry)
    if pct is None:
        adaptive_pct = ENTRY_ZONE_MIN_PCT
        try:
            if stop_loss is not None:
                risk_pct = abs(entry - float(stop_loss)) / max(abs(entry), 1e-9)
                adaptive_pct = max(ENTRY_ZONE_MIN_PCT, min(ENTRY_ZONE_MAX_PCT, risk_pct * ENTRY_ZONE_RISK_FRACTION))
        except Exception:
            adaptive_pct = ENTRY_ZONE_MIN_PCT
        pct = adaptive_pct
    pct = max(ENTRY_ZONE_MIN_PCT, min(ENTRY_ZONE_MAX_PCT, float(pct)))
    low = _round_price_dynamic(entry * (1 - pct))
    high = _round_price_dynamic(entry * (1 + pct))
    return low, high


def get_current_price(symbol: str) -> float:
    url = f"{BINANCE_FUTURES_API}/fapi/v1/ticker/price"
    for attempt in range(BINANCE_MAX_RETRIES):
        try:
            r = requests.get(url, params={"symbol": symbol}, timeout=10)
            r.raise_for_status()
            return float(r.json()["price"])
        except Exception:
            if attempt == BINANCE_MAX_RETRIES - 1:
                raise
            time.sleep(BINANCE_RETRY_DELAY)


def estimate_minutes_to_entry(
    symbol: str,
    entry_zone: Dict[str, float],
    timeframes: List[str],
    *,
    current_price: Optional[float] = None,
) -> Dict[str, int]:
    try:
        if current_price is None:
            current_price = get_current_price(symbol)
        current_price = float(current_price)
        zone_mid = (entry_zone["low"] + entry_zone["high"]) / 2

        if entry_zone["low"] <= current_price <= entry_zone["high"]:
            return {"min": 1, "max": 5}

        distance_pct = abs(current_price - zone_mid) / current_price

        if "5M" in timeframes:
            speed = 0.004
            base_tf = 5
        elif "15M" in timeframes:
            speed = 0.0025
            base_tf = 15
        else:
            speed = 0.0015
            base_tf = calculate_signal_validity(timeframes)

        candles_needed = max(1, distance_pct / speed)
        minutes_estimated = candles_needed * base_tf

        return {
            "min": max(1, int(minutes_estimated * 0.6)),
            "max": int(minutes_estimated * 1.4),
        }
    except Exception as e:
        logger.warning(f"Fallback estimate_minutes_to_entry: {e}")
        base = calculate_signal_validity(timeframes)
        return {"min": max(1, int(base * 0.5)), "max": int(base * 1.5)}


def calculate_entry_wait_minutes(estimated_minutes: Dict[str, int]) -> int:
    try:
        estimated_max = int(estimated_minutes.get("max") or 0)
    except Exception:
        estimated_max = 0
    planned = estimated_max + ENTRY_WAIT_BUFFER_MINUTES
    planned = max(MIN_ENTRY_WAIT_MINUTES, planned)
    planned = min(MAX_ENTRY_WAIT_MINUTES, planned)
    return int(planned)


def _tp1_progress_from_entry(direction: str, entry_price: float, tp1: float, current_price: float) -> float:
    denominator = abs(tp1 - entry_price)
    if denominator <= 1e-9:
        return 0.0
    if str(direction).upper() == "LONG":
        moved = max(0.0, current_price - entry_price)
    else:
        moved = max(0.0, entry_price - current_price)
    return round((moved / denominator) * 100.0, 2)


def _r_progress_from_entry(direction: str, entry_price: float, stop_loss: float, current_price: float) -> float:
    denominator = abs(entry_price - stop_loss)
    if denominator <= 1e-9:
        return 0.0
    if str(direction).upper() == "LONG":
        moved = current_price - entry_price
    else:
        moved = entry_price - current_price
    return round(moved / denominator, 4)


def _pending_entry_is_still_actionable(
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profits: List[float],
    current_price: float,
    zone_low: float,
    zone_high: float,
) -> tuple[bool, Dict[str, Optional[float]]]:
    details: Dict[str, Optional[float]] = {
        "tp1_progress_at_send_pct": None,
        "r_progress_at_send": None,
        "zone_distance_pct": None,
        "actionability_reason": None,
    }
    try:
        direction = str(direction).upper()
        entry_price = float(entry_price)
        stop_loss = float(stop_loss)
        current_price = float(current_price)
        zone_low = float(zone_low)
        zone_high = float(zone_high)
    except Exception:
        details["actionability_reason"] = "invalid_payload"
        return False, details

    if current_price <= 0 or entry_price <= 0 or stop_loss <= 0:
        details["actionability_reason"] = "invalid_price"
        return False, details

    if take_profits:
        try:
            details["tp1_progress_at_send_pct"] = _tp1_progress_from_entry(direction, entry_price, float(take_profits[0]), current_price)
        except Exception:
            details["tp1_progress_at_send_pct"] = None
    details["r_progress_at_send"] = _r_progress_from_entry(direction, entry_price, stop_loss, current_price)

    if zone_low <= current_price <= zone_high:
        details["zone_distance_pct"] = 0.0
        details["actionability_reason"] = "already_in_reset_zone"
        return False, details

    if direction == "LONG":
        if current_price <= zone_high:
            details["zone_distance_pct"] = round(max(0.0, (zone_high - current_price) / max(entry_price, 1e-9)) * 100.0, 4)
            details["actionability_reason"] = "pre_reset_not_armed"
            return False, details
        details["zone_distance_pct"] = round(max(0.0, (current_price - zone_high) / max(entry_price, 1e-9)) * 100.0, 4)
    else:
        if current_price >= zone_low:
            details["zone_distance_pct"] = round(max(0.0, (current_price - zone_low) / max(entry_price, 1e-9)) * 100.0, 4)
            details["actionability_reason"] = "pre_reset_not_armed"
            return False, details
        details["zone_distance_pct"] = round(max(0.0, (zone_low - current_price) / max(entry_price, 1e-9)) * 100.0, 4)

    details["actionability_reason"] = "armed_waiting_reset"
    return True, details



def recent_duplicate_exists(symbol: str, direction: str, visibility: str) -> bool:
    since = datetime.utcnow() - timedelta(minutes=DEDUP_MINUTES)
    return signals_collection().find_one({
        "symbol": symbol,
        "direction": direction,
        "visibility": visibility,
        "created_at": {"$gte": since},
    }) is not None


def telegram_signal_blocked(symbol: Optional[str] = None, direction: Optional[str] = None) -> bool:
    """
    Bloquea nuevas señales mientras siga vigente una señal en TELEGRAM.
    Debe mirar telegram_valid_until, no valid_until interno.
    """
    now = datetime.utcnow()
    query = {"telegram_valid_until": {"$gt": now}}
    if symbol:
        query["symbol"] = symbol
    if direction:
        query["direction"] = str(direction).upper()
    return signals_collection().find_one(query, sort=[("telegram_valid_until", -1)]) is not None


def _get_evaluation_valid_until(signal_doc: Dict) -> Optional[datetime]:
    """
    Compatibilidad:
    - señales nuevas usan evaluation_valid_until
    - señales antiguas siguen usando valid_until
    """
    return signal_doc.get("evaluation_valid_until") or signal_doc.get("valid_until")


def _telegram_visibility_until(now: datetime) -> datetime:
    return now + timedelta(minutes=TELEGRAM_SIGNAL_COOLDOWN_MINUTES)



def _visible_tiers_for_plan(plan: Optional[str], *, admin: bool = False) -> List[str]:
    if admin:
        return [PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM]
    plan_value = normalize_plan(plan)
    if plan_value == PLAN_PREMIUM:
        return [PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM]
    if plan_value == PLAN_PLUS:
        return [PLAN_FREE, PLAN_PLUS]
    return [PLAN_FREE]

# ======================================================
# GENERAR SEÑALES POR PLAN
# ======================================================

def generate_user_signal_for_plan(base_signal: Dict):
    visibility = base_signal.get("visibility", PLAN_FREE)
    now = datetime.utcnow()

    for user in users_collection().find({}):
        if is_effectively_banned(user):
            continue

        user_id = user.get("user_id")
        user_plan = user.get("plan", PLAN_FREE)
        plan_end = user.get("plan_end")
        admin = is_admin(user_id)

        if plan_end and plan_end < now:
            continue

        if visibility in _visible_tiers_for_plan(user_plan, admin=admin):
            existing = user_signals_collection().find_one({
                "user_id": user_id,
                "symbol": base_signal["symbol"],
                "telegram_valid_until": {"$gt": now}
            })
            if existing:
                continue

            generate_user_signal(base_signal, user_id)

# ======================================================
# CREAR SEÑAL BASE
# ======================================================

def create_base_signal(
    symbol: str,
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profits: List[float],
    timeframes: List[str],
    visibility: str,
    score: Optional[float] = None,
    components: Optional[List[str]] = None,
    profiles: Optional[Dict[str, Dict]] = None,
    atr_pct: Optional[float] = None,
    normalized_score: Optional[float] = None,
    raw_components: Optional[List[str]] = None,
    normalized_components: Optional[List[str]] = None,
    setup_group: Optional[str] = None,
    score_profile: Optional[str] = None,
    score_calibration: Optional[str] = None,
    send_mode: Optional[str] = None,
    entry_model_price: Optional[float] = None,
    entry_sent_price: Optional[float] = None,
    tp1_progress_at_send_pct: Optional[float] = None,
    r_progress_at_send: Optional[float] = None,
    setup_stage: Optional[str] = None,
    candidate_tier: Optional[str] = None,
    final_tier: Optional[str] = None,
    entry_model: Optional[str] = None,
    current_market_price: Optional[float] = None,
    strategy_name: Optional[str] = None,
    strategy_version: Optional[str] = None,
    regime_state: Optional[str] = None,
    regime_reason: Optional[str] = None,
    regime_bias: Optional[str] = None,
    router_version: Optional[str] = None,
) -> Dict:

    if telegram_signal_blocked(symbol, direction=direction):
        logger.info(f"⏳ Bloqueo activo para {symbol} {direction}, no se crea nueva señal")
        return {}

    zone_low, zone_high = calculate_entry_zone(entry_price, stop_loss=stop_loss)

    if current_market_price is not None:
        try:
            current_price = float(current_market_price)
        except Exception:
            current_price = None
    else:
        current_price = None

    if current_price is None or current_price <= 0:
        try:
            current_price = get_current_price(symbol)
        except Exception as e:
            logger.warning(f"Fallback current_price en create_base_signal: {e}")
            current_price = entry_price

    send_mode_normalized = str(send_mode or "").strip().lower()

    if send_mode_normalized != "market_on_close":
        actionable, pending_details = _pending_entry_is_still_actionable(
            direction=direction,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profits=take_profits,
            current_price=current_price,
            zone_low=zone_low,
            zone_high=zone_high,
        )
        tp1_progress_at_send_pct = pending_details.get("tp1_progress_at_send_pct")
        r_progress_at_send = pending_details.get("r_progress_at_send")
        if not actionable:
            logger.info(
                "⏭️ Señal pending descartada | %s %s | reason=%s | price=%s | entry=%s | zone=%s→%s | tp1_progress_at_send_pct=%s | r_progress_at_send=%s",
                symbol,
                direction,
                pending_details.get("actionability_reason"),
                current_price,
                entry_price,
                zone_low,
                zone_high,
                tp1_progress_at_send_pct,
                r_progress_at_send,
            )
            return {}

    estimated_minutes = estimate_minutes_to_entry(
        symbol,
        {"low": zone_low, "high": zone_high},
        timeframes,
        current_price=current_price,
    )

    signal = new_signal(
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profits=take_profits,
        timeframes=timeframes,
        visibility=visibility,
        leverage=LEVERAGE_PROFILES,
        components=components,
        score=score
    )

    if profiles:
        signal["profiles"] = profiles
    signal["leverage_profiles"] = LEVERAGE_PROFILES

    now = datetime.utcnow()
    market_validity_minutes = calculate_signal_validity(
        timeframes,
        visibility=visibility,
        score=score,
        atr_pct=atr_pct,
    )
    if send_mode_normalized == "market_on_close":
        entry_wait_minutes = 0
        entry_valid_until = now
        evaluation_valid_until = now + timedelta(minutes=market_validity_minutes)
    else:
        entry_wait_minutes = calculate_entry_wait_minutes(estimated_minutes)
        entry_valid_until = now + timedelta(minutes=entry_wait_minutes)
        evaluation_valid_until = entry_valid_until + timedelta(minutes=market_validity_minutes)
    telegram_valid_until = _telegram_visibility_until(now)

    inserted_id = signals_collection().insert_one(signal).inserted_id

    signals_collection().update_one(
        {"_id": inserted_id},
        {"$set": {
            "created_at": now,
            # valid_until se conserva por compatibilidad, pero ahora representa
            # la ventana canónica de evaluación del mercado.
            "valid_until": evaluation_valid_until,
            "evaluation_valid_until": evaluation_valid_until,
            "telegram_valid_until": telegram_valid_until,
            "entry_zone": {"low": zone_low, "high": zone_high},
            "estimated_entry_minutes": estimated_minutes,
            "entry_wait_minutes": entry_wait_minutes,
            "entry_valid_until": entry_valid_until,
            "profiles": profiles if profiles else signal.get("profiles"),
            "leverage_profiles": LEVERAGE_PROFILES,
            "validity_minutes": market_validity_minutes,
            "market_validity_minutes": market_validity_minutes,
            "telegram_visibility_minutes": TELEGRAM_SIGNAL_COOLDOWN_MINUTES,
            "signal_market_price": current_price,
            "signal_atr_pct": atr_pct,
            "normalized_score": normalized_score,
            "raw_components": raw_components or [],
            "normalized_components": normalized_components or [],
            "setup_group": setup_group,
            "score_profile": score_profile,
            "score_calibration": score_calibration,
            "send_mode": send_mode,
            "entry_model_price": entry_model_price,
            "entry_sent_price": entry_sent_price,
            "tp1_progress_at_send_pct": tp1_progress_at_send_pct,
            "r_progress_at_send": r_progress_at_send,
            "setup_stage": setup_stage,
            "candidate_tier": candidate_tier,
            "final_tier": final_tier,
            "entry_model": entry_model,
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,
            "regime_state": regime_state,
            "regime_reason": regime_reason,
            "regime_bias": regime_bias,
            "router_version": router_version,
            "evaluated": False,
            "evaluation_scope_version": MARKET_EVALUATION_VERSION,
        }}
    )

    signal["evaluated"] = False
    signal["_id"] = inserted_id
    signal["created_at"] = now
    signal["valid_until"] = evaluation_valid_until
    signal["evaluation_valid_until"] = evaluation_valid_until
    signal["telegram_valid_until"] = telegram_valid_until
    signal["entry_zone"] = {"low": zone_low, "high": zone_high}
    signal["estimated_entry_minutes"] = estimated_minutes
    signal["entry_wait_minutes"] = entry_wait_minutes
    signal["entry_valid_until"] = entry_valid_until
    signal["validity_minutes"] = market_validity_minutes
    signal["market_validity_minutes"] = market_validity_minutes
    signal["telegram_visibility_minutes"] = TELEGRAM_SIGNAL_COOLDOWN_MINUTES
    signal["signal_market_price"] = current_price
    signal["signal_atr_pct"] = atr_pct
    signal["normalized_score"] = normalized_score
    signal["raw_components"] = raw_components or []
    signal["normalized_components"] = normalized_components or []
    signal["setup_group"] = setup_group
    signal["score_profile"] = score_profile
    signal["score_calibration"] = score_calibration
    signal["send_mode"] = send_mode
    signal["entry_model_price"] = entry_model_price
    signal["entry_sent_price"] = entry_sent_price
    signal["tp1_progress_at_send_pct"] = tp1_progress_at_send_pct
    signal["r_progress_at_send"] = r_progress_at_send
    signal["setup_stage"] = setup_stage
    signal["candidate_tier"] = candidate_tier
    signal["final_tier"] = final_tier
    signal["entry_model"] = entry_model
    signal["strategy_name"] = strategy_name
    signal["strategy_version"] = strategy_version
    signal["regime_state"] = regime_state
    signal["regime_reason"] = regime_reason
    signal["regime_bias"] = regime_bias
    signal["router_version"] = router_version
    signal["evaluation_scope_version"] = MARKET_EVALUATION_VERSION
    signal["schema_version"] = signal.get("schema_version", 1)
    signal["updated_at"] = now

    return signal

# ======================================================
# GENERAR SEÑAL USUARIO
# ======================================================

def _fallback_profiles(direction: str, entry: float) -> Dict[str, Dict]:
    if direction == "LONG":
        return {
            "conservador": {
                "stop_loss": _round_price_dynamic(entry * 0.992),
                "take_profits": [_round_price_dynamic(entry * 1.009), _round_price_dynamic(entry * 1.016)],
            },
            "moderado": {
                "stop_loss": _round_price_dynamic(entry * 0.9932),
                "take_profits": [_round_price_dynamic(entry * 1.008), _round_price_dynamic(entry * 1.014)],
            },
            "agresivo": {
                "stop_loss": _round_price_dynamic(entry * 0.9942),
                "take_profits": [_round_price_dynamic(entry * 1.007), _round_price_dynamic(entry * 1.012)],
            },
        }

    return {
        "conservador": {
            "stop_loss": _round_price_dynamic(entry * 1.008),
            "take_profits": [_round_price_dynamic(entry * 0.991), _round_price_dynamic(entry * 0.984)],
        },
        "moderado": {
            "stop_loss": _round_price_dynamic(entry * 1.0068),
            "take_profits": [_round_price_dynamic(entry * 0.992), _round_price_dynamic(entry * 0.986)],
        },
        "agresivo": {
            "stop_loss": _round_price_dynamic(entry * 1.0058),
            "take_profits": [_round_price_dynamic(entry * 0.993), _round_price_dynamic(entry * 0.988)],
        },
    }


def build_user_signal_document(base_signal: Dict, user_id: int) -> Dict:
    direction = str(base_signal["direction"]).upper()
    entry = float(base_signal["entry_price"])
    profiles = base_signal.get("profiles") or _fallback_profiles(direction, entry)

    normalized_profiles = {}
    for profile_name in ["conservador", "moderado", "agresivo"]:
        src = profiles.get(profile_name, {})
        normalized_profiles[profile_name] = {
            "stop_loss": _round_price_dynamic(float(src.get("stop_loss", entry))),
            "take_profits": [
                _round_price_dynamic(float(src.get("take_profits", [entry, entry])[0])),
                _round_price_dynamic(float(src.get("take_profits", [entry, entry])[1])),
            ],
            "leverage": LEVERAGE_PROFILES[profile_name],
        }

    user_signal = new_user_signal(
        user_id=user_id,
        signal_id=str(base_signal["_id"]),
        symbol=base_signal["symbol"],
        direction=direction,
        entry_price=_round_price_dynamic(entry),
        entry_zone=base_signal.get("entry_zone") or dict(zip(["low", "high"], calculate_entry_zone(entry, stop_loss=normalized_profiles.get("conservador", {}).get("stop_loss")))),
        profiles=normalized_profiles,
        leverage_profiles=LEVERAGE_PROFILES,
        timeframes=base_signal["timeframes"],
        valid_until=base_signal["valid_until"],
        evaluation_valid_until=base_signal.get("evaluation_valid_until", base_signal["valid_until"]),
        telegram_valid_until=base_signal["telegram_valid_until"],
        fingerprint=secrets.token_hex(4),
        visibility=base_signal["visibility"],
        score=base_signal.get("score"),
        normalized_score=base_signal.get("normalized_score"),
        components=base_signal.get("components") or [],
        raw_components=base_signal.get("raw_components") or [],
        normalized_components=base_signal.get("normalized_components") or [],
        setup_group=base_signal.get("setup_group"),
        score_profile=base_signal.get("score_profile"),
        score_calibration=base_signal.get("score_calibration"),
        atr_pct=base_signal.get("signal_atr_pct", base_signal.get("atr_pct")),
        market_validity_minutes=base_signal.get("market_validity_minutes", base_signal.get("validity_minutes")),
        telegram_visibility_minutes=base_signal.get("telegram_visibility_minutes", TELEGRAM_SIGNAL_COOLDOWN_MINUTES),
        evaluation_scope_version=base_signal.get("evaluation_scope_version", MARKET_EVALUATION_VERSION),
    )
    user_signal["send_mode"] = base_signal.get("send_mode")
    user_signal["entry_valid_until"] = base_signal.get("entry_valid_until")
    user_signal["entry_wait_minutes"] = base_signal.get("entry_wait_minutes")
    user_signal["entry_model_price"] = base_signal.get("entry_model_price")
    user_signal["entry_sent_price"] = base_signal.get("entry_sent_price")
    user_signal["tp1_progress_at_send_pct"] = base_signal.get("tp1_progress_at_send_pct")
    user_signal["r_progress_at_send"] = base_signal.get("r_progress_at_send")
    user_signal["setup_stage"] = base_signal.get("setup_stage")
    user_signal["candidate_tier"] = base_signal.get("candidate_tier")
    user_signal["final_tier"] = base_signal.get("final_tier")
    user_signal["entry_model"] = base_signal.get("entry_model")
    user_signal["strategy_name"] = base_signal.get("strategy_name")
    user_signal["strategy_version"] = base_signal.get("strategy_version")
    user_signal["regime_state"] = base_signal.get("regime_state")
    user_signal["regime_reason"] = base_signal.get("regime_reason")
    user_signal["regime_bias"] = base_signal.get("regime_bias")
    user_signal["router_version"] = base_signal.get("router_version")
    return user_signal


def generate_user_signal(base_signal: Dict, user_id: int) -> Dict:
    now = datetime.utcnow()
    existing = user_signals_collection().find_one({
        "user_id": user_id,
        "signal_id": str(base_signal["_id"]),
    })
    if existing:
        return existing

    user_signal = build_user_signal_document(base_signal, user_id)
    user_signals_collection().update_one(
        {"user_id": int(user_id), "signal_id": str(base_signal["_id"])} ,
        {"$setOnInsert": user_signal},
        upsert=True,
    )
    inserted = user_signals_collection().find_one({
        "user_id": int(user_id),
        "signal_id": str(base_signal["_id"]),
    })
    return inserted or user_signal

# ======================================================
# FORMATO TELEGRAM
# ======================================================

def format_user_signal(user_signal: Dict) -> str:
    tz = pytz.timezone(USER_TIMEZONE)
    start = user_signal["created_at"].astimezone(tz).strftime("%H:%M")
    end = user_signal["telegram_valid_until"].astimezone(tz).strftime("%H:%M")

    text = (
        f"📊 NUEVA SEÑAL – FUTUROS USDT\n\n"
        f"🏷️ PLAN: {user_signal['visibility'].upper()}\n\n"
        f"Par: {user_signal['symbol']}\n"
        f"Dirección: {user_signal['direction']}\n"
        f"Entrada base: {user_signal['entry_price']}\n\n"
        f"Margen: ISOLATED\n"
        f"Timeframes: {' / '.join(user_signal['timeframes'])}\n\n"
    )

    for profile in ["conservador", "moderado", "agresivo"]:
        p = user_signal["profiles"][profile]
        text += (
            "━━━━━━━━━━━━━━━━━━\n"
            f"{profile.upper()}\n"
            f"SL: {p['stop_loss']}\n"
            f"TP1: {p['take_profits'][0]}\n"
            f"TP2: {p['take_profits'][1]}\n"
            f"Apalancamiento: {LEVERAGE_PROFILES[profile]}\n\n"
        )

    text += f"⏳ Activa: {start} → {end}\n"
    text += f"🔐 ID: {user_signal['fingerprint']}\n"
    return text

# ======================================================
# OBTENER SEÑALES USUARIO
# ======================================================

def get_latest_base_signal_for_plan(user_id: int, user_plan: Optional[str] = None):
    """
    Esto controla lo que el usuario VE en Telegram.
    Debe usar telegram_valid_until, no valid_until interno.
    """
    admin = is_admin(user_id)
    visible_tiers = _visible_tiers_for_plan(user_plan, admin=admin)
    now = datetime.utcnow()

    return list(
        user_signals_collection()
        .find({
            "user_id": user_id,
            "visibility": {"$in": visible_tiers},
            "telegram_valid_until": {"$gt": now}
        })
        .sort("created_at", -1)
        .limit(MAX_SIGNALS_PER_QUERY)
    )



def _base_signal_from_history(history_doc: Dict, signal_id: str) -> Dict:
    tp1 = history_doc.get("tp1")
    tp2 = history_doc.get("tp2")
    take_profits = [value for value in [tp1, tp2] if value is not None]
    return {
        "_id": signal_id,
        "symbol": history_doc.get("symbol"),
        "direction": history_doc.get("direction"),
        "entry_price": history_doc.get("entry_price"),
        "stop_loss": history_doc.get("stop_loss"),
        "take_profits": take_profits,
        "timeframes": history_doc.get("timeframes") or [],
        "visibility": history_doc.get("visibility"),
        "score": history_doc.get("score"),
        "normalized_score": history_doc.get("normalized_score"),
        "setup_group": history_doc.get("setup_group"),
        "score_profile": history_doc.get("score_profile"),
        "score_calibration": history_doc.get("score_calibration"),
        "created_at": history_doc.get("signal_created_at"),
        "updated_at": history_doc.get("updated_at", history_doc.get("signal_created_at")),
        "valid_until": history_doc.get("signal_valid_until"),
        "evaluation_valid_until": history_doc.get("evaluation_valid_until"),
        "telegram_valid_until": history_doc.get("telegram_valid_until"),
        "market_validity_minutes": history_doc.get("market_validity_minutes"),
        "evaluated": True,
        "result": history_doc.get("result"),
        "evaluated_at": history_doc.get("evaluated_at"),
    }


def _build_synthetic_user_signal(base_signal: Dict, user_id: int, signal_id: str) -> Optional[Dict]:
    if not base_signal:
        return None
    synthetic = build_user_signal_document(base_signal, int(user_id))
    synthetic["signal_id"] = str(signal_id)
    synthetic["created_at"] = base_signal.get("created_at")
    synthetic["updated_at"] = base_signal.get("updated_at", base_signal.get("created_at"))
    synthetic["history_mode"] = True

    result_doc = signal_results_collection().find_one({"base_signal_id": str(signal_id)})
    if result_doc:
        synthetic["result"] = result_doc.get("result")
        synthetic["evaluated"] = True
        synthetic["evaluated_at"] = result_doc.get("evaluated_at")
        synthetic["result_doc"] = result_doc
    else:
        synthetic["evaluated"] = bool(base_signal.get("evaluated"))
        if base_signal.get("result"):
            synthetic["result"] = base_signal.get("result")
            synthetic["evaluated_at"] = base_signal.get("evaluated_at")

    return synthetic


def get_user_signal_by_signal_id(user_id: int, signal_id: str) -> Optional[Dict]:
    if not signal_id:
        return None
    existing = user_signals_collection().find_one({
        "user_id": int(user_id),
        "signal_id": str(signal_id),
    })
    if existing:
        return existing

    base_signal = get_base_signal_by_signal_id(signal_id)
    if not base_signal:
        history_doc = signal_history_collection().find_one({"signal_id": str(signal_id)})
        if history_doc:
            base_signal = _base_signal_from_history(history_doc, signal_id)
    if not base_signal:
        return None
    return _build_synthetic_user_signal(base_signal, user_id, signal_id)


def get_base_signal_by_signal_id(signal_id: str) -> Optional[Dict]:
    if not signal_id:
        return None
    try:
        oid = ObjectId(str(signal_id))
    except Exception:
        return None
    return signals_collection().find_one({"_id": oid})


def _distance_fraction(direction: str, start_price: Optional[float], target_price: Optional[float]) -> Optional[float]:
    try:
        start = float(start_price)
        target = float(target_price)
        if start <= 0:
            return None
        if str(direction).upper() == "LONG":
            return abs(target - start) / start
        return abs(start - target) / start
    except Exception:
        return None


def get_signal_analysis_for_user(user_id: int, signal_id: str, profile_name: str = "moderado") -> Optional[Dict]:
    user_signal = get_user_signal_by_signal_id(user_id, signal_id)
    if not user_signal:
        return None

    base_signal = get_base_signal_by_signal_id(signal_id) or {}
    direction = str(user_signal.get("direction") or base_signal.get("direction") or "").upper()
    profiles = user_signal.get("profiles") or base_signal.get("profiles") or {}
    selected_profile = str(profile_name or "moderado").strip().lower()
    if selected_profile not in {"conservador", "moderado", "agresivo"}:
        selected_profile = "moderado"
    selected_payload = profiles.get(selected_profile) or profiles.get("moderado") or {}
    take_profits = selected_payload.get("take_profits") or []
    entry = user_signal.get("entry_price") or base_signal.get("entry_price")

    warnings = []
    if not base_signal:
        warnings.append("Señal legacy: parte de la metadata avanzada no está disponible en la base actual.")
    if not selected_payload:
        warnings.append("No encontré el perfil operativo solicitado dentro de la señal.")

    now = datetime.utcnow()
    telegram_valid_until = user_signal.get("telegram_valid_until") or base_signal.get("telegram_valid_until")
    entry_valid_until = user_signal.get("entry_valid_until") or base_signal.get("entry_valid_until") or telegram_valid_until
    evaluation_valid_until = _get_evaluation_valid_until(user_signal) or _get_evaluation_valid_until(base_signal)
    telegram_window_open = isinstance(telegram_valid_until, datetime) and telegram_valid_until > now
    entry_window_open = isinstance(entry_valid_until, datetime) and entry_valid_until > now
    evaluation_window_open = isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now

    analysis = {
        **base_signal,
        **user_signal,
        "selected_profile": selected_profile,
        "selected_profile_payload": selected_payload,
        "warnings": warnings,
        "telegram_window_open": telegram_window_open,
        "entry_window_open": entry_window_open,
        "evaluation_window_open": evaluation_window_open,
    }
    analysis.setdefault("normalized_score", base_signal.get("normalized_score"))
    analysis.setdefault("setup_group", base_signal.get("setup_group"))
    analysis.setdefault("score_profile", base_signal.get("score_profile"))
    analysis.setdefault("score_calibration", base_signal.get("score_calibration"))
    analysis.setdefault("components", base_signal.get("components") or user_signal.get("components") or [])
    analysis.setdefault("raw_components", base_signal.get("raw_components") or user_signal.get("raw_components") or [])
    analysis.setdefault("normalized_components", base_signal.get("normalized_components") or user_signal.get("normalized_components") or [])
    analysis.setdefault("atr_pct", base_signal.get("signal_atr_pct", base_signal.get("atr_pct", user_signal.get("atr_pct"))))
    analysis["selected_stop_distance_pct"] = _distance_fraction(direction, entry, selected_payload.get("stop_loss"))
    analysis["selected_tp1_distance_pct"] = _distance_fraction(direction, entry, take_profits[0] if len(take_profits) > 0 else None)
    analysis["selected_tp2_distance_pct"] = _distance_fraction(direction, entry, take_profits[1] if len(take_profits) > 1 else None)
    return analysis




def _result_to_label(result: Optional[str]) -> str:
    mapping = {
        "won": "GANADA",
        "lost": "PERDIDA",
        "expired": "EXPIRADA",
    }
    return mapping.get(str(result or "").lower(), "Aún sin cierre final")


def _observe_live_signal_progress(signal_doc: Dict[str, Any], as_of: datetime) -> Dict[str, Any]:
    direction = str(signal_doc.get("direction") or "").upper()
    symbol = signal_doc.get("symbol")
    stop_loss = signal_doc.get("stop_loss")
    take_profits = list(signal_doc.get("take_profits") or [])
    entry_price = signal_doc.get("entry_price")
    created_at = signal_doc.get("created_at")
    valid_until = _get_evaluation_valid_until(signal_doc)
    telegram_valid_until = signal_doc.get("telegram_valid_until")
    send_mode = str(signal_doc.get("send_mode") or "").strip().lower()

    snapshot: Dict[str, Any] = {
        "entry_touched": False,
        "entry_touched_at": None,
        "tp1_hit": False,
        "tp1_touched_at": None,
        "tp2_hit": False,
        "tp2_touched_at": None,
        "stop_hit": False,
        "stop_touched_at": None,
        "tp1_progress_max_pct": 0.0,
        "max_favorable_excursion_r": 0.0,
        "max_adverse_excursion_r": 0.0,
        "effective_valid_until": valid_until,
        "entry_valid_until": None,
    }

    tp1 = take_profits[0] if len(take_profits) > 0 else None
    tp2 = take_profits[1] if len(take_profits) > 1 else None

    if not symbol or not direction or stop_loss is None or tp1 is None or entry_price is None or not created_at:
        return snapshot

    live_end = as_of
    if isinstance(valid_until, datetime) and valid_until < live_end:
        live_end = valid_until
    if not isinstance(live_end, datetime) or live_end <= created_at:
        return snapshot

    try:
        entry_price = float(entry_price)
        stop_loss = float(stop_loss)
        tp1 = float(tp1)
        tp2 = float(tp2) if tp2 is not None else None
    except Exception:
        return snapshot

    entry_window_end = signal_doc.get("entry_valid_until")
    if not isinstance(entry_window_end, datetime):
        entry_window_end = telegram_valid_until if isinstance(telegram_valid_until, datetime) else valid_until
    if isinstance(entry_window_end, datetime) and isinstance(valid_until, datetime) and entry_window_end > valid_until:
        entry_window_end = valid_until
    snapshot["entry_valid_until"] = entry_window_end

    try:
        market_validity_minutes = int(signal_doc.get("market_validity_minutes") or signal_doc.get("validity_minutes") or 0)
    except Exception:
        market_validity_minutes = 0
    if market_validity_minutes <= 0:
        market_validity_minutes = calculate_signal_validity(signal_doc.get("timeframes") or ["5M"])

    try:
        klines = _fetch_klines_between(symbol, created_at, live_end, interval="1m")
    except Exception:
        return snapshot

    entry_touched = False
    entry_touched_at: Optional[datetime] = None
    effective_valid_until = valid_until if isinstance(valid_until, datetime) else live_end

    if send_mode == "market_on_close":
        entry_touched = True
        entry_touched_at = created_at if isinstance(created_at, datetime) else None
        sent_entry = signal_doc.get("entry_sent_price")
        if sent_entry is not None:
            try:
                entry_price = float(sent_entry)
            except Exception:
                pass

    for row in klines:
        try:
            high = float(row[2])
            low = float(row[3])
        except Exception:
            continue

        if send_mode != "market_on_close" and not entry_touched and _entry_window_allows_new_fill(row, entry_window_end):
            if _pending_entry_touched(signal_doc, direction, entry_price, high, low):
                entry_touched = True
                candle_open_dt, candle_close_dt = _candle_time_bounds(row)
                entry_touched_at = candle_close_dt or candle_open_dt
                if isinstance(entry_touched_at, datetime):
                    effective_valid_until = entry_touched_at + timedelta(minutes=market_validity_minutes)
                    if isinstance(valid_until, datetime) and effective_valid_until > valid_until:
                        effective_valid_until = valid_until

        if not entry_touched:
            continue

        if not _candle_within_window(row, effective_valid_until):
            break

        excursions = _excursions_after_entry_r(direction, entry_price, stop_loss, high, low)
        snapshot["max_favorable_excursion_r"] = max(float(snapshot["max_favorable_excursion_r"] or 0.0), float(excursions.get("favorable_r") or 0.0))
        snapshot["max_adverse_excursion_r"] = max(float(snapshot["max_adverse_excursion_r"] or 0.0), float(excursions.get("adverse_r") or 0.0))
        progress = _tp1_progress_pct(direction, entry_price, tp1, high, low)
        if progress is not None:
            snapshot["tp1_progress_max_pct"] = max(float(snapshot["tp1_progress_max_pct"] or 0.0), float(progress))

        candle_open_dt, candle_close_dt = _candle_time_bounds(row)
        touched_at = candle_close_dt or candle_open_dt

        if direction == "LONG":
            tp1_hit = high >= tp1
            tp2_hit = tp2 is not None and high >= tp2
            sl_hit = low <= stop_loss
        else:
            tp1_hit = low <= tp1
            tp2_hit = tp2 is not None and low <= tp2
            sl_hit = high >= stop_loss

        if sl_hit and (tp2_hit or tp1_hit):
            snapshot["stop_hit"] = True
            snapshot["stop_touched_at"] = touched_at
            break
        if tp2_hit:
            snapshot["tp1_hit"] = True
            snapshot["tp1_touched_at"] = touched_at
            snapshot["tp2_hit"] = True
            snapshot["tp2_touched_at"] = touched_at
            break
        if tp1_hit:
            snapshot["tp1_hit"] = True
            snapshot["tp1_touched_at"] = touched_at
            break
        if sl_hit:
            snapshot["stop_hit"] = True
            snapshot["stop_touched_at"] = touched_at
            break

    snapshot["entry_touched"] = entry_touched
    snapshot["entry_touched_at"] = entry_touched_at
    snapshot["effective_valid_until"] = effective_valid_until
    snapshot["tp1_progress_max_pct"] = round(float(snapshot["tp1_progress_max_pct"] or 0.0), 2)
    snapshot["max_favorable_excursion_r"] = round(float(snapshot["max_favorable_excursion_r"] or 0.0), 4)
    snapshot["max_adverse_excursion_r"] = round(float(snapshot["max_adverse_excursion_r"] or 0.0), 4)
    return snapshot


def _tracking_entry_state(direction: str, current_price: Optional[float], zone_low: Optional[float], zone_high: Optional[float], now: datetime, telegram_valid_until: Optional[datetime], evaluation_valid_until: Optional[datetime], final_result: Optional[str], send_mode: Optional[str] = None) -> tuple[str, bool, bool]:
    if final_result:
        return "SEÑAL CERRADA", False, False
    mode = str(send_mode or "").strip().lower()
    if mode == "market_on_close":
        if isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now:
            return "ACTIVA DESDE ENVÍO", False, True
        return "SEÑAL FINALIZADA", False, False
    if current_price is None or zone_low is None or zone_high is None:
        return "SIN SNAPSHOT DE PRECIO", False, False
    if zone_low <= current_price <= zone_high:
        return "RESET EN ZONA", True, True
    direction = str(direction).upper()
    if isinstance(telegram_valid_until, datetime) and telegram_valid_until > now:
        if direction == "LONG":
            if current_price > zone_high:
                return "ESPERANDO RESET", False, True
            return "RESET YA PASÓ", False, False
        if current_price < zone_low:
            return "ESPERANDO RESET", False, True
        return "RESET YA PASÓ", False, False
    if isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now:
        return "EN EVALUACIÓN", False, False
    return "SEÑAL FINALIZADA", False, False


def get_signal_tracking_for_user(user_id: int, signal_id: str, profile_name: str = "moderado") -> Optional[Dict]:
    user_signal = get_user_signal_by_signal_id(user_id, signal_id)
    if not user_signal:
        return None

    base_signal = get_base_signal_by_signal_id(signal_id) or {}
    direction = str(user_signal.get("direction") or base_signal.get("direction") or "").upper()
    profiles = user_signal.get("profiles") or base_signal.get("profiles") or {}
    selected_profile = str(profile_name or "moderado").strip().lower()
    if selected_profile not in {"conservador", "moderado", "agresivo"}:
        selected_profile = "moderado"
    selected_payload = profiles.get(selected_profile) or profiles.get("moderado") or {}

    entry = user_signal.get("entry_price") or base_signal.get("entry_price")
    entry_zone = user_signal.get("entry_zone") or base_signal.get("entry_zone") or {}
    zone_low = entry_zone.get("low")
    zone_high = entry_zone.get("high")
    stop_loss = selected_payload.get("stop_loss")
    take_profits = selected_payload.get("take_profits") or []
    evaluation_valid_until = _get_evaluation_valid_until(user_signal) or _get_evaluation_valid_until(base_signal)
    telegram_valid_until = user_signal.get("telegram_valid_until") or base_signal.get("telegram_valid_until")
    entry_valid_until = user_signal.get("entry_valid_until") or base_signal.get("entry_valid_until") or telegram_valid_until
    send_mode = user_signal.get("send_mode") or base_signal.get("send_mode")
    result_doc = signal_results_collection().find_one({"base_signal_id": str(signal_id)}, sort=[("evaluated_at", -1)])
    final_result = (result_doc or {}).get("result")

    warnings = []
    current_price = None
    try:
        current_price = get_current_price(str(user_signal.get("symbol") or base_signal.get("symbol") or ""))
    except Exception as exc:
        warnings.append(f"No pude refrescar el precio en vivo: {exc}")

    now = datetime.utcnow()
    entry_state_label, in_entry_zone, signal_active_for_entry = _tracking_entry_state(
        direction,
        current_price,
        zone_low,
        zone_high,
        now,
        entry_valid_until,
        evaluation_valid_until,
        final_result,
        send_mode,
    )

    live_progress: Dict[str, Any] = {}
    if not final_result:
        live_signal_doc = {
            **base_signal,
            **user_signal,
            "direction": direction,
            "entry_price": entry,
            "entry_zone": entry_zone,
            "stop_loss": stop_loss,
            "take_profits": take_profits,
            "created_at": user_signal.get("created_at") or base_signal.get("created_at"),
            "evaluation_valid_until": evaluation_valid_until,
            "entry_valid_until": entry_valid_until,
            "telegram_valid_until": telegram_valid_until,
            "send_mode": send_mode,
        }
        try:
            live_progress = _observe_live_signal_progress(live_signal_doc, now)
        except Exception as exc:
            warnings.append(f"No pude reconstruir el tracking intrabar: {exc}")
            live_progress = {}

    if live_progress.get("entry_touched"):
        entry_state_label = "RESET EJECUTADO"
        in_entry_zone = False
        signal_active_for_entry = False

    stop_distance_pct = _distance_fraction(direction, entry, stop_loss)
    tp1_distance_pct = _distance_fraction(direction, entry, take_profits[0] if len(take_profits) > 0 else None)
    current_move_pct = _distance_fraction(direction, entry, current_price) if current_price is not None else None
    distance_to_entry_pct = abs(float(current_price) - float(entry)) / float(entry) if current_price is not None and entry else None

    tp1_hit_now = False
    tp2_hit_now = False
    stop_hit_now = False
    resolution = str((result_doc or {}).get("resolution") or "").lower()
    if final_result:
        if resolution == "tp2":
            tp1_hit_now = True
            tp2_hit_now = True
        elif resolution == "tp1":
            tp1_hit_now = True
        elif resolution == "sl":
            stop_hit_now = True
    elif live_progress:
        tp1_hit_now = bool(live_progress.get("tp1_hit"))
        tp2_hit_now = bool(live_progress.get("tp2_hit"))
        stop_hit_now = bool(live_progress.get("stop_hit"))
    elif current_price is not None and entry and stop_loss:
        try:
            cp = float(current_price)
            sl = float(stop_loss)
            if direction == "LONG":
                stop_hit_now = cp <= sl
                if len(take_profits) > 0:
                    tp1_hit_now = cp >= float(take_profits[0])
                if len(take_profits) > 1:
                    tp2_hit_now = cp >= float(take_profits[1])
            else:
                stop_hit_now = cp >= sl
                if len(take_profits) > 0:
                    tp1_hit_now = cp <= float(take_profits[0])
                if len(take_profits) > 1:
                    tp2_hit_now = cp <= float(take_profits[1])
        except Exception:
            warnings.append("No pude evaluar correctamente TP/SL con el precio actual.")

    if tp1_distance_pct and current_move_pct is not None:
        progress_to_tp1_pct = round(max(0.0, min(1.5, float(current_move_pct) / float(tp1_distance_pct))) * 100, 2)
    else:
        progress_to_tp1_pct = None
    if live_progress and live_progress.get("entry_touched"):
        progress_to_tp1_pct = live_progress.get("tp1_progress_max_pct")

    telegram_window_open = isinstance(telegram_valid_until, datetime) and telegram_valid_until > now
    entry_window_open = isinstance(entry_valid_until, datetime) and entry_valid_until > now
    evaluation_window_open = isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now

    if final_result == "won":
        recommendation = "La señal ya cerró como ganadora. Úsala solo como referencia de seguimiento."
        state_label = "FINALIZADA"
    elif final_result == "lost":
        recommendation = "La señal ya cerró en pérdida. No busques reentrada sobre esta idea."
        state_label = "FINALIZADA"
    elif final_result == "expired":
        recommendation = "La ventana de mercado terminó sin objetivo claro. Úsala solo como referencia histórica."
        state_label = "FINALIZADA"
    elif stop_hit_now:
        recommendation = "La señal ya tocó el SL del perfil elegido durante el seguimiento. Espera el cierre final para la consolidación del resultado."
        state_label = "SL TOCADO"
        signal_active_for_entry = False
    elif tp2_hit_now:
        recommendation = "La señal ya alcanzó TP2 durante el seguimiento. Espera el cierre final para consolidar el resultado."
        state_label = "MUY EXTENDIDA"
        signal_active_for_entry = False
    elif tp1_hit_now:
        recommendation = "La señal ya alcanzó TP1 durante el seguimiento y sigue en evaluación hasta el cierre final."
        state_label = "EXTENDIDA"
        signal_active_for_entry = False
    elif live_progress.get("entry_touched") and evaluation_window_open:
        recommendation = "El reset ya tocó la entrada y la señal está en evaluación. Ahora importa la continuación posterior al reset."
        state_label = "EN EVALUACIÓN"
        signal_active_for_entry = False
    elif live_progress.get("entry_touched"):
        recommendation = "El reset ya tocó la entrada. La ventana operativa terminó; úsala solo como referencia histórica."
        state_label = "RESET EJECUTADO"
        signal_active_for_entry = False
    elif str(send_mode or "").strip().lower() == "market_on_close":
        recommendation = "La señal se activó al envío. Evalúala desde el precio enviado, no esperes retrace al entry."
        state_label = "ACTIVA DESDE ENVÍO"
    elif in_entry_zone:
        recommendation = "El precio está entrando en la zona prevista de reset. La activación real nace en este retroceso; no persigas fuera de zona."
        state_label = "RESET EN ZONA"
    elif signal_active_for_entry:
        recommendation = "La señal es anticipada. Espera el retroceso al nivel de reset y no entres por impulso antes de que vuelva a la zona."
        state_label = "ESPERANDO RESET"
    elif entry_state_label == "RESET YA PASÓ" and evaluation_window_open:
        recommendation = "La señal sigue visible en la MiniApp, pero el precio ya pasó la zona prevista de reset. No entrar tarde."
        state_label = "RESET YA PASÓ"
    elif entry_state_label == "RESET YA PASÓ":
        recommendation = "El precio ya pasó la zona prevista de reset. No entrar; úsala solo como referencia."
        state_label = "RESET YA PASÓ"
    elif evaluation_window_open:
        recommendation = "La señal sigue en evaluación dentro de la MiniApp. Úsala como referencia operativa."
        state_label = "EN EVALUACIÓN"
    else:
        recommendation = "La señal ya no está operativa. Úsala solo como referencia histórica."
        state_label = "FINALIZADA"

    if not selected_payload:
        warnings.append("No encontré el perfil operativo solicitado dentro de la señal.")
    if current_price is None:
        warnings.append("El seguimiento se muestra con el último snapshot disponible de la señal, sin precio en vivo.")

    return {
        **base_signal,
        **user_signal,
        "selected_profile": selected_profile,
        "selected_profile_payload": selected_payload,
        "stop_loss": stop_loss,
        "take_profits": take_profits,
        "current_price": current_price,
        "entry_zone_low": zone_low,
        "entry_zone_high": zone_high,
        "stop_distance_pct": stop_distance_pct,
        "tp1_distance_pct": tp1_distance_pct,
        "current_move_pct": current_move_pct,
        "distance_to_entry_pct": distance_to_entry_pct,
        "progress_to_tp1_pct": progress_to_tp1_pct,
        "tp1_hit_now": tp1_hit_now,
        "tp2_hit_now": tp2_hit_now,
        "stop_hit_now": stop_hit_now,
        "entry_touched": bool(live_progress.get("entry_touched") or (result_doc or {}).get("entry_touched")),
        "entry_touched_at": live_progress.get("entry_touched_at") or (result_doc or {}).get("entry_touched_at"),
        "tp1_touched_at": live_progress.get("tp1_touched_at"),
        "tp2_touched_at": live_progress.get("tp2_touched_at"),
        "stop_touched_at": live_progress.get("stop_touched_at"),
        "in_entry_zone": in_entry_zone,
        "is_operable_now": signal_active_for_entry,
        "entry_state_label": entry_state_label,
        "state_label": state_label,
        "result_label": _result_to_label(final_result),
        "recommendation": recommendation,
        "result": final_result,
        "result_doc": result_doc or {},
        "warnings": warnings,
        "telegram_window_open": telegram_window_open,
        "entry_window_open": entry_window_open,
        "evaluation_window_open": evaluation_window_open,
    }

def get_recent_user_signals_for_user(user_id: int, limit: int = 10, active_only: bool = False) -> List[Dict]:
    query = {"user_id": int(user_id)}
    if active_only:
        query["telegram_valid_until"] = {"$gt": datetime.utcnow()}
    return list(
        user_signals_collection()
        .find(query)
        .sort("created_at", -1)
        .limit(max(1, int(limit)))
    )

# ======================================================
# EVALUACIÓN AUTOMÁTICA DE SEÑALES (PERFIL CONSERVADOR)
# ======================================================

def _dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _fetch_klines_between(symbol: str, start_dt: datetime, end_dt: datetime, interval: str = "1m") -> List[List]:
    url = f"{BINANCE_FUTURES_API}/fapi/v1/klines"
    start_ms = _dt_to_ms(start_dt)
    end_ms = _dt_to_ms(end_dt)
    all_rows: List[List] = []

    while start_ms < end_ms:
        r = requests.get(
            url,
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": 1000,
            },
            timeout=15,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break

        all_rows.extend(rows)
        last_open_ms = int(rows[-1][0])
        next_start = last_open_ms + 60_000
        if next_start <= start_ms:
            break
        start_ms = next_start

        if len(rows) < 1000:
            break

    return all_rows


def _ms_to_dt(value: Any) -> Optional[datetime]:
    try:
        return datetime.utcfromtimestamp(int(value) / 1000.0)
    except Exception:
        return None


def _candle_time_bounds(row: List[Any]) -> tuple[Optional[datetime], Optional[datetime]]:
    open_dt = _ms_to_dt(row[0]) if row else None
    close_dt: Optional[datetime] = None
    try:
        if row and len(row) > 6 and row[6] is not None:
            close_dt = _ms_to_dt(row[6])
    except Exception:
        close_dt = None
    if open_dt is not None and close_dt is None:
        close_dt = open_dt + timedelta(minutes=1)
    return open_dt, close_dt


def _entry_window_allows_new_fill(row: List[Any], entry_window_end: Optional[datetime]) -> bool:
    if entry_window_end is None:
        return True
    candle_open_dt, candle_close_dt = _candle_time_bounds(row)
    if candle_open_dt is None:
        return False
    if candle_open_dt >= entry_window_end:
        return False
    if candle_close_dt is not None and candle_close_dt > entry_window_end:
        return False
    return True


def _candle_within_window(row: List[Any], window_end: Optional[datetime]) -> bool:
    if window_end is None:
        return True
    candle_open_dt, candle_close_dt = _candle_time_bounds(row)
    if candle_open_dt is None:
        return False
    if candle_open_dt >= window_end:
        return False
    if candle_close_dt is not None and candle_close_dt > window_end:
        return False
    return True


def _entry_zone_touched_in_candle(zone_low: float, zone_high: float, high: float, low: float) -> bool:
    return low <= zone_high and high >= zone_low


def _pending_entry_touched(signal_doc: Dict[str, Any], direction: str, entry_price: float, high: float, low: float) -> bool:
    entry_zone = signal_doc.get("entry_zone") or {}
    zone_low = entry_zone.get("low")
    zone_high = entry_zone.get("high")
    try:
        if zone_low is not None and zone_high is not None:
            zone_low = float(zone_low)
            zone_high = float(zone_high)
            return _entry_zone_touched_in_candle(zone_low, zone_high, high, low)
    except Exception:
        pass
    return _entry_touched_in_candle(direction, entry_price, high, low)


def _entry_touched_in_candle(direction: str, entry_price: float, high: float, low: float) -> bool:
    if direction == "LONG":
        return low <= entry_price
    if direction == "SHORT":
        return high >= entry_price
    return False


def _tp1_progress_pct(direction: str, entry_price: float, tp1: float, high: float, low: float) -> Optional[float]:
    distance = abs(tp1 - entry_price)
    if distance <= 0:
        return None
    if direction == "LONG":
        favorable_move = max(0.0, high - entry_price)
    else:
        favorable_move = max(0.0, entry_price - low)
    return round((favorable_move / distance) * 100.0, 2)


def _excursions_after_entry_r(direction: str, entry_price: float, stop_loss: float, high: float, low: float) -> Dict[str, float]:
    risk_distance = abs(entry_price - stop_loss)
    if risk_distance <= 0:
        return {"favorable_r": 0.0, "adverse_r": 0.0}

    if direction == "LONG":
        favorable_move = max(0.0, high - entry_price)
        adverse_move = max(0.0, entry_price - low)
    else:
        favorable_move = max(0.0, entry_price - low)
        adverse_move = max(0.0, high - entry_price)

    return {
        "favorable_r": round(favorable_move / risk_distance, 4),
        "adverse_r": round(adverse_move / risk_distance, 4),
    }


def _evaluation_observability_payload(
    *,
    entry_touched: bool,
    entry_touched_at: Optional[datetime],
    expiry_type: Optional[str],
    expiry_reason: Optional[str],
    tp1_progress_max_pct: Optional[float],
    max_favorable_excursion_r: Optional[float],
    max_adverse_excursion_r: Optional[float],
) -> Dict[str, Any]:
    return {
        "entry_touched": bool(entry_touched),
        "entry_touched_at": entry_touched_at,
        "expiry_type": expiry_type,
        "expiry_reason": expiry_reason,
        "tp1_progress_max_pct": round(float(tp1_progress_max_pct), 2) if tp1_progress_max_pct is not None else None,
        "max_favorable_excursion_r": round(float(max_favorable_excursion_r), 4) if max_favorable_excursion_r is not None else None,
        "max_adverse_excursion_r": round(float(max_adverse_excursion_r), 4) if max_adverse_excursion_r is not None else None,
    }


def _evaluate_signal_result(signal_doc: Dict) -> Dict[str, Any]:
    direction = str(signal_doc.get("direction", "")).upper()
    symbol = signal_doc.get("symbol")

    stop_loss = signal_doc.get("stop_loss")
    take_profits = list(signal_doc.get("take_profits") or [])
    entry_price = signal_doc.get("entry_price")

    # Fallback por compatibilidad si existe estructura por perfiles
    if (stop_loss is None or not take_profits) and signal_doc.get("profiles"):
        conservador = signal_doc.get("profiles", {}).get("conservador", {})
        stop_loss = conservador.get("stop_loss")
        take_profits = list(conservador.get("take_profits") or [])

    tp1 = take_profits[0] if len(take_profits) > 0 else None
    tp2 = take_profits[1] if len(take_profits) > 1 else None
    created_at = signal_doc.get("created_at")
    valid_until = _get_evaluation_valid_until(signal_doc)
    telegram_valid_until = signal_doc.get("telegram_valid_until")

    expired_no_fill = {
        "result": "expired",
        "resolution": "expired_no_fill",
        "completed": False,
        "tp_used": None,
        "sl_used": stop_loss,
        **_evaluation_observability_payload(
            entry_touched=False,
            entry_touched_at=None,
            expiry_type="no_fill",
            expiry_reason="entry_not_reached",
            tp1_progress_max_pct=0.0,
            max_favorable_excursion_r=0.0,
            max_adverse_excursion_r=0.0,
        ),
    }

    if not symbol or not direction or stop_loss is None or tp1 is None or entry_price is None or not created_at or not valid_until:
        return expired_no_fill

    try:
        entry_price = float(entry_price)
        stop_loss = float(stop_loss)
        tp1 = float(tp1)
        tp2 = float(tp2) if tp2 is not None else None
    except Exception:
        return expired_no_fill

    try:
        klines = _fetch_klines_between(symbol, created_at, valid_until, interval="1m")
    except Exception as e:
        logger.error(f"❌ Error descargando velas para evaluar {symbol}: {e}")
        return expired_no_fill

    send_mode = str(signal_doc.get("send_mode") or "").strip().lower()
    entry_window_end = signal_doc.get("entry_valid_until")
    if not isinstance(entry_window_end, datetime):
        entry_window_end = telegram_valid_until if isinstance(telegram_valid_until, datetime) else valid_until
    if isinstance(entry_window_end, datetime) and isinstance(valid_until, datetime) and entry_window_end > valid_until:
        entry_window_end = valid_until

    try:
        market_validity_minutes = int(signal_doc.get("market_validity_minutes") or signal_doc.get("validity_minutes") or 0)
    except Exception:
        market_validity_minutes = 0
    if market_validity_minutes <= 0:
        market_validity_minutes = calculate_signal_validity(signal_doc.get("timeframes") or ["5M"])

    effective_valid_until = valid_until
    if send_mode == "market_on_close":
        entry_touched = True
        entry_touched_at: Optional[datetime] = created_at if isinstance(created_at, datetime) else None
        sent_entry = signal_doc.get("entry_sent_price")
        if sent_entry is not None:
            try:
                entry_price = float(sent_entry)
            except Exception:
                pass
    else:
        entry_touched = False
        entry_touched_at = None
    tp1_progress_max_pct = 0.0
    max_favorable_excursion_r = 0.0
    max_adverse_excursion_r = 0.0

    def _merge_observability(payload: Dict[str, Any], *, expiry_type: Optional[str], expiry_reason: Optional[str]) -> Dict[str, Any]:
        payload.update(
            _evaluation_observability_payload(
                entry_touched=entry_touched,
                entry_touched_at=entry_touched_at,
                expiry_type=expiry_type,
                expiry_reason=expiry_reason,
                tp1_progress_max_pct=tp1_progress_max_pct,
                max_favorable_excursion_r=max_favorable_excursion_r,
                max_adverse_excursion_r=max_adverse_excursion_r,
            )
        )
        payload["effective_valid_until"] = effective_valid_until
        payload["entry_valid_until"] = entry_window_end
        return payload

    for row in klines:
        try:
            high = float(row[2])
            low = float(row[3])
        except Exception:
            continue

        if send_mode != "market_on_close" and not entry_touched and _entry_window_allows_new_fill(row, entry_window_end):
            if _pending_entry_touched(signal_doc, direction, entry_price, high, low):
                entry_touched = True
                candle_open_dt, candle_close_dt = _candle_time_bounds(row)
                entry_touched_at = candle_close_dt or candle_open_dt
                if isinstance(entry_touched_at, datetime):
                    effective_valid_until = entry_touched_at + timedelta(minutes=market_validity_minutes)
                    if isinstance(valid_until, datetime) and effective_valid_until > valid_until:
                        effective_valid_until = valid_until

        if not entry_touched:
            continue

        if not _candle_within_window(row, effective_valid_until):
            break

        excursions = _excursions_after_entry_r(direction, entry_price, stop_loss, high, low)
        max_favorable_excursion_r = max(max_favorable_excursion_r, float(excursions.get("favorable_r") or 0.0))
        max_adverse_excursion_r = max(max_adverse_excursion_r, float(excursions.get("adverse_r") or 0.0))
        progress = _tp1_progress_pct(direction, entry_price, tp1, high, low)
        if progress is not None:
            tp1_progress_max_pct = max(tp1_progress_max_pct, float(progress))

        if direction == "LONG":
            tp1_hit = high >= tp1
            tp2_hit = tp2 is not None and high >= tp2
            sl_hit = low <= stop_loss
            if sl_hit and (tp2_hit or tp1_hit):
                return _merge_observability(
                    {
                        "result": "lost",
                        "resolution": "sl",
                        "completed": True,
                        "tp_used": None,
                        "sl_used": stop_loss,
                    },
                    expiry_type=None,
                    expiry_reason=None,
                )
            if tp2_hit:
                return _merge_observability(
                    {
                        "result": "won",
                        "resolution": "tp2",
                        "completed": True,
                        "tp_used": tp2,
                        "sl_used": stop_loss,
                    },
                    expiry_type=None,
                    expiry_reason=None,
                )
            if tp1_hit:
                return _merge_observability(
                    {
                        "result": "won",
                        "resolution": "tp1",
                        "completed": True,
                        "tp_used": tp1,
                        "sl_used": stop_loss,
                    },
                    expiry_type=None,
                    expiry_reason=None,
                )
            if sl_hit:
                return _merge_observability(
                    {
                        "result": "lost",
                        "resolution": "sl",
                        "completed": True,
                        "tp_used": None,
                        "sl_used": stop_loss,
                    },
                    expiry_type=None,
                    expiry_reason=None,
                )

        elif direction == "SHORT":
            tp1_hit = low <= tp1
            tp2_hit = tp2 is not None and low <= tp2
            sl_hit = high >= stop_loss
            if sl_hit and (tp2_hit or tp1_hit):
                return _merge_observability(
                    {
                        "result": "lost",
                        "resolution": "sl",
                        "completed": True,
                        "tp_used": None,
                        "sl_used": stop_loss,
                    },
                    expiry_type=None,
                    expiry_reason=None,
                )
            if tp2_hit:
                return _merge_observability(
                    {
                        "result": "won",
                        "resolution": "tp2",
                        "completed": True,
                        "tp_used": tp2,
                        "sl_used": stop_loss,
                    },
                    expiry_type=None,
                    expiry_reason=None,
                )
            if tp1_hit:
                return _merge_observability(
                    {
                        "result": "won",
                        "resolution": "tp1",
                        "completed": True,
                        "tp_used": tp1,
                        "sl_used": stop_loss,
                    },
                    expiry_type=None,
                    expiry_reason=None,
                )
            if sl_hit:
                return _merge_observability(
                    {
                        "result": "lost",
                        "resolution": "sl",
                        "completed": True,
                        "tp_used": None,
                        "sl_used": stop_loss,
                    },
                    expiry_type=None,
                    expiry_reason=None,
                )

    if entry_touched:
        return _merge_observability(
            {
                "result": "expired",
                "resolution": "expired_after_entry",
                "completed": False,
                "tp_used": None,
                "sl_used": stop_loss,
            },
            expiry_type="after_entry_no_followthrough",
            expiry_reason="touched_entry_no_followthrough",
        )

    return expired_no_fill


def _result_r_metrics(signal_doc: Dict, evaluation: Dict[str, Any]) -> Dict[str, Optional[float]]:
    try:
        entry_price = float(signal_doc.get("entry_price"))
        stop_loss = float(signal_doc.get("stop_loss"))
    except Exception:
        return {
            "entry_price": None,
            "risk_pct": None,
            "reward_pct": None,
            "r_multiple": None,
        }

    if entry_price <= 0:
        return {
            "entry_price": None,
            "risk_pct": None,
            "reward_pct": None,
            "r_multiple": None,
        }

    risk_pct = abs(entry_price - stop_loss) / entry_price if stop_loss is not None else None
    tp_used = evaluation.get("tp_used")
    reward_pct = None
    if tp_used is not None:
        try:
            reward_pct = abs(float(tp_used) - entry_price) / entry_price
        except Exception:
            reward_pct = None

    resolution = str(evaluation.get("resolution") or "").lower()
    r_multiple = None
    if resolution == "tp1":
        r_multiple = 1.0
    elif resolution == "tp2":
        r_multiple = 2.0
    elif resolution == "sl":
        r_multiple = -1.0

    return {
        "entry_price": round(entry_price, 8),
        "risk_pct": round(risk_pct, 8) if risk_pct is not None else None,
        "reward_pct": round(reward_pct, 8) if reward_pct is not None else None,
        "r_multiple": r_multiple,
    }


def evaluate_expired_signals(limit: int = 100) -> int:
    """
    Evalúa SOLO señales base para que:
    - estadísticas coincidan con scanner
    - no se dupliquen resultados por usuario
    """
    now = datetime.utcnow()
    pending = list(
        signals_collection()
        .find({
            "$or": [
                {"evaluation_valid_until": {"$lte": now}},
                {
                    "evaluation_valid_until": {"$exists": False},
                    "valid_until": {"$lte": now},
                },
            ],
            "evaluated": {"$ne": True},
        })
        .sort("valid_until", 1)
        .limit(limit)
    )

    processed = 0

    for s in pending:
        try:
            evaluation = _evaluate_signal_result(s)
            result = evaluation.get("result", "expired")
            evaluated_at = datetime.utcnow()

            evaluation_valid_until = evaluation.get("effective_valid_until") or _get_evaluation_valid_until(s)
            entry_valid_until = evaluation.get("entry_valid_until") or s.get("entry_valid_until") or s.get("telegram_valid_until")

            metrics = _result_r_metrics(s, evaluation)
            created_at = s.get("created_at")
            resolution_minutes = None
            if isinstance(created_at, datetime):
                resolution_minutes = round(max((evaluated_at - created_at).total_seconds(), 0.0) / 60.0, 2)

            result_doc = new_signal_result(
                base_signal_id=str(s.get("_id")),
                signal_id=str(s.get("_id")),
                user_id=None,
                symbol=s.get("symbol"),
                direction=s.get("direction"),
                visibility=s.get("visibility"),
                plan=s.get("visibility"),
                score=s.get("score"),
                normalized_score=s.get("normalized_score"),
                setup_group=s.get("setup_group"),
                result=result,
                evaluated_profile="conservador",
                evaluation_scope="base",
                evaluation_scope_version=s.get("evaluation_scope_version", MARKET_EVALUATION_VERSION),
                tp_used=evaluation.get("tp_used"),
                sl_used=evaluation.get("sl_used", s.get("stop_loss")),
                entry_price=metrics.get("entry_price"),
                risk_pct=metrics.get("risk_pct"),
                reward_pct=metrics.get("reward_pct"),
                r_multiple=metrics.get("r_multiple"),
                resolution_minutes=resolution_minutes,
                signal_created_at=created_at,
                signal_valid_until=evaluation_valid_until,
                evaluation_valid_until=evaluation_valid_until,
                telegram_valid_until=s.get("telegram_valid_until"),
                market_validity_minutes=s.get("market_validity_minutes", s.get("validity_minutes")),
                entry_touched=evaluation.get("entry_touched"),
                entry_touched_at=evaluation.get("entry_touched_at"),
                expiry_type=evaluation.get("expiry_type"),
                expiry_reason=evaluation.get("expiry_reason"),
                tp1_progress_max_pct=evaluation.get("tp1_progress_max_pct"),
                max_favorable_excursion_r=evaluation.get("max_favorable_excursion_r"),
                max_adverse_excursion_r=evaluation.get("max_adverse_excursion_r"),
            )
            result_doc["evaluated_at"] = evaluated_at

            insert_result = signal_results_collection().insert_one(result_doc)
            result_doc["resolution"] = evaluation.get("resolution")
            result_doc["completed"] = bool(evaluation.get("completed"))
            result_doc["completed_at_level"] = evaluation.get("resolution")
            result_doc["entry_touched"] = evaluation.get("entry_touched")
            result_doc["entry_touched_at"] = evaluation.get("entry_touched_at")
            result_doc["expiry_type"] = evaluation.get("expiry_type")
            result_doc["expiry_reason"] = evaluation.get("expiry_reason")
            result_doc["tp1_progress_max_pct"] = evaluation.get("tp1_progress_max_pct")
            result_doc["max_favorable_excursion_r"] = evaluation.get("max_favorable_excursion_r")
            result_doc["max_adverse_excursion_r"] = evaluation.get("max_adverse_excursion_r")
            result_doc["_id"] = insert_result.inserted_id

            signal_results_collection().update_one(
                {"_id": result_doc["_id"]},
                {"$set": {
                    "resolution": result_doc["resolution"],
                    "completed": result_doc["completed"],
                    "completed_at_level": result_doc["completed_at_level"],
                    "entry_touched": result_doc.get("entry_touched"),
                    "entry_touched_at": result_doc.get("entry_touched_at"),
                    "expiry_type": result_doc.get("expiry_type"),
                    "expiry_reason": result_doc.get("expiry_reason"),
                    "tp1_progress_max_pct": result_doc.get("tp1_progress_max_pct"),
                    "max_favorable_excursion_r": result_doc.get("max_favorable_excursion_r"),
                    "max_adverse_excursion_r": result_doc.get("max_adverse_excursion_r"),
                }}
            )

            signals_collection().update_one(
                {"_id": s["_id"]},
                {
                    "$set": {
                        "evaluated": True,
                        "result": result,
                        "resolution": evaluation.get("resolution"),
                        "completed": bool(evaluation.get("completed")),
                        "evaluated_at": evaluated_at,
                        "evaluated_profile": "conservador",
                        "evaluation_scope_version": s.get("evaluation_scope_version", MARKET_EVALUATION_VERSION),
                        "entry_valid_until": entry_valid_until,
                        "entry_touched": evaluation.get("entry_touched"),
                        "entry_touched_at": evaluation.get("entry_touched_at"),
                        "expiry_type": evaluation.get("expiry_type"),
                        "expiry_reason": evaluation.get("expiry_reason"),
                        "tp1_progress_max_pct": evaluation.get("tp1_progress_max_pct"),
                        "max_favorable_excursion_r": evaluation.get("max_favorable_excursion_r"),
                        "max_adverse_excursion_r": evaluation.get("max_adverse_excursion_r"),
                    }
                }
            )

            try:
                upsert_signal_history_record(s, result_doc)
            except Exception as history_exc:
                logger.error("❌ Error actualizando histórico verificable %s: %s", s.get("symbol"), history_exc, exc_info=True)

            processed += 1

        except Exception as e:
            logger.error(f"❌ Error evaluando señal base {s.get('symbol')}: {e}", exc_info=True)

    if processed:
        logger.info(f"✅ Señales base evaluadas automáticamente: {processed}")

    return processed
