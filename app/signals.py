# app/signals.py

import os
import time
import logging
import secrets
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import requests
import pytz
from bson import ObjectId

from app.models import new_signal, new_signal_result, new_user_signal
from app.plans import PLAN_FREE, PLAN_PREMIUM
from app.config import is_admin
from app.database import (
    signals_collection,
    user_signals_collection,
    users_collection,
    signal_results_collection,
)

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
MARKET_EVALUATION_VERSION = "v2_market_canonical"

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


def calculate_entry_zone(entry: float, pct: float = 0.0015):
    low = round(entry * (1 - pct), 4)
    high = round(entry * (1 + pct), 4)
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


def estimate_minutes_to_entry(symbol: str, entry_zone: Dict[str, float], timeframes: List[str]) -> Dict[str, int]:
    try:
        current_price = get_current_price(symbol)
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


def recent_duplicate_exists(symbol: str, direction: str, visibility: str) -> bool:
    since = datetime.utcnow() - timedelta(minutes=DEDUP_MINUTES)
    return signals_collection().find_one({
        "symbol": symbol,
        "direction": direction,
        "visibility": visibility,
        "created_at": {"$gte": since},
    }) is not None


def telegram_signal_blocked(symbol: Optional[str] = None) -> bool:
    """
    Bloquea nuevas señales mientras siga vigente una señal en TELEGRAM.
    Debe mirar telegram_valid_until, no valid_until interno.
    """
    now = datetime.utcnow()
    query = {"telegram_valid_until": {"$gt": now}}
    if symbol:
        query["symbol"] = symbol
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

# ======================================================
# GENERAR SEÑALES POR PLAN
# ======================================================

def generate_user_signal_for_plan(base_signal: Dict):
    visibility = base_signal.get("visibility", PLAN_FREE)
    now = datetime.utcnow()

    for user in users_collection().find({}):
        if user.get("banned"):
            continue

        user_id = user.get("user_id")
        user_plan = user.get("plan", PLAN_FREE)
        plan_end = user.get("plan_end")
        admin = is_admin(user_id)

        if plan_end and plan_end < now:
            continue

        if admin or user_plan == visibility:
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
) -> Dict:

    if telegram_signal_blocked(symbol):
        logger.info(f"⏳ Bloqueo activo para {symbol}, no se crea nueva señal")
        return {}

    zone_low, zone_high = calculate_entry_zone(entry_price)
    estimated_minutes = estimate_minutes_to_entry(symbol, {"low": zone_low, "high": zone_high}, timeframes)

    try:
        current_price = get_current_price(symbol)
    except Exception as e:
        logger.warning(f"Fallback current_price en create_base_signal: {e}")
        current_price = entry_price

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
        entry_price=entry_price,
        current_price=current_price,
        atr_pct=atr_pct,
    )
    evaluation_valid_until = now + timedelta(minutes=market_validity_minutes)
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
                "stop_loss": round(entry * 0.992, 4),
                "take_profits": [round(entry * 1.009, 4), round(entry * 1.016, 4)],
            },
            "moderado": {
                "stop_loss": round(entry * 0.9932, 4),
                "take_profits": [round(entry * 1.008, 4), round(entry * 1.014, 4)],
            },
            "agresivo": {
                "stop_loss": round(entry * 0.9942, 4),
                "take_profits": [round(entry * 1.007, 4), round(entry * 1.012, 4)],
            },
        }

    return {
        "conservador": {
            "stop_loss": round(entry * 1.008, 4),
            "take_profits": [round(entry * 0.991, 4), round(entry * 0.984, 4)],
        },
        "moderado": {
            "stop_loss": round(entry * 1.0068, 4),
            "take_profits": [round(entry * 0.992, 4), round(entry * 0.986, 4)],
        },
        "agresivo": {
            "stop_loss": round(entry * 1.0058, 4),
            "take_profits": [round(entry * 0.993, 4), round(entry * 0.988, 4)],
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
            "stop_loss": round(float(src.get("stop_loss", entry)), 4),
            "take_profits": [
                round(float(src.get("take_profits", [entry, entry])[0]), 4),
                round(float(src.get("take_profits", [entry, entry])[1]), 4),
            ],
            "leverage": LEVERAGE_PROFILES[profile_name],
        }

    return new_user_signal(
        user_id=user_id,
        signal_id=str(base_signal["_id"]),
        symbol=base_signal["symbol"],
        direction=direction,
        entry_price=round(entry, 4),
        entry_zone=dict(zip(["low", "high"], calculate_entry_zone(entry))),
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
    visibility = PLAN_PREMIUM if is_admin(user_id) else (user_plan or PLAN_FREE)
    now = datetime.utcnow()

    return list(
        user_signals_collection()
        .find({
            "user_id": user_id,
            "visibility": visibility,
            "telegram_valid_until": {"$gt": now}
        })
        .sort("created_at", -1)
        .limit(MAX_SIGNALS_PER_QUERY)
    )



def get_user_signal_by_signal_id(user_id: int, signal_id: str) -> Optional[Dict]:
    if not signal_id:
        return None
    return user_signals_collection().find_one({
        "user_id": int(user_id),
        "signal_id": str(signal_id),
    })


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

    analysis = {
        **base_signal,
        **user_signal,
        "selected_profile": selected_profile,
        "selected_profile_payload": selected_payload,
        "warnings": warnings,
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


def _tracking_entry_state(direction: str, current_price: Optional[float], zone_low: Optional[float], zone_high: Optional[float], now: datetime, telegram_valid_until: Optional[datetime], evaluation_valid_until: Optional[datetime], final_result: Optional[str]) -> tuple[str, bool, bool]:
    if final_result:
        return "SEÑAL CERRADA", False, False
    if current_price is None or zone_low is None or zone_high is None:
        return "SIN SNAPSHOT DE PRECIO", False, False
    if zone_low <= current_price <= zone_high:
        return "EN ZONA DE ENTRADA", True, True
    direction = str(direction).upper()
    if isinstance(telegram_valid_until, datetime) and telegram_valid_until > now:
        if direction == "LONG":
            if current_price > zone_high:
                return "ENTRADA YA ALEJADA", False, False
            return "AÚN ESPERANDO ENTRADA", False, True
        if current_price < zone_low:
            return "ENTRADA YA ALEJADA", False, False
        return "AÚN ESPERANDO ENTRADA", False, True
    if isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now:
        return "CERRADA EN TELEGRAM / AÚN EVALUANDO", False, False
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
        telegram_valid_until,
        evaluation_valid_until,
        final_result,
    )

    stop_distance_pct = _distance_fraction(direction, entry, stop_loss)
    tp1_distance_pct = _distance_fraction(direction, entry, take_profits[0] if len(take_profits) > 0 else None)
    current_move_pct = _distance_fraction(direction, entry, current_price) if current_price is not None else None
    distance_to_entry_pct = abs(float(current_price) - float(entry)) / float(entry) if current_price is not None and entry else None

    tp1_hit_now = False
    tp2_hit_now = False
    stop_hit_now = False
    if current_price is not None and entry and stop_loss:
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
        recommendation = "El precio actual ya invalidó el SL del perfil elegido. No es una entrada sana."
        state_label = "INVALIDADA EN PRECIO ACTUAL"
        signal_active_for_entry = False
    elif tp2_hit_now:
        recommendation = "El precio actual ya alcanzó o superó TP2. El movimiento está demasiado extendido para perseguirlo."
        state_label = "MUY EXTENDIDA"
        signal_active_for_entry = False
    elif tp1_hit_now:
        recommendation = "El precio actual ya alcanzó TP1. La señal dejó de ser una entrada limpia."
        state_label = "EXTENDIDA"
        signal_active_for_entry = False
    elif in_entry_zone:
        recommendation = "El precio sigue dentro de la zona base. La señal todavía es operable si tu gestión acompaña."
        state_label = "ACTIVA"
    elif signal_active_for_entry:
        recommendation = "La señal aún no entró en zona, pero la ventana de Telegram sigue viva. Espera confirmación en entrada, no persigas precio."
        state_label = "ESPERANDO ENTRADA"
    elif isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now:
        recommendation = "La señal ya salió de Telegram y sigue en evaluación. Úsala solo como referencia."
        state_label = "CERRADA EN TELEGRAM / EVALUANDO"
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
        "in_entry_zone": in_entry_zone,
        "is_operable_now": signal_active_for_entry,
        "entry_state_label": entry_state_label,
        "state_label": state_label,
        "result_label": _result_to_label(final_result),
        "recommendation": recommendation,
        "result": final_result,
        "result_doc": result_doc or {},
        "warnings": warnings,
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


def _evaluate_signal_result(signal_doc: Dict) -> str:
    direction = str(signal_doc.get("direction", "")).upper()
    symbol = signal_doc.get("symbol")

    stop_loss = signal_doc.get("stop_loss")
    take_profits = signal_doc.get("take_profits", [])

    # Fallback por compatibilidad si existe estructura por perfiles
    if (stop_loss is None or not take_profits) and signal_doc.get("profiles"):
        conservador = signal_doc.get("profiles", {}).get("conservador", {})
        stop_loss = conservador.get("stop_loss")
        take_profits = conservador.get("take_profits", [])

    tp1 = take_profits[0] if take_profits else None
    created_at = signal_doc.get("created_at")
    valid_until = _get_evaluation_valid_until(signal_doc)

    if not symbol or not direction or stop_loss is None or tp1 is None or not created_at or not valid_until:
        return "expired"

    try:
        stop_loss = float(stop_loss)
        tp1 = float(tp1)
    except Exception:
        return "expired"

    try:
        klines = _fetch_klines_between(symbol, created_at, valid_until, interval="1m")
    except Exception as e:
        logger.error(f"❌ Error descargando velas para evaluar {symbol}: {e}")
        return "expired"

    for row in klines:
        try:
            high = float(row[2])
            low = float(row[3])
        except Exception:
            continue

        if direction == "LONG":
            if low <= stop_loss and high >= tp1:
                return "lost"
            if high >= tp1:
                return "won"
            if low <= stop_loss:
                return "lost"

        elif direction == "SHORT":
            if high >= stop_loss and low <= tp1:
                return "lost"
            if low <= tp1:
                return "won"
            if high >= stop_loss:
                return "lost"

    return "expired"


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
            result = _evaluate_signal_result(s)
            evaluated_at = datetime.utcnow()

            evaluation_valid_until = _get_evaluation_valid_until(s)

            result_doc = new_signal_result(
                base_signal_id=str(s.get("_id")),
                signal_id=str(s.get("_id")),
                user_id=None,
                symbol=s.get("symbol"),
                direction=s.get("direction"),
                visibility=s.get("visibility"),
                plan=s.get("visibility"),
                score=s.get("score"),
                result=result,
                evaluated_profile="conservador",
                evaluation_scope="base",
                evaluation_scope_version=s.get("evaluation_scope_version", MARKET_EVALUATION_VERSION),
                tp_used=(s.get("take_profits") or [None])[0],
                sl_used=s.get("stop_loss"),
                signal_created_at=s.get("created_at"),
                signal_valid_until=evaluation_valid_until,
                evaluation_valid_until=evaluation_valid_until,
                telegram_valid_until=s.get("telegram_valid_until"),
                market_validity_minutes=s.get("market_validity_minutes", s.get("validity_minutes")),
            )
            result_doc["evaluated_at"] = evaluated_at

            signal_results_collection().insert_one(result_doc)

            signals_collection().update_one(
                {"_id": s["_id"]},
                {
                    "$set": {
                        "evaluated": True,
                        "result": result,
                        "evaluated_at": evaluated_at,
                        "evaluated_profile": "conservador",
                        "evaluation_scope_version": s.get("evaluation_scope_version", MARKET_EVALUATION_VERSION),
                    }
                }
            )

            processed += 1

        except Exception as e:
            logger.error(f"❌ Error evaluando señal base {s.get('symbol')}: {e}", exc_info=True)

    if processed:
        logger.info(f"✅ Señales base evaluadas automáticamente: {processed}")

    return processed
