# app/scanner.py

import os
import re
import math
import inspect
import time
import logging
import asyncio
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
from telegram import Bot

from app.database import signals_collection
from app.realtime_pipeline import enqueue_signal_dispatch
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
from app.signals import create_base_signal
from app.observability import heartbeat
from app import regime_engine, strategy_router
from app import strategy as strategy_engine

logger = logging.getLogger(__name__)

BINANCE_FUTURES_API = "https://fapi.binance.com"

# ==============================
# Scanner runtime config
# ==============================

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "20"))
MIN_QUOTE_VOLUME = int(os.getenv("MIN_QUOTE_VOLUME", "20000000"))
DEDUP_MINUTES = int(os.getenv("DEDUP_MINUTES", "10"))

# Networking / rate limiting
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
REQUEST_MAX_RETRIES = max(1, int(os.getenv("REQUEST_MAX_RETRIES", "4")))
REQUEST_RETRY_BASE_SLEEP = float(os.getenv("REQUEST_RETRY_BASE_SLEEP", "0.6"))

# Concurrency: how many symbols we process in parallel.
SCANNER_SYMBOL_CONCURRENCY = max(1, int(os.getenv("SCANNER_SYMBOL_CONCURRENCY", "24")))

# Force a global inter-request delay (serializes requests); keep false by default.
SCANNER_FORCE_REQUEST_DELAY = str(os.getenv("SCANNER_FORCE_REQUEST_DELAY", "false")).strip().lower() in {"1", "true", "yes", "on"}

# MTF cache: 15M/1H only refresh when a new candle bucket opens.
# 5M remains uncached by default because the live breakout/reset setup is decided there.
SCANNER_ENABLE_HTF_CACHE = str(os.getenv("SCANNER_ENABLE_HTF_CACHE", "true")).strip().lower() in {"1", "true", "yes", "on"}
SCANNER_5M_CACHE_SECONDS = max(0.0, float(os.getenv("SCANNER_5M_CACHE_SECONDS", "0")))
SCANNER_HTF_STALE_GRACE_SECONDS = max(0.0, float(os.getenv("SCANNER_HTF_STALE_GRACE_SECONDS", "900")))
ACTIVE_SYMBOLS_CACHE_SECONDS = max(10.0, float(os.getenv("ACTIVE_SYMBOLS_CACHE_SECONDS", "300")))
SCANNER_BOOTSTRAP_BATCH_SIZE = max(0, int(os.getenv("SCANNER_BOOTSTRAP_BATCH_SIZE", "48")))
SCANNER_15M_REFRESH_BATCH_SIZE = max(0, int(os.getenv("SCANNER_15M_REFRESH_BATCH_SIZE", "20")))
SCANNER_1H_REFRESH_BATCH_SIZE = max(0, int(os.getenv("SCANNER_1H_REFRESH_BATCH_SIZE", "8")))

# Optional: soft QPS limiter (token-bucket). Defaults are conservative to avoid Binance bans.
SCANNER_MAX_REQUESTS_PER_SECOND = float(os.getenv("SCANNER_MAX_REQUESTS_PER_SECOND", "8"))
SCANNER_MAX_BURST = int(os.getenv("SCANNER_MAX_BURST", "16"))

# 5m fetching.
# La estrategia actual es 5M-nativa, así que el default permanece activado.
# Aun así, el valor de entorno debe respetarse para poder perfilar coste y experimentos controlados.
SCANNER_FETCH_5M_ENV = str(os.getenv("SCANNER_FETCH_5M", "true")).strip().lower() in {"1", "true", "yes", "on"}
SCANNER_FETCH_5M = SCANNER_FETCH_5M_ENV

# Kline limits: IMPORTANT — must be large enough for the slowest indicator (EMA200).
# Defaults are intentionally >= 260 to ensure warm-up and to enable the HTF context gate (>= 220).
KLINE_LIMIT_1H = int(os.getenv("SCANNER_KLINE_LIMIT_1H", "260"))
KLINE_LIMIT_15M = int(os.getenv("SCANNER_KLINE_LIMIT_15M", "260"))
KLINE_LIMIT_5M = int(os.getenv("SCANNER_KLINE_LIMIT_5M", "260"))

# Some strategies export these constants; if present, enforce minimums to avoid NaN/indicator-warmup outages.
_STRATEGY_EMA_SLOW = int(getattr(strategy_engine, "EMA_SLOW", 200))
_STRATEGY_LOOKBACK = int(getattr(strategy_engine, "BREAKOUT_LOOKBACK", 24))
_MIN_5M_BARS = max(_STRATEGY_EMA_SLOW + 30, _STRATEGY_LOOKBACK + 90, 260)
_MIN_HTF_BARS = 220

if KLINE_LIMIT_5M < _MIN_5M_BARS:
    logger.warning("⚠️ SCANNER_KLINE_LIMIT_5M=%s es insuficiente para EMA_SLOW=%s; subiendo a %s para evitar NaNs y bloqueo por trend_structure", KLINE_LIMIT_5M, _STRATEGY_EMA_SLOW, _MIN_5M_BARS)
    KLINE_LIMIT_5M = _MIN_5M_BARS

if KLINE_LIMIT_15M < _MIN_HTF_BARS:
    logger.warning("⚠️ SCANNER_KLINE_LIMIT_15M=%s < %s; el filtro HTF de shorts queda deshabilitado. Subiendo a %s.", KLINE_LIMIT_15M, _MIN_HTF_BARS, _MIN_HTF_BARS)
    KLINE_LIMIT_15M = _MIN_HTF_BARS

if KLINE_LIMIT_1H < _MIN_HTF_BARS:
    logger.warning("⚠️ SCANNER_KLINE_LIMIT_1H=%s < %s; el filtro HTF de shorts queda deshabilitado. Subiendo a %s.", KLINE_LIMIT_1H, _MIN_HTF_BARS, _MIN_HTF_BARS)
    KLINE_LIMIT_1H = _MIN_HTF_BARS

# Global inter-request delay (serializes requests across threads). Use only if you really need to.
EFFECTIVE_REQUEST_DELAY = REQUEST_DELAY if SCANNER_FORCE_REQUEST_DELAY else 0.0

# Thresholds basados en raw_score real.
PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "83"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "76"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "69"))

# BTC market-regime guard.
BTC_REGIME_ENABLED = str(os.getenv("BTC_REGIME_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
BTC_REGIME_SYMBOL = str(os.getenv("BTC_REGIME_SYMBOL", "BTCUSDT")).strip().upper() or "BTCUSDT"
BTC_REGIME_APPLY_TO_BTC_SYMBOL = str(os.getenv("BTC_REGIME_APPLY_TO_BTC_SYMBOL", "false")).strip().lower() in {"1", "true", "yes", "on"}
BTC_REGIME_5M_LOOKBACK = max(40, int(os.getenv("BTC_REGIME_5M_LOOKBACK", "120")))
BTC_REGIME_15M_LOOKBACK = max(24, int(os.getenv("BTC_REGIME_15M_LOOKBACK", "96")))
BTC_REGIME_DIRECTIONAL_MOVE_PCT = max(0.15, float(os.getenv("BTC_REGIME_DIRECTIONAL_MOVE_PCT", "0.55")))
BTC_REGIME_FAST_MOVE_PCT = max(0.08, float(os.getenv("BTC_REGIME_FAST_MOVE_PCT", "0.28")))
BTC_REGIME_SHOCK_MOVE_PCT = max(0.20, float(os.getenv("BTC_REGIME_SHOCK_MOVE_PCT", "0.90")))
BTC_REGIME_SHOCK_RANGE_ATR = max(1.0, float(os.getenv("BTC_REGIME_SHOCK_RANGE_ATR", "2.20")))
BTC_REGIME_SHOCK_BODY_ATR = max(0.8, float(os.getenv("BTC_REGIME_SHOCK_BODY_ATR", "1.20")))
BTC_REGIME_COOLDOWN_BARS = max(1, int(os.getenv("BTC_REGIME_COOLDOWN_BARS", "3")))
BTC_REGIME_PREMIUM_SHOCK_SCORE_BUFFER = max(0.0, float(os.getenv("BTC_REGIME_PREMIUM_SHOCK_SCORE_BUFFER", "2.0")))
BTC_REGIME_FAIL_OPEN = str(os.getenv("BTC_REGIME_FAIL_OPEN", "true")).strip().lower() in {"1", "true", "yes", "on"}
BTC_REGIME_SNAPSHOT_TTL_SECONDS = max(15.0, float(os.getenv("BTC_REGIME_SNAPSHOT_TTL_SECONDS", "180")))

# Freshness guard for pending-entry signals.
# If the price already advanced too much from the intended reset entry, the alert must not be sent.
PENDING_ENTRY_MAX_PROGRESS_TO_TP1_PCT = float(os.getenv("PENDING_ENTRY_MAX_PROGRESS_TO_TP1_PCT", "18"))
PENDING_ENTRY_MAX_R_PROGRESS = float(os.getenv("PENDING_ENTRY_MAX_R_PROGRESS", "0.25"))

MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT = float(os.getenv("MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT", "15"))
MAX_CLOSE_MARKET_R_PROGRESS = float(os.getenv("MAX_CLOSE_MARKET_R_PROGRESS", "0.15"))
SCORE_CALIBRATION_VERSION = "v9_breakout_reset_tiered_continuation_guard"
ENTRY_MODEL_NAME = "breakout_reset_retest_pending_v1"
SETUP_STAGE_CLOSED_CONFIRMED = "closed_confirmed"

_PROFILE_CONFIGS = {
    "premium": dict(strategy_engine.PREMIUM_PROFILE),
    "plus": dict(strategy_engine.PLUS_PROFILE),
    "free": dict(strategy_engine.FREE_PROFILE),
}
_PROFILE_SCORE_MAP = {
    round(float(cfg.get("score", 0.0)), 2): name for name, cfg in _PROFILE_CONFIGS.items()
}

_btc_regime_snapshot_lock = threading.Lock()
_btc_regime_snapshot: Dict[str, Any] = {
    "fetched_at_ts": 0.0,
    "state": "unknown",
    "bias": "neutral",
    "allow": True,
    "reason": "uninitialized",
}


def _strategy_call_kwargs(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    *,
    reference_market_price: Optional[float],
    debug_counts: Optional[Dict[str, int]],
) -> Dict:
    kwargs: Dict = {
        "df_1h": df_1h,
        "df_15m": df_15m,
        "df_5m": df_5m,
    }
    try:
        signature = inspect.signature(strategy_engine.mtf_strategy)
        parameters = signature.parameters
    except (TypeError, ValueError):
        return kwargs

    if "reference_market_price" in parameters:
        kwargs["reference_market_price"] = reference_market_price

    if "debug_counts" in parameters:
        kwargs["debug_counts"] = debug_counts if debug_counts is not None else {}

    return kwargs


def _now_ts() -> float:
    return time.time()


def _closed_timeframe_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "close_time" not in df.columns:
        return df.copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    closed = df[df["close_time"] <= now_utc].copy()
    if not closed.empty:
        return closed
    if len(df) > 1:
        return df.iloc[:-1].copy()
    return df.copy()


def _closed_15m_frame(df_15m: pd.DataFrame) -> pd.DataFrame:
    return _closed_timeframe_frame(df_15m)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _simple_atr_series(df: pd.DataFrame, period: int = 14) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window=period, min_periods=period).mean()


def _direction_from_move(move: float, tolerance: float = 1e-9) -> str:
    if move > tolerance:
        return "up"
    if move < -tolerance:
        return "down"
    return "neutral"


def _direction_matches_bias(direction: str, bias: str) -> bool:
    direction_key = str(direction or "").strip().upper()
    bias_key = str(bias or "").strip().lower()
    return (direction_key == "LONG" and bias_key == "up") or (direction_key == "SHORT" and bias_key == "down")


def _classify_btc_regime(df_5m: pd.DataFrame, df_15m: pd.DataFrame) -> Dict[str, Any]:
    closed_5m = _closed_timeframe_frame(df_5m)
    closed_15m = _closed_timeframe_frame(df_15m)
    if len(closed_5m) < 20 or len(closed_15m) < 6:
        return {
            "state": "unknown",
            "bias": "neutral",
            "allow": True,
            "reason": "insufficient_btc_history",
            "block_reason": None,
        }

    atr_5m = _simple_atr_series(closed_5m, period=14)
    if atr_5m.empty or pd.isna(atr_5m.iloc[-1]) or float(atr_5m.iloc[-1]) <= 1e-9:
        return {
            "state": "unknown",
            "bias": "neutral",
            "allow": True,
            "reason": "btc_atr_unavailable",
            "block_reason": None,
        }

    last_5m = closed_5m.iloc[-1]
    prev_5m = closed_5m.iloc[-2]
    atr_now = float(atr_5m.iloc[-1])
    last_range_atr = abs(float(last_5m["high"]) - float(last_5m["low"])) / atr_now
    last_body_atr = abs(float(last_5m["close"]) - float(last_5m["open"])) / atr_now
    last_move_pct = abs((float(last_5m["close"]) / max(float(prev_5m["close"]), 1e-9) - 1.0) * 100.0)

    recent_window = closed_5m.tail(max(BTC_REGIME_COOLDOWN_BARS, 3)).copy()
    recent_atr = atr_5m.tail(len(recent_window)).reset_index(drop=True)
    recent_window = recent_window.reset_index(drop=True)
    recent_ranges_atr = (recent_window["high"] - recent_window["low"]).abs() / recent_atr
    recent_bodies_atr = (recent_window["close"] - recent_window["open"]).abs() / recent_atr
    recent_shock = bool(
        (recent_ranges_atr >= BTC_REGIME_SHOCK_RANGE_ATR).any()
        or (recent_bodies_atr >= BTC_REGIME_SHOCK_BODY_ATR).any()
    )

    move_15m_pct = 0.0
    if len(closed_15m) >= 4:
        anchor_close = float(closed_15m.iloc[-4]["close"])
        move_15m_pct = (float(closed_15m.iloc[-1]["close"]) / max(anchor_close, 1e-9) - 1.0) * 100.0
    elif len(closed_15m) >= 2:
        anchor_close = float(closed_15m.iloc[-2]["close"])
        move_15m_pct = (float(closed_15m.iloc[-1]["close"]) / max(anchor_close, 1e-9) - 1.0) * 100.0

    move_fast_pct = (float(last_5m["close"]) / max(float(recent_window.iloc[0]["open"]), 1e-9) - 1.0) * 100.0
    directional_bias = _direction_from_move(move_15m_pct)
    if directional_bias == "neutral":
        directional_bias = _direction_from_move(move_fast_pct)
    if directional_bias == "neutral":
        directional_bias = _direction_from_move(float(last_5m["close"]) - float(last_5m["open"]))

    shock_now = bool(
        last_range_atr >= BTC_REGIME_SHOCK_RANGE_ATR
        or last_body_atr >= BTC_REGIME_SHOCK_BODY_ATR
        or last_move_pct >= BTC_REGIME_SHOCK_MOVE_PCT
    )

    state = "normal"
    reason = "btc_regime_normal"
    block_reason = None

    if shock_now:
        state = "vol_shock"
        reason = "btc_regime_vol_shock"
        block_reason = "btc_regime_vol_shock"
    elif recent_shock:
        state = "cooldown"
        reason = "btc_regime_cooldown"
        block_reason = "btc_regime_cooldown"
    elif abs(move_15m_pct) >= BTC_REGIME_DIRECTIONAL_MOVE_PCT and abs(move_fast_pct) >= BTC_REGIME_FAST_MOVE_PCT:
        if directional_bias == "up":
            state = "trend_up"
            reason = "btc_regime_trend_up"
        elif directional_bias == "down":
            state = "trend_down"
            reason = "btc_regime_trend_down"

    return {
        "state": state,
        "bias": directional_bias,
        "allow": state in {"normal", "trend_up", "trend_down", "unknown"},
        "reason": reason,
        "block_reason": block_reason,
        "symbol": BTC_REGIME_SYMBOL,
        "metrics": {
            "move_15m_pct": round(move_15m_pct, 4),
            "move_fast_pct": round(move_fast_pct, 4),
            "last_move_pct": round(last_move_pct, 4),
            "last_range_atr": round(last_range_atr, 4),
            "last_body_atr": round(last_body_atr, 4),
        },
    }


def _snapshot_btc_regime() -> Dict[str, Any]:
    with _btc_regime_snapshot_lock:
        return dict(_btc_regime_snapshot)


def _store_btc_regime(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(snapshot or {})
    payload.setdefault("fetched_at_ts", _now_ts())
    with _btc_regime_snapshot_lock:
        _btc_regime_snapshot.clear()
        _btc_regime_snapshot.update(payload)
        return dict(_btc_regime_snapshot)


def _timeframe_bucket_id(interval: str, now_ts: Optional[float] = None) -> Optional[int]:
    interval_key = str(interval or "").strip().lower()
    seconds = {
        "5m": 5 * 60,
        "15m": 15 * 60,
        "1h": 60 * 60,
    }.get(interval_key)
    if seconds is None:
        return None
    current_ts = _now_ts() if now_ts is None else float(now_ts)
    return int(current_ts // seconds)


def _cache_enabled_for_interval(interval: str) -> bool:
    interval_key = str(interval or "").strip().lower()
    if interval_key in {"15m", "1h"}:
        return SCANNER_ENABLE_HTF_CACHE
    if interval_key == "5m":
        return SCANNER_5M_CACHE_SECONDS > 0.0
    return False


def _infer_setup_group(signal: Dict) -> str:
    explicit = str(signal.get("setup_group") or signal.get("score_profile") or signal.get("candidate_tier") or "").strip().lower()
    if explicit in _PROFILE_CONFIGS:
        return explicit
    try:
        score = round(float(signal.get("score", signal.get("raw_score", 0.0))), 2)
    except Exception:
        score = 0.0
    return _PROFILE_SCORE_MAP.get(score, "free")


def _profile_config_for_signal(signal: Dict) -> Dict:
    return dict(_PROFILE_CONFIGS.get(_infer_setup_group(signal), _PROFILE_CONFIGS["free"]))


def _safe_rr(entry_price: float, stop_loss: float, target_price: float) -> float:
    risk = abs(stop_loss - entry_price)
    if risk <= 1e-9:
        return 0.0
    return abs(target_price - entry_price) / risk


def _progress_from_model_to_tp1_pct(model_entry: float, model_tp1: float, sent_entry: float, direction: str) -> float:
    denominator = abs(model_tp1 - model_entry)
    if denominator <= 1e-9:
        return 0.0
    if direction == "LONG":
        moved = max(0.0, sent_entry - model_entry)
    else:
        moved = max(0.0, model_entry - sent_entry)
    return round((moved / denominator) * 100.0, 2)


def _r_progress_from_model_entry(model_entry: float, stop_loss: float, sent_entry: float, direction: str) -> float:
    denominator = abs(model_entry - stop_loss)
    if denominator <= 1e-9:
        return 0.0
    if direction == "LONG":
        moved = sent_entry - model_entry
    else:
        moved = model_entry - sent_entry
    return round(moved / denominator, 4)


def _reprice_profiles(model_profiles: Dict[str, Dict], model_entry: float, stop_loss: float, market_entry: float, direction: str) -> Dict[str, Dict]:
    repriced: Dict[str, Dict] = {}
    market_risk = abs(stop_loss - market_entry)
    for profile_name, payload in (model_profiles or {}).items():
        take_profits = list(payload.get("take_profits") or [])
        tp1 = take_profits[0] if len(take_profits) > 0 else None
        tp2 = take_profits[1] if len(take_profits) > 1 else None
        tp1_rr = _safe_rr(model_entry, stop_loss, float(tp1)) if tp1 is not None else 0.0
        tp2_rr = _safe_rr(model_entry, stop_loss, float(tp2)) if tp2 is not None else 0.0
        if direction == "LONG":
            new_tps = [
                round(market_entry + (market_risk * tp1_rr), 8),
                round(market_entry + (market_risk * tp2_rr), 8),
            ]
        else:
            new_tps = [
                round(market_entry - (market_risk * tp1_rr), 8),
                round(market_entry - (market_risk * tp2_rr), 8),
            ]
        repriced[profile_name] = {
            "stop_loss": round(float(payload.get("stop_loss", stop_loss)), 8),
            "take_profits": new_tps,
            "leverage": payload.get("leverage"),
        }
    return repriced


def _coerce_reference_price(current_price) -> float:
    try:
        if isinstance(current_price, pd.DataFrame):
            if current_price.empty or "close" not in current_price.columns:
                return 0.0
            return float(current_price.iloc[-1]["close"])
        if isinstance(current_price, pd.Series):
            if "close" in current_price.index:
                return float(current_price["close"])
            return 0.0
        return float(current_price or 0.0)
    except Exception:
        return 0.0


def _apply_close_market_execution(result: Dict, current_price: float) -> Optional[Dict]:
    if not result:
        return None

    enriched = dict(result)
    if str(enriched.get("send_mode") or "").strip().lower() == "market_on_close":
        # La estrategia actual ya viene emitida a mercado. Evitar doble repricing/discard.
        return enriched

    direction = str(enriched.get("direction") or "").upper().strip()
    if direction not in {"LONG", "SHORT"}:
        return None

    model_entry = float(enriched.get("entry_price") or 0.0)
    stop_loss = float(enriched.get("stop_loss") or 0.0)
    market_entry = _coerce_reference_price(current_price)
    if market_entry <= 0 or stop_loss <= 0 or model_entry <= 0:
        return None

    if direction == "LONG" and market_entry <= stop_loss:
        return None
    if direction == "SHORT" and market_entry >= stop_loss:
        return None

    setup_group = _infer_setup_group(enriched)
    profile_cfg = _profile_config_for_signal(enriched)
    risk_pct = abs(stop_loss - market_entry) / max(market_entry, 1e-9)
    if risk_pct > float(profile_cfg.get("max_risk_pct", 1.0)):
        return None

    model_profiles = dict(enriched.get("profiles") or {})
    conservative = model_profiles.get("conservador") or {}
    model_take_profits = list(conservative.get("take_profits") or enriched.get("take_profits") or [])
    if not model_take_profits:
        return None
    model_tp1 = float(model_take_profits[0])
    progress_pct = _progress_from_model_to_tp1_pct(model_entry, model_tp1, market_entry, direction)
    r_progress = _r_progress_from_model_entry(model_entry, stop_loss, market_entry, direction)
    if progress_pct > MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT or r_progress > MAX_CLOSE_MARKET_R_PROGRESS:
        return None

    repriced_profiles = _reprice_profiles(model_profiles, model_entry, stop_loss, market_entry, direction)
    conservative_profile = repriced_profiles.get("conservador") or {}
    take_profits = list(conservative_profile.get("take_profits") or [])
    if not take_profits:
        return None
    conservative_tp1_rr = _safe_rr(market_entry, stop_loss, float(take_profits[0]))
    if conservative_tp1_rr < float(profile_cfg.get("min_rr", 0.0)):
        return None

    enriched.update({
        "entry_price": round(market_entry, 8),
        "take_profits": take_profits,
        "profiles": repriced_profiles,
        "raw_score": float(enriched.get("raw_score", enriched.get("score", 0.0))),
        "normalized_score": float(enriched.get("normalized_score", enriched.get("score", 0.0))),
        "components": list(enriched.get("components") or []),
        "raw_components": list(enriched.get("raw_components") or enriched.get("components") or []),
        "normalized_components": list(enriched.get("normalized_components") or enriched.get("components") or []),
        "setup_group": setup_group,
        "score_profile": setup_group,
        "score_calibration": SCORE_CALIBRATION_VERSION,
        "send_mode": "market_on_close",
        "entry_model_price": round(model_entry, 8),
        "entry_sent_price": round(market_entry, 8),
        "tp1_progress_at_send_pct": progress_pct,
        "r_progress_at_send": r_progress,
        "setup_stage": SETUP_STAGE_CLOSED_CONFIRMED,
        "candidate_tier": setup_group,
        "final_tier": setup_group,
        "entry_model": ENTRY_MODEL_NAME,
    })
    return enriched


def build_symbol_candidate(symbol: str, df_1h: pd.DataFrame, df_15m: pd.DataFrame, df_5m: Optional[pd.DataFrame], *, debug_counts: Optional[Dict[str, int]] = None) -> Optional[Dict]:
    closed_1h = _closed_timeframe_frame(df_1h)
    closed_15m = _closed_timeframe_frame(df_15m)
    reference_price = None
    try:
        if df_5m is not None and len(df_5m) > 0:
            reference_price = float(df_5m.iloc[-1]["close"])
        elif not closed_15m.empty:
            reference_price = float(closed_15m.iloc[-1]["close"])
    except Exception:
        reference_price = None

    strategy_kwargs = _strategy_call_kwargs(
        df_1h=closed_1h,
        df_15m=closed_15m,
        df_5m=df_5m,
        reference_market_price=reference_price,
        debug_counts=debug_counts,
    )
    result = strategy_engine.mtf_strategy(**strategy_kwargs)
    if not result:
        return None
    price_for_candidate = reference_price
    if price_for_candidate is None:
        try:
            price_for_candidate = float(result.get("entry_sent_price") or result.get("entry_price") or 0.0)
        except Exception:
            price_for_candidate = 0.0
    return _build_candidate(symbol, result, price_for_candidate)


def route_symbol_candidate(
    symbol: str,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    *,
    market_regime: Optional[Dict[str, Any]],
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Dict]:
    closed_1h = _closed_timeframe_frame(df_1h)
    closed_15m = _closed_timeframe_frame(df_15m)
    reference_price = None
    try:
        if df_5m is not None and len(df_5m) > 0:
            reference_price = float(df_5m.iloc[-1]["close"])
        elif not closed_15m.empty:
            reference_price = float(closed_15m.iloc[-1]["close"])
    except Exception:
        reference_price = None

    result = strategy_router.route_candidate(
        symbol=symbol,
        df_1h=closed_1h,
        df_15m=closed_15m,
        df_5m=df_5m,
        market_regime=market_regime,
        reference_market_price=reference_price,
        debug_counts=debug_counts,
    )
    if not result:
        return None
    price_for_candidate = reference_price
    if price_for_candidate is None:
        try:
            price_for_candidate = float(result.get("entry_sent_price") or result.get("entry_price") or 0.0)
        except Exception:
            price_for_candidate = 0.0
    return _build_candidate(symbol, result, price_for_candidate)


class ActiveSymbolsCache:
    def __init__(self):
        self._symbols: List[str] = []
        self._fetched_at: float = 0.0
        self._lock = threading.Lock()
        self._stats = {"hits": 0, "misses": 0, "stores": 0, "stale_hits": 0}

    def clear(self) -> None:
        with self._lock:
            self._symbols = []
            self._fetched_at = 0.0
            for key in list(self._stats.keys()):
                self._stats[key] = 0

    def get(self, *, allow_stale: bool = False) -> Optional[List[str]]:
        now_ts = _now_ts()
        with self._lock:
            if not self._symbols:
                self._stats["misses"] += 1
                return None
            age = now_ts - self._fetched_at
            if age <= ACTIVE_SYMBOLS_CACHE_SECONDS:
                self._stats["hits"] += 1
                return list(self._symbols)
            if allow_stale:
                self._stats["stale_hits"] += 1
                return list(self._symbols)
            self._stats["misses"] += 1
            return None

    def set(self, symbols: List[str]) -> None:
        with self._lock:
            self._symbols = list(symbols)
            self._fetched_at = _now_ts()
            self._stats["stores"] += 1

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            snap = dict(self._stats)
            snap["count"] = len(self._symbols)
            return snap


class BinanceRateLimitedError(RuntimeError):
    def __init__(self, status_code: int, body: str, *, banned_until_ts: Optional[float] = None, retry_after_seconds: Optional[float] = None):
        self.status_code = int(status_code)
        self.body = str(body or "")
        self.banned_until_ts = banned_until_ts
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"binance_rate_limited status={self.status_code} body={self.body[:120]}")


class BinanceCooldownActiveError(RuntimeError):
    def __init__(self, remaining_seconds: float, reason: str = ""):
        self.remaining_seconds = max(0.0, float(remaining_seconds))
        self.reason = str(reason or "")
        super().__init__(f"binance_cooldown_active remaining={self.remaining_seconds:.1f}s reason={self.reason[:120]}")


class BinanceRequestGate:
    def __init__(self):
        self._blocked_until = 0.0
        self._reason = ""
        self._lock = threading.Lock()

    def clear(self) -> None:
        with self._lock:
            self._blocked_until = 0.0
            self._reason = ""

    def activate_until(self, until_ts: float, reason: str = "") -> None:
        blocked_until = max(0.0, float(until_ts))
        with self._lock:
            if blocked_until > self._blocked_until:
                self._blocked_until = blocked_until
                self._reason = str(reason or self._reason)

    def activate_for(self, seconds: float, reason: str = "") -> None:
        self.activate_until(_now_ts() + max(0.0, float(seconds)), reason=reason)

    def remaining_seconds(self) -> float:
        with self._lock:
            return max(0.0, self._blocked_until - _now_ts())

    def reason(self) -> str:
        with self._lock:
            return self._reason

    def ensure_ready(self) -> None:
        remaining = self.remaining_seconds()
        if remaining > 0.0:
            raise BinanceCooldownActiveError(remaining, self.reason())


def _parse_binance_ban_until(body: str) -> Optional[float]:
    match = re.search(r"banned until\s+(\d{10,})", str(body or ""))
    if not match:
        return None
    raw_value = match.group(1)
    value = float(raw_value)
    if value > 10_000_000_000:
        value = value / 1000.0
    return value


def _bootstrap_total_cycles(symbol_count: int) -> int:
    if SCANNER_BOOTSTRAP_BATCH_SIZE <= 0 or symbol_count <= 0:
        return 0
    return int(math.ceil(float(symbol_count) / float(SCANNER_BOOTSTRAP_BATCH_SIZE)))


def _select_symbols_for_cycle(symbols: List[str], cycle_number: int) -> Tuple[List[str], bool]:
    symbol_count = len(symbols)
    total_cycles = _bootstrap_total_cycles(symbol_count)
    if total_cycles <= 0 or cycle_number >= total_cycles:
        return list(symbols), False
    start = cycle_number * SCANNER_BOOTSTRAP_BATCH_SIZE
    end = min(symbol_count, start + SCANNER_BOOTSTRAP_BATCH_SIZE)
    return list(symbols[start:end]), True


def _rotating_refresh_subset(symbols: List[str], batch_size: int, cycle_number: int) -> Set[str]:
    symbol_count = len(symbols)
    if batch_size <= 0 or symbol_count <= 0:
        return set()
    if batch_size >= symbol_count:
        return set(symbols)
    start = (cycle_number * batch_size) % symbol_count
    selected: List[str] = []
    for offset in range(batch_size):
        selected.append(symbols[(start + offset) % symbol_count])
    return set(selected)


class TimeframeKlineCache:
    def __init__(self):
        self._entries: Dict[Tuple[str, str, int], Dict] = {}
        self._lock = threading.Lock()
        self._stats = {
            "hits": 0,
            "misses": 0,
            "stores": 0,
            "bypasses": 0,
            "evictions": 0,
            "stale_hits": 0,
        }

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            for key in list(self._stats.keys()):
                self._stats[key] = 0

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            snap = dict(self._stats)
            snap["entries"] = len(self._entries)
            return snap

    def delta(self, before: Dict[str, int]) -> Dict[str, int]:
        after = self.snapshot()
        keys = set(after) | set(before or {})
        return {key: int(after.get(key, 0)) - int((before or {}).get(key, 0)) for key in keys}

    def get(self, symbol: str, interval: str, limit: int, *, allow_stale: bool = False, stale_grace_seconds: float = 0.0) -> Optional[pd.DataFrame]:
        interval_key = str(interval or "").strip().lower()
        if not _cache_enabled_for_interval(interval_key):
            with self._lock:
                self._stats["bypasses"] += 1
            return None
        key = (str(symbol).upper(), interval_key, int(limit))
        now_ts = _now_ts()
        with self._lock:
            entry = self._entries.get(key)
            if not entry:
                self._stats["misses"] += 1
                return None
            if interval_key in {"15m", "1h"}:
                current_bucket = _timeframe_bucket_id(interval_key, now_ts=now_ts)
                if entry.get("bucket_id") == current_bucket:
                    self._stats["hits"] += 1
                    return entry["df"].copy(deep=False)
                age = now_ts - float(entry.get("fetched_at", now_ts))
                if allow_stale and age <= max(0.0, float(stale_grace_seconds)):
                    self._stats["stale_hits"] += 1
                    return entry["df"].copy(deep=False)
                self._entries.pop(key, None)
                self._stats["evictions"] += 1
                self._stats["misses"] += 1
                return None
            max_age = float(entry.get("max_age_seconds", 0.0) or 0.0)
            age = now_ts - float(entry.get("fetched_at", now_ts))
            if age <= max_age:
                self._stats["hits"] += 1
                return entry["df"].copy(deep=False)
            self._entries.pop(key, None)
            self._stats["evictions"] += 1
            self._stats["misses"] += 1
            return None

    def set(self, symbol: str, interval: str, limit: int, df: pd.DataFrame) -> None:
        interval_key = str(interval or "").strip().lower()
        if not _cache_enabled_for_interval(interval_key):
            return
        now_ts = _now_ts()
        entry = {
            "df": df.copy(deep=False),
            "fetched_at": now_ts,
            "bucket_id": _timeframe_bucket_id(interval_key, now_ts=now_ts),
            "max_age_seconds": SCANNER_5M_CACHE_SECONDS if interval_key == "5m" else 0.0,
        }
        key = (str(symbol).upper(), interval_key, int(limit))
        with self._lock:
            self._entries[key] = entry
            self._stats["stores"] += 1


class RateLimiter:
    def __init__(self, delay: float):
        self.delay = max(0.0, float(delay))
        self.last_request = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        if self.delay <= 0:
            return
        with self._lock:
            elapsed = time.time() - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_request = time.time()


rate_limiter = RateLimiter(EFFECTIVE_REQUEST_DELAY)
_kline_cache = TimeframeKlineCache()
_active_symbols_cache = ActiveSymbolsCache()
_request_gate = BinanceRequestGate()


class TokenBucket:
    """Thread-safe token bucket to cap request rate without serializing all work.

    This avoids the previous "delay=0" + high concurrency situation that can silently
    trigger upstream rate-limits and starve the scanner.
    """

    def __init__(self, rate: float, capacity: int):
        self.rate = max(0.1, float(rate))
        self.capacity = max(1, int(capacity))
        self.tokens = float(self.capacity)
        self.updated_at = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        need = max(0.0, float(tokens))
        if need <= 0:
            return
        while True:
            with self._lock:
                now = time.time()
                elapsed = now - self.updated_at
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + (elapsed * self.rate))
                    self.updated_at = now
                if self.tokens >= need:
                    self.tokens -= need
                    return
                missing = need - self.tokens
                wait_s = missing / self.rate if self.rate > 0 else 0.25
            time.sleep(max(0.01, min(wait_s, 1.0)))


_token_bucket = TokenBucket(SCANNER_MAX_REQUESTS_PER_SECOND, SCANNER_MAX_BURST)


def _request_json(url: str, *, params: dict | None = None, timeout: int = REQUEST_TIMEOUT):
    """Requests wrapper with retries + rate limiting."""
    _request_gate.ensure_ready()
    last_exc: Exception | None = None
    for attempt in range(REQUEST_MAX_RETRIES):
        try:
            _request_gate.ensure_ready()
            _token_bucket.acquire(1.0)
            rate_limiter.wait()
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code in (418, 429):
                body = str(resp.text or "")
                if resp.status_code == 418:
                    banned_until_ts = _parse_binance_ban_until(body)
                    if banned_until_ts is not None:
                        _request_gate.activate_until(banned_until_ts, reason=body)
                    else:
                        _request_gate.activate_for(300.0, reason=body)
                    raise BinanceRateLimitedError(resp.status_code, body, banned_until_ts=banned_until_ts)
                retry_after_header = resp.headers.get("Retry-After") if getattr(resp, "headers", None) else None
                try:
                    retry_after_seconds = float(retry_after_header) if retry_after_header is not None else 60.0
                except Exception:
                    retry_after_seconds = 60.0
                _request_gate.activate_for(retry_after_seconds, reason=body)
                raise BinanceRateLimitedError(resp.status_code, body, retry_after_seconds=retry_after_seconds)
            if 500 <= resp.status_code < 600:
                raise RuntimeError(f"binance_5xx status={resp.status_code} body={resp.text[:120]}")
            resp.raise_for_status()
            return resp.json()
        except (BinanceRateLimitedError, BinanceCooldownActiveError):
            raise
        except Exception as exc:
            last_exc = exc
            sleep_s = min(8.0, REQUEST_RETRY_BASE_SLEEP * (2 ** attempt))
            sleep_s = sleep_s + (0.05 * (attempt + 1))
            time.sleep(sleep_s)
    raise last_exc or RuntimeError('request_failed')



def get_klines(symbol: str, interval: str, limit: int = 220, *, allow_stale: bool = False, stale_grace_seconds: float = 0.0) -> pd.DataFrame:
    interval_key = str(interval or "").strip().lower()
    cached = _kline_cache.get(symbol, interval_key, limit, allow_stale=allow_stale, stale_grace_seconds=stale_grace_seconds)
    if cached is not None:
        return cached

    url = f"{BINANCE_FUTURES_API}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval_key, "limit": int(limit)}
    payload = _request_json(url, params=params, timeout=REQUEST_TIMEOUT)

    df = pd.DataFrame(
        payload,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df[["open_time", "close_time", "open", "high", "low", "close", "volume"]]
    _kline_cache.set(symbol, interval_key, limit, df)
    return df


def _fetch_btc_regime_snapshot(*, force_refresh: bool = False) -> Dict[str, Any]:
    if not BTC_REGIME_ENABLED:
        return {
            "state": "disabled",
            "bias": "neutral",
            "allow": True,
            "reason": "btc_regime_disabled",
            "block_reason": None,
            "fetched_at_ts": _now_ts(),
        }

    cached = _snapshot_btc_regime()
    cached_ts = _safe_float(cached.get("fetched_at_ts"), 0.0)
    if not force_refresh and cached_ts > 0.0 and (_now_ts() - cached_ts) <= BTC_REGIME_SNAPSHOT_TTL_SECONDS:
        return cached

    try:
        btc_15m = get_klines(
            BTC_REGIME_SYMBOL,
            "15m",
            limit=BTC_REGIME_15M_LOOKBACK,
            allow_stale=True,
            stale_grace_seconds=SCANNER_HTF_STALE_GRACE_SECONDS,
        )
        btc_5m = get_klines(BTC_REGIME_SYMBOL, "5m", limit=BTC_REGIME_5M_LOOKBACK)
        snapshot = _classify_btc_regime(btc_5m, btc_15m)
        snapshot["fetched_at_ts"] = _now_ts()
        return _store_btc_regime(snapshot)
    except Exception as exc:
        logger.warning("⚠️ BTC regime guard no pudo refrescar snapshot: %s", exc)
        cached = _snapshot_btc_regime()
        cached_ts = _safe_float(cached.get("fetched_at_ts"), 0.0)
        if cached_ts > 0.0:
            fallback = dict(cached)
            fallback["stale"] = True
            fallback["reason"] = str(fallback.get("reason") or "btc_regime_cached_stale")
            return fallback
        if BTC_REGIME_FAIL_OPEN:
            return {
                "state": "unknown",
                "bias": "neutral",
                "allow": True,
                "reason": "btc_regime_fetch_failed_fail_open",
                "block_reason": None,
                "error": str(exc),
                "fetched_at_ts": _now_ts(),
            }
        return {
            "state": "vol_shock",
            "bias": "neutral",
            "allow": False,
            "reason": "btc_regime_fetch_failed_fail_closed",
            "block_reason": "btc_regime_fetch_failed_fail_closed",
            "error": str(exc),
            "fetched_at_ts": _now_ts(),
        }



def get_active_futures_symbols(*, allow_stale_on_error: bool = True) -> List[str]:
    cached = _active_symbols_cache.get()
    if cached is not None:
        return cached

    url = f"{BINANCE_FUTURES_API}/fapi/v1/ticker/24hr"
    try:
        payload = _request_json(url, timeout=REQUEST_TIMEOUT)
    except (BinanceRateLimitedError, BinanceCooldownActiveError):
        if allow_stale_on_error:
            stale = _active_symbols_cache.get(allow_stale=True)
            if stale is not None:
                logger.warning("⚠️ Usando cache stale de símbolos activos por cooldown/rate-limit")
                return stale
        raise

    symbols = [
        item["symbol"]
        for item in payload
        if str(item.get("symbol", "")).endswith("USDT")
        and float(item.get("quoteVolume", 0.0) or 0.0) >= MIN_QUOTE_VOLUME
    ]
    _active_symbols_cache.set(symbols)
    logger.info("📊 %s símbolos activos con volumen suficiente", len(symbols))
    return symbols


def recent_duplicate_exists(symbol: str, direction: str, visibility: str) -> bool:
    since = datetime.utcnow() - timedelta(minutes=DEDUP_MINUTES)
    exists = (
        signals_collection().find_one(
            {
                "symbol": symbol,
                "direction": direction,
                "visibility": visibility,
                "created_at": {"$gte": since},
            }
        )
        is not None
    )

    if exists:
        logger.info(
            "♻️ Duplicado reciente detectado: %s %s (%s)",
            symbol,
            direction,
            visibility,
        )
    return exists


def _safe_ratio(num: float, den: float) -> float:
    try:
        den = float(den)
        if den == 0:
            return 0.0
        return float(num) / den
    except Exception:
        return 0.0


def _entry_quality(reference_price: float, direction: str, entry_price: float, stop_loss: float) -> float:
    """Bonus suave por entrada menos tardía usando precio de referencia."""
    try:
        risk = abs(entry_price - stop_loss)
        if risk <= 1e-9:
            return 0.0
        if direction == "LONG":
            progress = max(0.0, reference_price - entry_price) / risk
        else:
            progress = max(0.0, entry_price - reference_price) / risk
        freshness = max(0.0, min(1.0, 1.0 - progress))
        return round(freshness * 10.0, 2)
    except Exception:
        return 0.0


def _volume_quality(df_15m: pd.DataFrame) -> float:
    """Bonus suave de volumen entre 0 y 5."""
    try:
        last = df_15m.iloc[-1]
        volume = float(last["volume"])
        vol_ma = float(df_15m["volume"].tail(20).mean())

        if vol_ma <= 0:
            return 0.0

        ratio = volume / vol_ma

        if ratio >= 1.8:
            return 5.0
        if ratio >= 1.5:
            return 4.0
        if ratio >= 1.3:
            return 3.0
        if ratio >= 1.15:
            return 2.0
        if ratio >= 1.0:
            return 1.0
        return 0.0
    except Exception:
        return 0.0


def _raw_score(signal: Dict) -> float:
    return float(signal.get("raw_score", signal.get("score", 0.0)))


def _normalized_score(signal: Dict) -> float:
    return float(
        signal.get(
            "normalized_score",
            signal.get("score", signal.get("raw_score", 0.0)),
        )
    )


def _setup_group(signal: Dict) -> str:
    return str(signal.get("setup_group", "")).strip().lower()


def _apply_btc_regime_guard(candidate: Optional[Dict], btc_regime: Optional[Dict]) -> Optional[Dict]:
    if not candidate or not BTC_REGIME_ENABLED:
        return candidate

    snapshot = dict(btc_regime or {})
    state = str(snapshot.get("state") or "unknown").strip().lower()
    bias = str(snapshot.get("bias") or "neutral").strip().lower()
    symbol = str(candidate.get("symbol") or "").strip().upper()
    if symbol == BTC_REGIME_SYMBOL and not BTC_REGIME_APPLY_TO_BTC_SYMBOL:
        candidate["btc_regime"] = state
        candidate["btc_regime_bias"] = bias
        candidate["btc_regime_reason"] = "btc_regime_symbol_exempt"
        candidate["btc_regime_guard_action"] = "allow"
        return candidate

    candidate["btc_regime"] = state
    candidate["btc_regime_bias"] = bias
    candidate["btc_regime_reason"] = str(snapshot.get("reason") or "btc_regime_unknown")
    candidate["btc_regime_guard_action"] = "allow"

    direction = str(candidate.get("direction") or "").strip().upper()
    setup_group = _setup_group(candidate)
    raw_score = _raw_score(candidate)
    premium_shock_floor = PREMIUM_RAW_SCORE_MIN + BTC_REGIME_PREMIUM_SHOCK_SCORE_BUFFER

    if state == "vol_shock":
        if setup_group == "premium" and _direction_matches_bias(direction, bias) and raw_score >= premium_shock_floor:
            candidate["btc_regime_guard_action"] = "allow_premium_aligned_shock"
            return candidate
        candidate["btc_regime_guard_action"] = "block"
        candidate["btc_regime_block_reason"] = "btc_regime_vol_shock"
        return None

    if state == "cooldown":
        if setup_group == "premium" and _direction_matches_bias(direction, bias) and raw_score >= premium_shock_floor:
            candidate["btc_regime_guard_action"] = "allow_premium_aligned_cooldown"
            return candidate
        candidate["btc_regime_guard_action"] = "block"
        candidate["btc_regime_block_reason"] = "btc_regime_cooldown"
        return None

    if state == "trend_up" and direction == "SHORT":
        candidate["btc_regime_guard_action"] = "block"
        candidate["btc_regime_block_reason"] = "btc_regime_countertrend"
        return None

    if state == "trend_down" and direction == "LONG":
        candidate["btc_regime_guard_action"] = "block"
        candidate["btc_regime_block_reason"] = "btc_regime_countertrend"
        return None

    return candidate


# ------------------------------------------------------
# CLASIFICACIÓN MUTUAMENTE EXCLUSIVA POR PLAN
# ------------------------------------------------------
# PREMIUM / PLUS / FREE salen directamente del perfil nativo de liquidez.
# No existe promoción posterior entre tiers. La jerarquía la maneja el routing al usuario.
# ------------------------------------------------------

def _qualifies_for_premium(signal: Dict) -> bool:
    return _setup_group(signal) == "premium" and _raw_score(signal) >= PREMIUM_RAW_SCORE_MIN


def _qualifies_for_plus(signal: Dict) -> bool:
    return _setup_group(signal) == "plus" and _raw_score(signal) >= PLUS_RAW_SCORE_MIN


def _qualifies_for_free(signal: Dict) -> bool:
    return _setup_group(signal) == "free" and _raw_score(signal) >= FREE_RAW_SCORE_MIN


def _pick_best(
    pool: List[Dict],
    predicate,
    used_symbols: Set[str],
) -> Optional[Dict]:
    for signal in pool:
        symbol = str(signal.get("symbol", ""))
        if not symbol or symbol in used_symbols:
            continue
        if predicate(signal):
            return signal
    return None


def _build_base_signal(signal: Dict, visibility: str) -> Optional[Dict]:
    return create_base_signal(
        symbol=str(signal["symbol"]),
        direction=str(signal["direction"]).upper(),
        entry_price=float(signal["entry_price"]),
        stop_loss=float(signal["stop_loss"]),
        take_profits=list(signal["take_profits"]),
        timeframes=list(signal.get("timeframes", ["5M"])),
        visibility=visibility,
        score=_raw_score(signal),
        components=signal.get("components", []),
        profiles=signal.get("profiles"),
        atr_pct=signal.get("atr_pct"),
        normalized_score=_normalized_score(signal),
        raw_components=signal.get("raw_components"),
        normalized_components=signal.get("normalized_components"),
        setup_group=signal.get("setup_group"),
        score_profile=signal.get("score_profile"),
        score_calibration=signal.get("score_calibration"),
        send_mode=signal.get("send_mode"),
        entry_model_price=signal.get("entry_model_price"),
        entry_sent_price=signal.get("entry_sent_price"),
        tp1_progress_at_send_pct=signal.get("tp1_progress_at_send_pct"),
        r_progress_at_send=signal.get("r_progress_at_send"),
        setup_stage=signal.get("setup_stage"),
        candidate_tier=signal.get("candidate_tier"),
        final_tier=signal.get("final_tier"),
        entry_model=signal.get("entry_model"),
        current_market_price=signal.get("signal_market_price"),
        strategy_name=signal.get("strategy_name"),
        strategy_version=signal.get("strategy_version"),
        regime_state=signal.get("regime_state"),
        regime_reason=signal.get("regime_reason"),
        regime_bias=signal.get("regime_bias"),
        router_version=signal.get("router_version"),
    )


def _select_dispatchable_signal(
    pool: List[Dict],
    visibility: str,
    used_symbols: Set[str],
) -> Optional[tuple[Dict, Dict]]:
    for signal in pool:
        symbol = str(signal.get("symbol", ""))
        if not symbol or symbol in used_symbols:
            continue

        direction = str(signal.get("direction", "")).upper()
        if recent_duplicate_exists(symbol, direction, visibility):
            continue

        base_signal = _build_base_signal(signal, visibility)
        if not base_signal:
            logger.info(
                "⏭️ Señal descartada al crear base_signal: %s %s (%s) | setup=%s | raw_score=%s | normalized_score=%s | signal_market_price=%s | entry=%s | tp1_progress_at_send_pct=%s | r_progress_at_send=%s",
                symbol,
                direction,
                visibility,
                signal.get("setup_group"),
                signal.get("raw_score"),
                signal.get("normalized_score"),
                signal.get("signal_market_price"),
                signal.get("entry_price"),
                signal.get("tp1_progress_at_send_pct"),
                signal.get("r_progress_at_send"),
            )
            continue

        used_symbols.add(symbol)
        return signal, base_signal

    return None


def _build_candidate(symbol: str, result: Dict, reference_price: float) -> Optional[Dict]:
    if not result:
        return None

    candidate = dict(result)
    direction = str(candidate.get("direction") or "").upper().strip()
    if direction not in {"LONG", "SHORT"}:
        return None

    raw_score = _raw_score(candidate)
    normalized_score = _normalized_score(candidate)
    entry_quality = _entry_quality(
        reference_price,
        direction,
        float(candidate.get("entry_price") or 0.0),
        float(candidate.get("stop_loss") or 0.0),
    )
    final_score = round(
        normalized_score + (entry_quality * 0.35),
        2,
    )

    candidate["symbol"] = symbol
    candidate["direction"] = direction
    candidate["raw_score"] = raw_score
    candidate["normalized_score"] = normalized_score
    candidate["entry_quality"] = entry_quality
    candidate["volume_quality"] = 0.0
    candidate["final_score"] = final_score
    candidate["signal_market_price"] = round(float(reference_price or 0.0), 8) if reference_price else None
    try:
        conservative_profile = (candidate.get("profiles") or {}).get("conservador") or {}
        conservative_tps = list(conservative_profile.get("take_profits") or candidate.get("take_profits") or [])
        if conservative_tps:
            candidate["tp1_progress_at_send_pct"] = _progress_from_model_to_tp1_pct(
                float(candidate.get("entry_price") or 0.0),
                float(conservative_tps[0]),
                float(reference_price or 0.0),
                direction,
            )
        candidate["r_progress_at_send"] = _r_progress_from_model_entry(
            float(candidate.get("entry_price") or 0.0),
            float(candidate.get("stop_loss") or 0.0),
            float(reference_price or 0.0),
            direction,
        )
    except Exception:
        candidate.setdefault("tp1_progress_at_send_pct", None)
        candidate.setdefault("r_progress_at_send", None)
    candidate.setdefault("send_mode", "entry_zone_pending")
    candidate.setdefault("candidate_tier", candidate.get("setup_group"))
    candidate.setdefault("final_tier", candidate.get("setup_group"))
    return candidate


def _record_failure(debug_counts: Dict[str, int], reason: str) -> None:
    if not reason:
        reason = "unknown"
    debug_counts[reason] = int(debug_counts.get(reason, 0)) + 1


def _merge_debug_counts(total: Dict[str, int], local: Dict[str, int]) -> None:
    for key, value in (local or {}).items():
        total[key] = int(total.get(key, 0)) + int(value)


def _compact_rejects(rejects: Dict[str, int], limit: int = 10) -> Dict[str, int]:
    items = sorted((rejects or {}).items(), key=lambda kv: kv[1], reverse=True)
    compact = {k: int(v) for k, v in items[: max(1, int(limit))]}
    other = sum(int(v) for _k, v in items[max(1, int(limit)) :])
    if other:
        compact["__other__"] = other
    return compact



def _extract_failure_reason(local: Dict[str, int]) -> Optional[str]:
    if not local:
        return None
    return max(local.items(), key=lambda item: item[1])[0]


def _process_symbol(symbol: str, *, refresh_15m: bool = False, refresh_1h: bool = False, market_regime: Optional[Dict] = None) -> Tuple[Optional[Dict], Dict[str, int], Optional[str]]:
    local_debug: Dict[str, int] = {}
    try:
        df_1h = get_klines(
            symbol,
            "1h",
            limit=KLINE_LIMIT_1H,
            allow_stale=not refresh_1h,
            stale_grace_seconds=SCANNER_HTF_STALE_GRACE_SECONDS,
        )
        df_15m = get_klines(
            symbol,
            "15m",
            limit=KLINE_LIMIT_15M,
            allow_stale=not refresh_15m,
            stale_grace_seconds=SCANNER_HTF_STALE_GRACE_SECONDS,
        )
        df_5m = get_klines(symbol, "5m", limit=KLINE_LIMIT_5M) if SCANNER_FETCH_5M else None
        candidate = route_symbol_candidate(symbol, df_1h, df_15m, df_5m, market_regime=market_regime, debug_counts=local_debug)
        if candidate is None:
            return None, local_debug, None
        return candidate, local_debug, None
    except Exception as exc:
        return None, local_debug, f"{symbol}: {exc}"


async def scan_market_async(bot: Bot):
    logger.info(
        "📡 Scanner iniciado — clasificación exclusiva por plan + ranking con normalized_score"
    )
    logger.info(
        "⚙️ Scanner config | concurrency=%s | request_delay_env=%ss | effective_request_delay=%ss | force_request_delay=%s | fetch_5m=%s | htf_cache=%s | 5m_cache_seconds=%s | htf_stale_grace=%ss | symbol_cache=%ss | bootstrap_batch=%s | 15m_refresh_batch=%s | 1h_refresh_batch=%s | kline_limits={'1h': %s, '15m': %s, '5m': %s}",
        SCANNER_SYMBOL_CONCURRENCY,
        REQUEST_DELAY,
        EFFECTIVE_REQUEST_DELAY,
        SCANNER_FORCE_REQUEST_DELAY,
        SCANNER_FETCH_5M,
        SCANNER_ENABLE_HTF_CACHE,
        SCANNER_5M_CACHE_SECONDS,
        SCANNER_HTF_STALE_GRACE_SECONDS,
        ACTIVE_SYMBOLS_CACHE_SECONDS,
        SCANNER_BOOTSTRAP_BATCH_SIZE,
        SCANNER_15M_REFRESH_BATCH_SIZE,
        SCANNER_1H_REFRESH_BATCH_SIZE,
        KLINE_LIMIT_1H,
        KLINE_LIMIT_15M,
        KLINE_LIMIT_5M,
    )

    cycle_number = 0

    while True:
        try:
            cooldown_remaining = _request_gate.remaining_seconds()
            if cooldown_remaining > 0.0:
                wait_seconds = min(max(1.0, cooldown_remaining + 1.0), 900.0)
                logger.warning("⏸️ Scanner en cooldown por rate-limit Binance | remaining=%.1fs | reason=%s", cooldown_remaining, _request_gate.reason()[:160])
                heartbeat("scanner", status="warn", details={"cooldown_seconds": cooldown_remaining, "reason": _request_gate.reason()[:200]})
                await asyncio.sleep(wait_seconds)
                continue

            cycle_started_at = datetime.utcnow()
            cache_stats_before = _kline_cache.snapshot()
            symbols = get_active_futures_symbols()
            market_regime = regime_engine.fetch_market_regime_snapshot(get_klines, force_refresh=False)
            symbols_for_cycle, bootstrap_mode = _select_symbols_for_cycle(symbols, cycle_number)
            refresh_15m_symbols = _rotating_refresh_subset(symbols_for_cycle if bootstrap_mode else symbols, SCANNER_15M_REFRESH_BATCH_SIZE, cycle_number)
            refresh_1h_symbols = _rotating_refresh_subset(symbols_for_cycle if bootstrap_mode else symbols, SCANNER_1H_REFRESH_BATCH_SIZE, cycle_number)
            active_symbols = symbols_for_cycle if bootstrap_mode else symbols
            candidates: List[Dict] = []
            reject_totals: Dict[str, int] = {}
            failures = 0
            failure_samples: List[str] = []
            semaphore = asyncio.Semaphore(SCANNER_SYMBOL_CONCURRENCY)

            async def _run(symbol: str):
                async with semaphore:
                    return await asyncio.to_thread(
                        _process_symbol,
                        symbol,
                        refresh_15m=(symbol in refresh_15m_symbols),
                        refresh_1h=(symbol in refresh_1h_symbols),
                        market_regime=market_regime,
                    )

            results = await asyncio.gather(*[_run(symbol) for symbol in active_symbols])
            for candidate, local_debug, failure in results:
                if candidate:
                    candidates.append(candidate)
                else:
                    _merge_debug_counts(reject_totals, local_debug)
                if failure:
                    failures += 1
                    if len(failure_samples) < 5:
                        failure_samples.append(failure)

            cycle_duration = (datetime.utcnow() - cycle_started_at).total_seconds()
            cache_stats = _kline_cache.delta(cache_stats_before)

            if failures and not candidates:
                logger.warning(
                    "📭 Sin oportunidades en este ciclo, pero hubo errores de scanner | cycle=%s symbols=%s failures=%s lag=n/a duration=%.3fs samples=%s | cache=%s",
                    cycle_number,
                    len(active_symbols),
                    failures,
                    cycle_duration,
                    failure_samples,
                    cache_stats,
                )
                heartbeat(
                    "scanner",
                    status="warn",
                    details={
                        "cycle": cycle_number,
                        "symbols": len(active_symbols),
                        "candidates": 0,
                        "selected": 0,
                        "failures": failures,
                        "failure_samples": failure_samples,
                        "duration_seconds": cycle_duration,
                        "kline_cache": cache_stats,
                        "bootstrap_mode": bootstrap_mode,
                        "universe_symbols": len(symbols),
                        "active_symbols": len(active_symbols),
                    },
                )
                cycle_number += 1
                await asyncio.sleep(max(0.0, SCAN_INTERVAL_SECONDS - cycle_duration))
                continue

            if not candidates:
                logger.info(
                    "📭 No hay oportunidades fuertes en este ciclo | duration=%.3fs | rejects=%s | cache=%s",
                    cycle_duration,
                    _compact_rejects(reject_totals),
                    cache_stats,
                )
                heartbeat(
                    "scanner",
                    status="ok",
                    details={
                        "cycle": cycle_number,
                        "symbols": len(active_symbols),
                        "candidates": 0,
                        "selected": 0,
                        "failures": failures,
                        "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
                        "cycle_started_at": cycle_started_at.isoformat(),
                        "duration_seconds": cycle_duration,
                        "rejects": reject_totals,
                        "kline_cache": cache_stats,
                        "bootstrap_mode": bootstrap_mode,
                        "universe_symbols": len(symbols),
                        "active_symbols": len(active_symbols),
                    },
                )
                cycle_number += 1
                await asyncio.sleep(max(0.0, SCAN_INTERVAL_SECONDS - cycle_duration))
                continue

            candidates.sort(
                key=lambda x: (
                    x.get("final_score", _normalized_score(x)),
                    x.get("normalized_score", x.get("score", _raw_score(x))),
                    x.get("raw_score", x.get("score", 0)),
                    x.get("entry_quality", 0),
                    x.get("volume_quality", 0),
                    x.get("symbol", ""),
                ),
                reverse=True,
            )

            premium_candidates = [c for c in candidates if _qualifies_for_premium(c)]
            plus_candidates = [c for c in candidates if _qualifies_for_plus(c)]
            free_candidates = [c for c in candidates if _qualifies_for_free(c)]

            logger.info(
                "📚 Candidatos | total=%s | premium=%s | plus=%s | free=%s | regime=%s/%s | strategy=%s | duration=%.3fs | bootstrap=%s active=%s universe=%s | rejects=%s | cache=%s",
                len(candidates),
                len(premium_candidates),
                len(plus_candidates),
                len(free_candidates),
                market_regime.get("state"),
                market_regime.get("bias"),
                market_regime.get("strategy_name"),
                cycle_duration,
                bootstrap_mode,
                len(active_symbols),
                len(symbols),
                _compact_rejects(reject_totals),
                cache_stats,
            )

            used_symbols: Set[str] = set()
            selected = [
                (PLAN_PREMIUM, "🥇 ORO", premium_candidates),
                (PLAN_PLUS, "🥈 PLATA", plus_candidates),
                (PLAN_FREE, "🥉 BRONCE", free_candidates),
            ]
            selected_count = 0

            for visibility, medal, pool in selected:
                chosen = _select_dispatchable_signal(pool, visibility, used_symbols)
                if not chosen:
                    continue

                signal, base_signal = chosen
                symbol = str(signal["symbol"])
                direction = str(signal["direction"]).upper()
                raw_score = _raw_score(signal)
                normalized_score = _normalized_score(signal)
                final_score = float(signal.get("final_score", normalized_score))

                try:
                    enqueue_signal_dispatch(base_signal)
                    selected_count += 1
                except Exception as e:
                    logger.error("⚠️ Error encolando señal para dispatch: %s", e, exc_info=True)
                    continue

                logger.info(
                    "✅ %s | %s %s | raw_score=%s | normalized_score=%s | final_score=%s | entry_q=%s | vol_q=%s | setup=%s | plan=%s | calib=%s | send_mode=%s | stage=%s | strategy=%s | regime=%s",
                    medal,
                    symbol,
                    direction,
                    raw_score,
                    normalized_score,
                    final_score,
                    signal.get("entry_quality", 0),
                    signal.get("volume_quality", 0),
                    signal.get("setup_group", "unknown"),
                    visibility,
                    signal.get("score_calibration", "unknown"),
                    signal.get("send_mode", "unknown"),
                    signal.get("setup_stage", "unknown"),
                    signal.get("strategy_name", "unknown"),
                    signal.get("regime_state", market_regime.get("state")),
                )

            heartbeat(
                "scanner",
                status="ok",
                details={
                    "cycle": cycle_number,
                    "symbols": len(symbols),
                    "candidates": len(candidates),
                    "premium_candidates": len(premium_candidates),
                    "plus_candidates": len(plus_candidates),
                    "free_candidates": len(free_candidates),
                    "selected": selected_count,
                    "failures": failures,
                    "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
                    "cycle_started_at": cycle_started_at.isoformat(),
                    "duration_seconds": cycle_duration,
                    "rejects": reject_totals,
                    "kline_cache": cache_stats,
                    "market_regime": market_regime,
                    "btc_regime": market_regime,
                },
            )
            cycle_number += 1
            await asyncio.sleep(max(0.0, SCAN_INTERVAL_SECONDS - cycle_duration))

        except (BinanceRateLimitedError, BinanceCooldownActiveError) as exc:
            remaining = _request_gate.remaining_seconds()
            wait_seconds = min(max(1.0, remaining + 1.0 if remaining > 0.0 else 60.0), 900.0)
            heartbeat("scanner", status="warn", details={"error": str(exc), "cycle": cycle_number, "cooldown_seconds": remaining})
            logger.warning("⏸️ Scanner pausado por rate-limit Binance | wait=%.1fs | error=%s", wait_seconds, exc)
            await asyncio.sleep(wait_seconds)
        except Exception as exc:
            heartbeat("scanner", status="error", details={"error": str(exc), "cycle": cycle_number})
            logger.error("❌ Error crítico en scanner", exc_info=True)
            await asyncio.sleep(60)


def scan_market(bot: Bot):
    logger.info("🚀 Iniciando scanner en thread separado")
    asyncio.run(scan_market_async(bot))
