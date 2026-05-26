from __future__ import annotations

from typing import Optional, Dict, Tuple, List, Any

import hashlib
import math
import os
from datetime import datetime, timedelta, timezone
import pandas as pd

from app import breakout_reset_setup_store

try:
    import ta  # type: ignore
except Exception:  # pragma: no cover - fallback for constrained runtimes/tests
    class _FallbackTrend:
        @staticmethod
        def ema_indicator(series: pd.Series, window: int) -> pd.Series:
            return series.ewm(span=window, adjust=False, min_periods=window).mean()

        @staticmethod
        def adx(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.Series:
            high = high.astype(float)
            low = low.astype(float)
            close = close.astype(float)
            up_move = high.diff()
            down_move = -low.diff()
            plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
            minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
            prev_close = close.shift(1)
            tr = pd.concat([
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ], axis=1).max(axis=1)
            atr = tr.ewm(alpha=1 / max(window, 1), adjust=False, min_periods=window).mean()
            plus_di = 100 * (plus_dm.ewm(alpha=1 / max(window, 1), adjust=False, min_periods=window).mean() / atr.replace(0, pd.NA))
            minus_di = 100 * (minus_dm.ewm(alpha=1 / max(window, 1), adjust=False, min_periods=window).mean() / atr.replace(0, pd.NA))
            dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)) * 100
            return dx.ewm(alpha=1 / max(window, 1), adjust=False, min_periods=window).mean().fillna(0.0)

    class _FallbackVolatility:
        @staticmethod
        def average_true_range(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.Series:
            high = high.astype(float)
            low = low.astype(float)
            close = close.astype(float)
            prev_close = close.shift(1)
            tr = pd.concat([
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ], axis=1).max(axis=1)
            return tr.ewm(alpha=1 / max(window, 1), adjust=False, min_periods=window).mean()

    class _FallbackTA:
        trend = _FallbackTrend()
        volatility = _FallbackVolatility()

    ta = _FallbackTA()

# =======================================
# CONFIGURACIÓN BASE
# =======================================

EMA_FAST = 20
EMA_MID = 50
EMA_SLOW = 200

ADX_PERIOD = 14
ATR_PERIOD = 14
BREAKOUT_LOOKBACK = 24

MAX_SCORE = 100.0
FREE_NORMALIZATION_PENALTY = 8.0
SCORE_CALIBRATION_VERSION = "v18_multi_reset_timing_guard"
ENTRY_MODEL_NAME = "breakout_reset_multi_model_timing_guard_v1"
SETUP_STAGE_PRE_RESET_WAITING_RETEST = "pre_reset_waiting_retest"
SETUP_STAGE_RESET_TOUCH_LIVE = "reset_touch_live"
SETUP_STAGE_RESET_REBOUNDED_BEFORE_PUBLISH = "reset_rebounded_before_publish"
SEND_MODE_PENDING_ENTRY = "entry_zone_pending"
ENTRY_ZONE_MIN_PCT = float(os.getenv("ENTRY_ZONE_MIN_PCT", "0.0015"))
ENTRY_ZONE_MAX_PCT = float(os.getenv("ENTRY_ZONE_MAX_PCT", "0.0035"))
ENTRY_ZONE_RISK_FRACTION = float(os.getenv("ENTRY_ZONE_RISK_FRACTION", "0.22"))
PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "83"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "76"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "72"))
BREAKOUT_RESET_RECENT_BARS = int(os.getenv("BREAKOUT_RESET_RECENT_BARS", "6"))
BREAKOUT_RESET_INVALIDATION_BODY_ATR = float(os.getenv("BREAKOUT_RESET_INVALIDATION_BODY_ATR", "0.20"))
BREAKOUT_RESET_SETUP_TTL_BARS = int(os.getenv("BREAKOUT_RESET_SETUP_TTL_BARS", "10"))
BREAKOUT_RESET_SETUP_TTL_MINUTES = int(os.getenv("BREAKOUT_RESET_SETUP_TTL_MINUTES", str(max(30, BREAKOUT_RESET_SETUP_TTL_BARS * 5))))
BREAKOUT_RESET_STATEFUL_ENABLED = str(os.getenv("BREAKOUT_RESET_STATEFUL_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
BREAKOUT_RESET_SOFT_GATES_ENABLED = str(os.getenv("BREAKOUT_RESET_SOFT_GATES_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
BREAKOUT_RESET_HARD_ADX_MIN = float(os.getenv("BREAKOUT_RESET_HARD_ADX_MIN", "8.0"))
BREAKOUT_RESET_ATR_HARD_MIN_FRACTION = float(os.getenv("BREAKOUT_RESET_ATR_HARD_MIN_FRACTION", "0.55"))
BREAKOUT_RESET_ATR_HARD_MAX_MULT = float(os.getenv("BREAKOUT_RESET_ATR_HARD_MAX_MULT", "1.35"))
BREAKOUT_RESET_BREAKOUT_BODY_HARD_FRACTION = float(os.getenv("BREAKOUT_RESET_BREAKOUT_BODY_HARD_FRACTION", "0.55"))
BREAKOUT_RESET_CONTINUATION_BODY_HARD_FRACTION = float(os.getenv("BREAKOUT_RESET_CONTINUATION_BODY_HARD_FRACTION", "0.55"))
BREAKOUT_RESET_MIN_OVERSHOOT_HARD_ATR = float(os.getenv("BREAKOUT_RESET_MIN_OVERSHOOT_HARD_ATR", "0.035"))
BREAKOUT_RESET_MIN_PRE_RESET_SPACE_HARD_ATR = float(os.getenv("BREAKOUT_RESET_MIN_PRE_RESET_SPACE_HARD_ATR", "0.020"))
BREAKOUT_RESET_MIN_EXTENSION_HARD_ATR = float(os.getenv("BREAKOUT_RESET_MIN_EXTENSION_HARD_ATR", "0.040"))
BREAKOUT_RESET_MAX_EXTENSION_HARD_ATR = float(os.getenv("BREAKOUT_RESET_MAX_EXTENSION_HARD_ATR", "1.35"))
BREAKOUT_RESET_ADAPTIVE_RESET_ZONE_ENABLED = str(os.getenv("BREAKOUT_RESET_ADAPTIVE_RESET_ZONE_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
BREAKOUT_RESET_RESET_ZONE_PADDING_ATR = float(os.getenv("BREAKOUT_RESET_RESET_ZONE_PADDING_ATR", "0.12"))
BREAKOUT_RESET_RESET_ZONE_MAX_PADDING_ATR = float(os.getenv("BREAKOUT_RESET_RESET_ZONE_MAX_PADDING_ATR", "0.18"))
BREAKOUT_RESET_RESET_ZONE_NEAR_MISS_ATR = float(os.getenv("BREAKOUT_RESET_RESET_ZONE_NEAR_MISS_ATR", "0.10"))
BREAKOUT_RESET_MULTI_RESET_ENABLED = str(os.getenv("BREAKOUT_RESET_MULTI_RESET_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
BREAKOUT_RESET_ALLOW_EMA20_RESET = str(os.getenv("BREAKOUT_RESET_ALLOW_EMA20_RESET", "true")).strip().lower() in {"1", "true", "yes", "on"}
BREAKOUT_RESET_ALLOW_MIDPOINT_RESET = str(os.getenv("BREAKOUT_RESET_ALLOW_MIDPOINT_RESET", "true")).strip().lower() in {"1", "true", "yes", "on"}
BREAKOUT_RESET_ALLOW_SHALLOW_RESET = str(os.getenv("BREAKOUT_RESET_ALLOW_SHALLOW_RESET", "true")).strip().lower() in {"1", "true", "yes", "on"}
BREAKOUT_RESET_SHALLOW_MIN_ATR = float(os.getenv("BREAKOUT_RESET_SHALLOW_MIN_ATR", "0.16"))
BREAKOUT_RESET_SHALLOW_MAX_ATR = float(os.getenv("BREAKOUT_RESET_SHALLOW_MAX_ATR", "0.42"))
BREAKOUT_RESET_PUBLICATION_TIMING_GUARD_ENABLED = str(os.getenv("BREAKOUT_RESET_PUBLICATION_TIMING_GUARD_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
BREAKOUT_RESET_MAX_ENTRY_SLIPPAGE_ATR = float(os.getenv("BREAKOUT_RESET_MAX_ENTRY_SLIPPAGE_ATR", "0.12"))
BREAKOUT_RESET_MAX_TP1_PROGRESS_BEFORE_PUBLISH = float(os.getenv("BREAKOUT_RESET_MAX_TP1_PROGRESS_BEFORE_PUBLISH", "0.30"))
BREAKOUT_RESET_MAX_LIVE_RANGE_ATR = float(os.getenv("BREAKOUT_RESET_MAX_LIVE_RANGE_ATR", "1.35"))


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _required_history_bars() -> int:
    """Minimum number of 5M candles required to avoid indicator warmup NaNs.

    This strategy uses EMA200 + ADX/ATR + breakout lookback windows, so anything below
    ~230 bars can silently kill signal generation (trend_structure always fails).
    """
    return max(EMA_SLOW + 30, BREAKOUT_LOOKBACK + 90, 260)

# =======================================
# PERFILES DE VALIDACIÓN
# =======================================
# SHARED_PROFILE:
#   benchmark comparable entre perfiles. Lo usamos para normalizar score,
#   y además sirve como base del perfil PLUS.
# FREE_PROFILE:
#   setup más flexible, pero ahora un poco más exigente que antes.
# PREMIUM_PROFILE:
#   mismo ADN breakout + reset, pero con puertas algo más estrictas que PLUS.

SHARED_PROFILE = {
    "name": "shared",
    "adx_min": _env_float("PLUS_ADX_MIN", 17.1),
    "atr_pct_min": _env_float("PLUS_ATR_PCT_MIN", 0.0023),
    "atr_pct_max": _env_float("PLUS_ATR_PCT_MAX", 0.0128),
    "min_body_ratio_breakout": _env_float("PLUS_MIN_BODY_RATIO_BREAKOUT", 0.27),
    "min_body_ratio_continuation": _env_float("PLUS_MIN_BODY_RATIO_CONTINUATION", 0.19),
    "min_extension_atr": _env_float("PLUS_MIN_EXTENSION_ATR", 0.22),
    "max_extension_atr": _env_float("PLUS_MAX_EXTENSION_ATR", 0.78),
    "min_breakout_overshoot_atr": _env_float("PLUS_MIN_BREAKOUT_OVERSHOOT_ATR", 0.12),
    "min_pre_reset_space_atr": _env_float("PLUS_MIN_PRE_RESET_SPACE_ATR", 0.10),
    "min_rel_volume_continuation": _env_float("PLUS_MIN_REL_VOLUME_CONTINUATION", 1.05),
    "min_close_position_continuation": _env_float("PLUS_MIN_CLOSE_POSITION_CONTINUATION", 0.61),
    "min_post_breakout_progress_atr": _env_float("PLUS_MIN_POST_BREAKOUT_PROGRESS_ATR", 0.06),
}

FREE_PROFILE = {
    "name": "free",
    "adx_min": _env_float("FREE_ADX_MIN", 15.8),
    "atr_pct_min": _env_float("FREE_ATR_PCT_MIN", 0.0020),
    "atr_pct_max": _env_float("FREE_ATR_PCT_MAX", 0.0142),
    "min_body_ratio_breakout": _env_float("FREE_MIN_BODY_RATIO_BREAKOUT", 0.22),
    "min_body_ratio_continuation": _env_float("FREE_MIN_BODY_RATIO_CONTINUATION", 0.16),
    "min_extension_atr": _env_float("FREE_MIN_EXTENSION_ATR", 0.18),
    "max_extension_atr": _env_float("FREE_MAX_EXTENSION_ATR", 0.86),
    "min_breakout_overshoot_atr": _env_float("FREE_MIN_BREAKOUT_OVERSHOOT_ATR", 0.08),
    "min_pre_reset_space_atr": _env_float("FREE_MIN_PRE_RESET_SPACE_ATR", 0.06),
    "min_rel_volume_continuation": _env_float("FREE_MIN_REL_VOLUME_CONTINUATION", 0.98),
    "min_close_position_continuation": _env_float("FREE_MIN_CLOSE_POSITION_CONTINUATION", 0.54),
    "min_post_breakout_progress_atr": _env_float("FREE_MIN_POST_BREAKOUT_PROGRESS_ATR", 0.04),
    "score": 78.0,
}

PLUS_PROFILE = {
    **SHARED_PROFILE,
    "name": "plus",
    "score": 86.0,
}

PREMIUM_PROFILE = {
    **SHARED_PROFILE,
    "name": "premium",
    "adx_min": _env_float("PREMIUM_ADX_MIN", 17.8),
    "atr_pct_min": _env_float("PREMIUM_ATR_PCT_MIN", 0.0025),
    "atr_pct_max": _env_float("PREMIUM_ATR_PCT_MAX", 0.0122),
    "min_body_ratio_breakout": _env_float("PREMIUM_MIN_BODY_RATIO_BREAKOUT", 0.30),
    "min_body_ratio_continuation": _env_float("PREMIUM_MIN_BODY_RATIO_CONTINUATION", 0.21),
    "min_extension_atr": _env_float("PREMIUM_MIN_EXTENSION_ATR", 0.26),
    "max_extension_atr": _env_float("PREMIUM_MAX_EXTENSION_ATR", 0.70),
    "min_breakout_overshoot_atr": _env_float("PREMIUM_MIN_BREAKOUT_OVERSHOOT_ATR", 0.16),
    "min_pre_reset_space_atr": _env_float("PREMIUM_MIN_PRE_RESET_SPACE_ATR", 0.14),
    "min_rel_volume_continuation": _env_float("PREMIUM_MIN_REL_VOLUME_CONTINUATION", 1.14),
    "min_close_position_continuation": _env_float("PREMIUM_MIN_CLOSE_POSITION_CONTINUATION", 0.70),
    "min_post_breakout_progress_atr": _env_float("PREMIUM_MIN_POST_BREAKOUT_PROGRESS_ATR", 0.10),
    "score": 90.0,
}

# =======================================
# PERFILES DE TRADING POR APALANCAMIENTO
# =======================================

TRADING_PROFILES = {
    "conservador": {
        "leverage": "20x-30x",
        "stop_atr_mult": _env_float("TRADE_CONSERVADOR_STOP_ATR_MULT", 0.95),
        "min_stop_pct": _env_float("TRADE_CONSERVADOR_MIN_STOP_PCT", 0.0062),
        "max_stop_pct": _env_float("TRADE_CONSERVADOR_MAX_STOP_PCT", 0.0098),
        "tp1_rr": _env_float("TRADE_CONSERVADOR_TP1_RR", 1.00),
        "tp2_rr": _env_float("TRADE_CONSERVADOR_TP2_RR", 1.85),
    },
    "moderado": {
        "leverage": "30x-40x",
        "stop_atr_mult": _env_float("TRADE_MODERADO_STOP_ATR_MULT", 0.85),
        "min_stop_pct": _env_float("TRADE_MODERADO_MIN_STOP_PCT", 0.0054),
        "max_stop_pct": _env_float("TRADE_MODERADO_MAX_STOP_PCT", 0.0084),
        "tp1_rr": _env_float("TRADE_MODERADO_TP1_RR", 1.12),
        "tp2_rr": _env_float("TRADE_MODERADO_TP2_RR", 2.00),
    },
    "agresivo": {
        "leverage": "40x-50x",
        "stop_atr_mult": _env_float("TRADE_AGRESIVO_STOP_ATR_MULT", 0.78),
        "min_stop_pct": _env_float("TRADE_AGRESIVO_MIN_STOP_PCT", 0.0048),
        "max_stop_pct": _env_float("TRADE_AGRESIVO_MAX_STOP_PCT", 0.0072),
        "tp1_rr": _env_float("TRADE_AGRESIVO_TP1_RR", 1.28),
        "tp2_rr": _env_float("TRADE_AGRESIVO_TP2_RR", 2.25),
    },
}


# =======================================
# INDICADORES
# =======================================


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ema20"] = ta.trend.ema_indicator(df["close"], EMA_FAST)
    df["ema50"] = ta.trend.ema_indicator(df["close"], EMA_MID)
    df["ema200"] = ta.trend.ema_indicator(df["close"], EMA_SLOW)

    df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], ADX_PERIOD)

    atr = ta.volatility.average_true_range(
        df["high"], df["low"], df["close"], ATR_PERIOD
    )
    df["atr"] = atr
    df["atr_pct"] = df["atr"] / df["close"]

    df["body"] = (df["close"] - df["open"]).abs()
    df["range"] = (df["high"] - df["low"]).replace(0, 1e-9)
    df["body_ratio"] = df["body"] / df["range"]

    df["vol_ma"] = df["volume"].rolling(20).mean()

    return df


def _indicators_ready(last: pd.Series) -> bool:
    try:
        required = ["ema20", "ema50", "ema200", "adx", "atr", "atr_pct", "body_ratio"]
        for key in required:
            v = float(last.get(key))
            if math.isnan(v) or not math.isfinite(v):
                return False
        return True
    except Exception:
        return False


# =======================================
# HELPERS
# =======================================


def _record_reject(debug_counts: Optional[Dict[str, int]], reason: str) -> None:
    if debug_counts is None:
        return
    key = str(reason or "unknown").strip() or "unknown"
    debug_counts[key] = int(debug_counts.get(key, 0)) + 1


def _record_soft_gate(debug_counts: Optional[Dict[str, int]], quality: Optional[Dict[str, float]], reason: str) -> None:
    """Track a quality issue that should reduce score instead of killing the setup.

    Phase 6 explicitly separates safety hard gates from quality gates. The score
    components already penalize weak ADX/body/volume/extension; this helper makes
    the penalty observable in the admin/debug funnel without returning None early.
    """
    key = str(reason or "soft_gate").strip() or "soft_gate"
    _record_reject(debug_counts, f"soft_gate_{key}")
    if quality is not None:
        quality[f"soft_gate_{key}"] = 1.0
        quality["soft_gate_count"] = float(quality.get("soft_gate_count", 0.0) or 0.0) + 1.0


def _atr_pct_within_hard_safety_band(atr_pct: float, profile: Dict) -> bool:
    lo = float(profile["atr_pct_min"]) * float(BREAKOUT_RESET_ATR_HARD_MIN_FRACTION)
    hi = float(profile["atr_pct_max"]) * float(BREAKOUT_RESET_ATR_HARD_MAX_MULT)
    return lo <= float(atr_pct) <= hi


def _continuation_hard_gate(last: pd.Series, direction: str, profile: Dict) -> bool:
    """Minimal safety gate for the post-breakout candle.

    The old _continuation_ok remains available for compatibility/tests, but the
    live strategy now lets marginal continuation quality flow into scoring.
    """
    if direction == "LONG":
        if float(last["close"]) <= float(last["open"]):
            return False
    else:
        if float(last["close"]) >= float(last["open"]):
            return False
    hard_body_min = float(profile["min_body_ratio_continuation"]) * float(BREAKOUT_RESET_CONTINUATION_BODY_HARD_FRACTION)
    return float(last["body_ratio"]) >= hard_body_min


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


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


def _calculate_entry_zone(entry: float, stop_loss: float) -> Tuple[float, float]:
    entry = float(entry)
    risk_pct = abs(entry - float(stop_loss)) / max(abs(entry), 1e-9)
    zone_pct = _clamp(risk_pct * ENTRY_ZONE_RISK_FRACTION, ENTRY_ZONE_MIN_PCT, ENTRY_ZONE_MAX_PCT)
    low = _round_price_dynamic(entry * (1 - zone_pct))
    high = _round_price_dynamic(entry * (1 + zone_pct))
    return low, high


def _expand_reset_zone_with_atr(zone_low: float, zone_high: float, atr: float) -> Tuple[float, float]:
    """Expand the reset touch band by a bounded ATR padding.

    Breakout resets in fast crypto markets often miss the exact broken level by
    a few basis points. This keeps the original risk-derived zone as the anchor,
    but gives the live-touch detector a small, bounded ATR cushion instead of
    requiring a perfect tick-level retest.
    """
    if not BREAKOUT_RESET_ADAPTIVE_RESET_ZONE_ENABLED:
        return float(zone_low), float(zone_high)
    try:
        atr_value = max(float(atr), 0.0)
    except Exception:
        atr_value = 0.0
    if atr_value <= 0:
        return float(zone_low), float(zone_high)
    padding_atr = _clamp(
        float(BREAKOUT_RESET_RESET_ZONE_PADDING_ATR),
        0.0,
        max(float(BREAKOUT_RESET_RESET_ZONE_MAX_PADDING_ATR), 0.0),
    )
    padding = atr_value * padding_atr
    if padding <= 0:
        return float(zone_low), float(zone_high)
    return _round_price_dynamic(float(zone_low) - padding), _round_price_dynamic(float(zone_high) + padding)


def _entry_zone_distance_atr(zone_low: float, zone_high: float, atr: float, candle_high: Optional[float], candle_low: Optional[float]) -> float:
    """Distance from a candle range to the reset zone, normalized by ATR."""
    try:
        atr_value = max(float(atr), 1e-9)
        high = float(candle_high) if candle_high is not None else float("nan")
        low = float(candle_low) if candle_low is not None else float("nan")
    except Exception:
        return float("inf")
    if not math.isfinite(high) or not math.isfinite(low):
        return float("inf")
    if low <= float(zone_high) and high >= float(zone_low):
        return 0.0
    if low > float(zone_high):
        return max(0.0, low - float(zone_high)) / atr_value
    return max(0.0, float(zone_low) - high) / atr_value


def _is_reset_near_miss(zone_low: float, zone_high: float, atr: float, candle_high: Optional[float], candle_low: Optional[float]) -> bool:
    return _entry_zone_distance_atr(zone_low, zone_high, atr, candle_high, candle_low) <= max(float(BREAKOUT_RESET_RESET_ZONE_NEAR_MISS_ATR), 0.0)


def _candle_touched_entry_zone(zone_low: float, zone_high: float, candle_high: Optional[float], candle_low: Optional[float]) -> bool:
    if candle_high is None or candle_low is None:
        return False
    return float(candle_low) <= float(zone_high) and float(candle_high) >= float(zone_low)


def _classify_live_reset_state(
    direction: str,
    current_price: float,
    zone_low: float,
    zone_high: float,
    *,
    candle_high: Optional[float] = None,
    candle_low: Optional[float] = None,
) -> str:
    direction = str(direction).upper().strip()
    current_price = float(current_price)
    touched_zone = _candle_touched_entry_zone(zone_low, zone_high, candle_high, candle_low)

    if direction == "LONG":
        if zone_low <= current_price <= zone_high:
            return SETUP_STAGE_RESET_TOUCH_LIVE
        if current_price > zone_high:
            return SETUP_STAGE_PRE_RESET_WAITING_RETEST if not touched_zone else SETUP_STAGE_RESET_REBOUNDED_BEFORE_PUBLISH
        return "reset_late_or_lost"

    if zone_low <= current_price <= zone_high:
        return SETUP_STAGE_RESET_TOUCH_LIVE
    if current_price < zone_low:
        return SETUP_STAGE_PRE_RESET_WAITING_RETEST if not touched_zone else SETUP_STAGE_RESET_REBOUNDED_BEFORE_PUBLISH
    return "reset_late_or_lost"


def _valid_reset_anchor(anchor: Optional[float], direction: str, level: float, reference_price: float) -> bool:
    try:
        value = float(anchor)
        level = float(level)
        reference_price = float(reference_price)
    except Exception:
        return False
    if not math.isfinite(value) or value <= 0:
        return False
    # Reset anchors must remain on the breakout side of the broken level and not
    # beyond the current reference extension. Otherwise they are either invalid
    # retests or late chase entries.
    if str(direction).upper() == "LONG":
        return level <= value <= max(reference_price, level)
    return min(reference_price, level) <= value <= level


def _reset_model_candidates(quality: Dict[str, float], last: pd.Series, direction: str, atr: float) -> List[Dict[str, float]]:
    """Build executable reset models without allowing late chase entries.

    Models are evaluated in priority order. The classic broken-level retest is
    still preferred, but strong continuations may reset on EMA20, the breakout
    body midpoint, or a shallow ATR pullback. Every model is later protected by
    the publication timing guard, so additional models cannot publish a signal
    that already ran toward TP1.
    """
    direction = str(direction).upper().strip()
    level = float(quality.get("level") or 0.0)
    reference_price = float(quality.get("reference_price") or last.get("close", 0.0) or 0.0)
    atr_value = max(float(atr or 0.0), 1e-9)
    models: List[Dict[str, float]] = []

    def add_model(name: str, anchor: Optional[float]) -> None:
        if not _valid_reset_anchor(anchor, direction, level, reference_price):
            return
        rounded = _round_price_dynamic(float(anchor))
        for existing in models:
            if abs(float(existing["entry_model_price"]) - rounded) <= atr_value * 0.015:
                return
        models.append({"name": name, "entry_model_price": rounded})

    add_model("level", _reset_entry_price(level, last, direction))

    if BREAKOUT_RESET_MULTI_RESET_ENABLED and BREAKOUT_RESET_ALLOW_EMA20_RESET:
        add_model("ema20", float(last.get("ema20", 0.0) or 0.0))

    if BREAKOUT_RESET_MULTI_RESET_ENABLED and BREAKOUT_RESET_ALLOW_MIDPOINT_RESET:
        bo = quality.get("breakout_candle_open")
        bc = quality.get("breakout_candle_close")
        if bo is not None and bc is not None:
            add_model("midpoint", (float(bo) + float(bc)) / 2.0)

    if BREAKOUT_RESET_MULTI_RESET_ENABLED and BREAKOUT_RESET_ALLOW_SHALLOW_RESET:
        current_extension = float(quality.get("current_extension_atr", quality.get("extension_atr", 0.0)) or 0.0)
        shallow_atr = _clamp(
            current_extension * 0.45,
            max(float(BREAKOUT_RESET_SHALLOW_MIN_ATR), 0.0),
            max(float(BREAKOUT_RESET_SHALLOW_MAX_ATR), float(BREAKOUT_RESET_SHALLOW_MIN_ATR)),
        )
        if direction == "LONG":
            add_model("shallow", level + (atr_value * shallow_atr))
        else:
            add_model("shallow", level - (atr_value * shallow_atr))

    return models


def _model_zone(entry_model_price: float, direction: str, atr_pct: float, atr: float) -> Dict[str, float]:
    trade_profiles = _build_trade_profiles(float(entry_model_price), direction, atr_pct)
    conservative = trade_profiles.get("conservador") or {}
    stop_loss = float(conservative.get("stop_loss") or 0.0)
    if stop_loss <= 0:
        return {}
    base_low, base_high = _calculate_entry_zone(float(entry_model_price), stop_loss)
    zone_low, zone_high = _expand_reset_zone_with_atr(base_low, base_high, atr)
    return {
        "entry_model_price": float(entry_model_price),
        "stop_loss": stop_loss,
        "base_zone_low": float(base_low),
        "base_zone_high": float(base_high),
        "zone_low": float(zone_low),
        "zone_high": float(zone_high),
        "trade_profiles": trade_profiles,
    }


def _select_live_reset_model(
    quality: Dict[str, float],
    last: pd.Series,
    direction: str,
    atr: float,
    atr_pct: float,
    live_price: float,
    live_high: float,
    live_low: float,
    debug_counts: Optional[Dict[str, int]],
) -> Tuple[Optional[Dict[str, Any]], str]:
    best_wait_stage = SETUP_STAGE_PRE_RESET_WAITING_RETEST
    near_miss_recorded = False
    for model in _reset_model_candidates(quality, last, direction, atr):
        zone = _model_zone(float(model["entry_model_price"]), direction, atr_pct, atr)
        if not zone:
            continue
        stage = _classify_live_reset_state(
            direction,
            live_price,
            float(zone["zone_low"]),
            float(zone["zone_high"]),
            candle_high=live_high,
            candle_low=live_low,
        )
        if stage == SETUP_STAGE_RESET_TOUCH_LIVE:
            _record_reject(debug_counts, "breakout_reset_touch_live")
            _record_reject(debug_counts, f"breakout_reset_model_{model['name']}")
            payload = dict(zone)
            payload["reset_model"] = str(model["name"])
            payload["stage"] = stage
            return payload, stage
        if _is_reset_near_miss(float(zone["zone_low"]), float(zone["zone_high"]), atr, live_high, live_low):
            near_miss_recorded = True
            _record_reject(debug_counts, f"breakout_reset_near_miss_{model['name']}")
        if stage == SETUP_STAGE_RESET_REBOUNDED_BEFORE_PUBLISH:
            best_wait_stage = SETUP_STAGE_RESET_REBOUNDED_BEFORE_PUBLISH
        elif stage == "reset_late_or_lost" and best_wait_stage != SETUP_STAGE_RESET_REBOUNDED_BEFORE_PUBLISH:
            best_wait_stage = "reset_late_or_lost"

    if near_miss_recorded:
        _record_reject(debug_counts, "breakout_reset_near_miss")
    return None, best_wait_stage


def _tp1_progress(direction: str, entry_price: float, current_price: float, tp1: float) -> float:
    direction = str(direction).upper().strip()
    entry_price = float(entry_price)
    current_price = float(current_price)
    tp1 = float(tp1)
    if direction == "LONG":
        distance = max(tp1 - entry_price, 1e-9)
        return max(0.0, current_price - entry_price) / distance
    distance = max(entry_price - tp1, 1e-9)
    return max(0.0, entry_price - current_price) / distance


def _publication_timing_guard(
    *,
    direction: str,
    live_price: float,
    live_high: float,
    live_low: float,
    entry_model_price: float,
    entry_price: float,
    atr: float,
    trade_profiles: Dict[str, Dict],
    debug_counts: Optional[Dict[str, int]],
) -> Tuple[bool, str, Dict[str, float]]:
    if not BREAKOUT_RESET_PUBLICATION_TIMING_GUARD_ENABLED:
        return True, "", {}
    atr_value = max(float(atr or 0.0), 1e-9)
    conservative = trade_profiles.get("conservador") or {}
    tps = conservative.get("take_profits") or []
    if not tps:
        _record_reject(debug_counts, "publication_timing_missing_tp1")
        return False, "publication_timing_missing_tp1", {}
    tp1 = float(tps[0])
    stop_loss = float(conservative.get("stop_loss") or 0.0)
    direction = str(direction).upper().strip()
    slippage_atr = abs(float(live_price) - float(entry_model_price)) / atr_value
    progress = _tp1_progress(direction, float(entry_price), float(live_price), tp1)
    live_range_atr = max(0.0, float(live_high) - float(live_low)) / atr_value
    diagnostics = {
        "entry_slippage_atr": round(slippage_atr, 4),
        "tp1_progress": round(progress, 4),
        "live_range_atr": round(live_range_atr, 4),
    }

    if slippage_atr > max(float(BREAKOUT_RESET_MAX_ENTRY_SLIPPAGE_ATR), 0.0):
        _record_reject(debug_counts, "publication_timing_entry_slippage")
        return False, "publication_timing_entry_slippage", diagnostics
    if progress > max(float(BREAKOUT_RESET_MAX_TP1_PROGRESS_BEFORE_PUBLISH), 0.0):
        _record_reject(debug_counts, "publication_timing_tp1_progress")
        return False, "publication_timing_tp1_progress", diagnostics
    if live_range_atr > max(float(BREAKOUT_RESET_MAX_LIVE_RANGE_ATR), 0.0):
        _record_reject(debug_counts, "publication_timing_live_range_extended")
        return False, "publication_timing_live_range_extended", diagnostics
    if direction == "LONG":
        if float(live_high) >= tp1:
            _record_reject(debug_counts, "publication_timing_tp1_already_touched")
            return False, "publication_timing_tp1_already_touched", diagnostics
        if stop_loss > 0 and float(live_low) <= stop_loss:
            _record_reject(debug_counts, "publication_timing_sl_already_touched")
            return False, "publication_timing_sl_already_touched", diagnostics
    else:
        if float(live_low) <= tp1:
            _record_reject(debug_counts, "publication_timing_tp1_already_touched")
            return False, "publication_timing_tp1_already_touched", diagnostics
        if stop_loss > 0 and float(live_high) >= stop_loss:
            _record_reject(debug_counts, "publication_timing_sl_already_touched")
            return False, "publication_timing_sl_already_touched", diagnostics
    return True, "", diagnostics


def _round_price_dynamic(value: float) -> float:
    return round(float(value), _price_round_digits(value))


def _volatility_regime_adjustment(atr_pct: float) -> float:
    if atr_pct >= 0.0105:
        return 1.08
    if atr_pct >= 0.0085:
        return 1.04
    if atr_pct <= 0.0032:
        return 0.94
    if atr_pct <= 0.0042:
        return 0.97
    return 1.0



def _adaptive_stop_pct(atr_pct: float, cfg: Dict) -> float:
    base_stop_pct = float(atr_pct) * float(cfg["stop_atr_mult"])
    adjusted_stop_pct = base_stop_pct * _volatility_regime_adjustment(float(atr_pct))
    min_stop_pct = float(cfg["min_stop_pct"])
    max_stop_pct = float(cfg["max_stop_pct"])
    return _clamp(adjusted_stop_pct, min_stop_pct, max_stop_pct)



def breakout_level(df: pd.DataFrame, direction: str) -> float:
    ref = df.iloc[-(BREAKOUT_LOOKBACK + 2):-2]

    if direction == "LONG":
        return float(ref["high"].max())

    return float(ref["low"].min())



def _trend_direction(last: pd.Series) -> Optional[str]:
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    ema200 = float(last["ema200"])
    close = float(last["close"])
    tolerance = max(close * 0.0010, 1e-9)

    # Regla original estricta primero.
    if ema20 > ema50 > ema200:
        return "LONG"
    if ema20 < ema50 < ema200:
        return "SHORT"

    # Relajación controlada: permitimos la transición donde EMA20 > EMA50
    # pero EMA50 aún no superó EMA200, siempre que el precio esté claramente
    # por encima de EMA200 (sesgo alcista confirmado en largo plazo).
    # Las condiciones "near cross" (EMA20 ≈ EMA50) han sido eliminadas porque
    # generaban señales en zonas de transición con win rate inferior al 35%.
    if ema20 > ema50 and close > ema200 and (close - ema200) > tolerance * 2:
        return "LONG"
    if ema20 < ema50 and close < ema200 and (ema200 - close) > tolerance * 2:
        return "SHORT"

    # Phase 6: allow a narrow EMA20/EMA50 near-cross as a soft continuation
    # state. This is not a full trend gate bypass: price must already be on the
    # correct side of EMA50 and EMA200, and the fast/medium separation must be
    # genuinely tight. Wider disagreement still rejects as trend_structure.
    near_cross = abs(ema20 - ema50) <= tolerance
    if near_cross and ema50 > ema200 and close > ema50 and (close - ema200) > tolerance * 2:
        return "LONG"
    if near_cross and ema50 < ema200 and close < ema50 and (ema200 - close) > tolerance * 2:
        return "SHORT"

    return None



def _trend_strength_score(last: pd.Series) -> float:
    close = max(float(last["close"]), 1e-9)
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    ema200 = float(last["ema200"])

    sep_fast = abs(ema20 - ema50) / close
    sep_slow = abs(ema50 - ema200) / close
    total_sep = sep_fast + sep_slow

    # 2.2% de separación acumulada ya cuenta como fuerza plena.
    return _clamp((total_sep / 0.022) * 18.0, 0.0, 18.0)



def _higher_tf_short_context_ok(df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> Tuple[bool, Dict[str, float]]:
    """
    Filtro contextual LIVIANO solo para SHORT.

    No convierte la estrategia en un sistema MTF completo: la entrada sigue
    naciendo en 5M con breakout + retest. Este filtro solo veta shorts que van
    claramente contra un contexto superior demasiado alcista.
    """
    diag: Dict[str, float] = {}

    if len(df_15m) < 220 or len(df_1h) < 220:
        # Si falta contexto suficiente, no vetamos el short. Preferimos no romper
        # cobertura por falta de histórico en timeframes superiores.
        return True, {"filter_applied": 0.0, "reason": 0.0}

    df15 = add_indicators(df_15m)
    df1h = add_indicators(df_1h)

    last15 = df15.iloc[-1]
    last1h = df1h.iloc[-1]

    if not _indicators_ready(last15) or not _indicators_ready(last1h):
        # If indicators are not ready on higher TFs, do not block shorts.
        return True, {"filter_applied": 0.0, "reason": -1.0}


    dir15 = _trend_direction(last15)
    dir1h = _trend_direction(last1h)
    strength15 = _trend_strength_score(last15)
    strength1h = _trend_strength_score(last1h)

    close15 = float(last15["close"])
    close1h = float(last1h["close"])
    ema20_15 = float(last15["ema20"])
    ema50_15 = float(last15["ema50"])
    ema20_1h = float(last1h["ema20"])
    ema50_1h = float(last1h["ema50"])

    above_ema20_15 = 1.0 if close15 > ema20_15 else 0.0
    above_ema20_1h = 1.0 if close1h > ema20_1h else 0.0
    bullish_bias_15 = 1.0 if ema20_15 > ema50_15 else 0.0
    bullish_bias_1h = 1.0 if ema20_1h > ema50_1h else 0.0

    diag = {
        "filter_applied": 1.0,
        "dir_15m_long": 1.0 if dir15 == "LONG" else 0.0,
        "dir_1h_long": 1.0 if dir1h == "LONG" else 0.0,
        "strength_15m": round(float(strength15), 2),
        "strength_1h": round(float(strength1h), 2),
        "close_above_ema20_15m": above_ema20_15,
        "close_above_ema20_1h": above_ema20_1h,
        "ema20_gt_ema50_15m": bullish_bias_15,
        "ema20_gt_ema50_1h": bullish_bias_1h,
    }

    # Veto fuerte: ambos marcos siguen claramente largos y además el precio se
    # sostiene por encima de EMA20. Ahí el short en 5M suele carecer de follow-through.
    if (
        dir15 == "LONG"
        and dir1h == "LONG"
        and strength15 >= 7.0
        and strength1h >= 7.0
        and close15 >= ema20_15
        and close1h >= ema20_1h
    ):
        diag["blocked"] = 1.0
        diag["block_reason"] = 1.0
        return False, diag

    # Veto intermedio: el 1H está claramente alcista y el 15M no muestra debilidad
    # suficiente todavía. Esto reduce shorts correctivos dentro de impulsos mayores.
    if (
        dir1h == "LONG"
        and strength1h >= 9.0
        and close1h >= ema20_1h
        and bullish_bias_15 == 1.0
        and above_ema20_15 == 1.0
    ):
        diag["blocked"] = 1.0
        diag["block_reason"] = 2.0
        return False, diag

    diag["blocked"] = 0.0
    diag["block_reason"] = 0.0
    return True, diag



def _higher_tf_long_context_ok(df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> Tuple[bool, Dict[str, float]]:
    """
    Filtro contextual LIVIANO solo para LONG.

    Veta LONGs en 5M que van claramente contra un contexto superior demasiado
    bajista. Simétrico a _higher_tf_short_context_ok para mantener coherencia
    entre direcciones y evitar el sesgo de win rate que tenía LONG (25% WR)
    por carecer de este filtro.
    """
    diag: Dict[str, float] = {}

    if len(df_15m) < 220 or len(df_1h) < 220:
        return True, {"filter_applied": 0.0, "reason": 0.0}

    df15 = add_indicators(df_15m)
    df1h = add_indicators(df_1h)

    last15 = df15.iloc[-1]
    last1h = df1h.iloc[-1]

    if not _indicators_ready(last15) or not _indicators_ready(last1h):
        return True, {"filter_applied": 0.0, "reason": -1.0}

    dir15 = _trend_direction(last15)
    dir1h = _trend_direction(last1h)
    strength15 = _trend_strength_score(last15)
    strength1h = _trend_strength_score(last1h)

    close15 = float(last15["close"])
    close1h = float(last1h["close"])
    ema20_15 = float(last15["ema20"])
    ema50_15 = float(last15["ema50"])
    ema20_1h = float(last1h["ema20"])
    ema50_1h = float(last1h["ema50"])

    below_ema20_15 = 1.0 if close15 < ema20_15 else 0.0
    below_ema20_1h = 1.0 if close1h < ema20_1h else 0.0
    bearish_bias_15 = 1.0 if ema20_15 < ema50_15 else 0.0
    bearish_bias_1h = 1.0 if ema20_1h < ema50_1h else 0.0

    diag = {
        "filter_applied": 1.0,
        "dir_15m_short": 1.0 if dir15 == "SHORT" else 0.0,
        "dir_1h_short": 1.0 if dir1h == "SHORT" else 0.0,
        "strength_15m": round(float(strength15), 2),
        "strength_1h": round(float(strength1h), 2),
        "close_below_ema20_15m": below_ema20_15,
        "close_below_ema20_1h": below_ema20_1h,
        "ema20_lt_ema50_15m": bearish_bias_15,
        "ema20_lt_ema50_1h": bearish_bias_1h,
    }

    # Veto fuerte: ambos marcos siguen claramente cortos y el precio se
    # sostiene por debajo de EMA20. El LONG en 5M carecería de follow-through.
    if (
        dir15 == "SHORT"
        and dir1h == "SHORT"
        and strength15 >= 7.0
        and strength1h >= 7.0
        and close15 <= ema20_15
        and close1h <= ema20_1h
    ):
        diag["blocked"] = 1.0
        diag["block_reason"] = 1.0
        return False, diag

    # Veto intermedio: el 1H está claramente bajista y el 15M no muestra
    # fortaleza alcista suficiente. Reduce LONGs correctivos dentro de impulsos bajistas.
    if (
        dir1h == "SHORT"
        and strength1h >= 9.0
        and close1h <= ema20_1h
        and bearish_bias_15 == 1.0
        and below_ema20_15 == 1.0
    ):
        diag["blocked"] = 1.0
        diag["block_reason"] = 2.0
        return False, diag

    diag["blocked"] = 0.0
    diag["block_reason"] = 0.0
    return True, diag



def _adx_score(adx_value: float, adx_min: float) -> float:
    # Pleno puntaje alrededor de adx_min + 18.
    return _clamp(((adx_value - adx_min) / 18.0) * 16.0, 0.0, 16.0)


def _atr_score(atr_pct: float, profile: Dict) -> float:
    lo = float(profile["atr_pct_min"])
    hi = float(profile["atr_pct_max"])
    mid = (lo + hi) / 2.0
    half = max((hi - lo) / 2.0, 1e-9)

    # Máximo cerca del centro del rango. Penaliza extremos.
    distance = abs(atr_pct - mid) / half
    return _clamp((1.0 - distance) * 12.0, 0.0, 12.0)



def _volume_score(last: pd.Series) -> float:
    vol_ma = float(last.get("vol_ma", 0.0) or 0.0)
    volume = float(last.get("volume", 0.0) or 0.0)

    if vol_ma <= 0:
        return 0.0

    ratio = volume / vol_ma

    if ratio >= 2.0:
        return 10.0
    if ratio >= 1.7:
        return 8.5
    if ratio >= 1.4:
        return 6.5
    if ratio >= 1.2:
        return 4.5
    if ratio >= 1.0:
        return 2.5
    return 0.0



def _breakout_level_for_candle(df: pd.DataFrame, candle_pos: int, direction: str) -> Optional[float]:
    """Return the structural level broken by a specific candle.

    The previous implementation always used the last two candles. That made
    Breakout + Reset behave like a one-snapshot pattern. Here the level belongs
    to the candle that actually broke structure, so a setup can remain valid for
    several candles while it waits for the reset.
    """
    try:
        pos = int(candle_pos)
        if pos <= 0:
            return None
        start = max(0, pos - int(BREAKOUT_LOOKBACK))
        ref = df.iloc[start:pos]
        if len(ref) < max(8, min(int(BREAKOUT_LOOKBACK), 12)):
            return None
        if str(direction).upper() == "LONG":
            return float(ref["high"].max())
        return float(ref["low"].min())
    except Exception:
        return None


def _recent_breakout_candidate_positions(df: pd.DataFrame) -> List[int]:
    last_pos = len(df) - 1
    lookback = max(1, int(BREAKOUT_RESET_RECENT_BARS))
    first_pos = max(int(BREAKOUT_LOOKBACK), last_pos - lookback + 1)
    if first_pos > last_pos:
        return []
    # Newest first: if two breakouts are still alive, the latest structure is
    # the one whose reset zone should be published.
    return list(range(last_pos, first_pos - 1, -1))


def _post_breakout_invalidated(df: pd.DataFrame, breakout_pos: int, level: float, direction: str, atr: float) -> Tuple[bool, str]:
    if breakout_pos >= len(df) - 1:
        return False, ""
    post = df.iloc[breakout_pos + 1:]
    body_threshold = max(float(atr) * float(BREAKOUT_RESET_INVALIDATION_BODY_ATR), 1e-9)

    if direction == "LONG":
        back_inside = post[post["close"].astype(float) < float(level)]
    else:
        back_inside = post[post["close"].astype(float) > float(level)]

    if back_inside.empty:
        return False, ""

    bodies = (back_inside["close"].astype(float) - back_inside["open"].astype(float)).abs()
    if bool((bodies >= body_threshold).any()):
        return True, "breakout_invalidated"
    return True, "breakout_drifted_back_inside"




def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _row_time_value(row: pd.Series) -> Optional[str]:
    for key in ("close_time", "open_time", "timestamp", "time"):
        value = row.get(key) if hasattr(row, "get") else None
        if value is None:
            continue
        try:
            ts = pd.Timestamp(value)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("UTC").tz_localize(None)
            return ts.isoformat()
        except Exception:
            return str(value)
    try:
        value = row.name
        if value is not None:
            ts = pd.Timestamp(value)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("UTC").tz_localize(None)
            return ts.isoformat()
    except Exception:
        pass
    return None


def _setup_id(symbol: str, direction: str, level: float, breakout_time: Optional[str], breakout_pos: Optional[int]) -> str:
    raw = "|".join([
        str(symbol or "UNKNOWN").upper().strip(),
        str(direction or "").upper().strip(),
        f"{float(level):.12f}",
        str(breakout_time or ""),
        str(breakout_pos if breakout_pos is not None else ""),
    ])
    return "br_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def _quality_from_setup_doc(doc: Dict[str, Any], last: pd.Series) -> Dict[str, float]:
    quality = dict(doc.get("quality") or {})
    level = float(doc.get("level") or quality.get("level") or 0.0)
    atr = max(float(last.get("atr", doc.get("atr", 0.0)) or doc.get("atr", 0.0) or 0.0), 1e-9)
    reference_price = float(last.get("close", quality.get("reference_price", 0.0)) or 0.0)
    direction = str(doc.get("direction") or "").upper().strip()
    if direction == "LONG":
        current_extension_atr = max(0.0, reference_price - level) / atr
    else:
        current_extension_atr = max(0.0, level - reference_price) / atr
    # Keep the original post-breakout extension for quality scoring. At the live
    # reset touch the current distance to the level is naturally small; using it
    # would incorrectly downgrade a valid stateful setup.
    original_extension_atr = float(quality.get("extension_atr", current_extension_atr) or current_extension_atr)
    quality.update({
        "level": level,
        "reference_price": reference_price,
        "extension_atr": original_extension_atr,
        "current_extension_atr": current_extension_atr,
        "setup_id": str(doc.get("setup_id") or ""),
        "stateful_setup": 1.0,
    })
    return quality


def _locate_breakout_position(df: pd.DataFrame, setup_doc: Dict[str, Any]) -> Optional[int]:
    try:
        target_time = str(setup_doc.get("breakout_candle_time") or "").strip()
        if target_time:
            for pos in range(len(df)):
                if _row_time_value(df.iloc[pos]) == target_time:
                    return pos
        pos_value = setup_doc.get("breakout_candle_pos")
        if pos_value is not None:
            pos = int(float(pos_value))
            if 0 <= pos < len(df):
                return pos
    except Exception:
        return None
    return None


def _setup_age_bars(df: pd.DataFrame, setup_doc: Dict[str, Any]) -> Optional[int]:
    pos = _locate_breakout_position(df, setup_doc)
    if pos is None:
        return None
    return max(0, (len(df) - 1) - int(pos))


def _is_setup_expired(df: pd.DataFrame, setup_doc: Dict[str, Any]) -> bool:
    age_bars = _setup_age_bars(df, setup_doc)
    if age_bars is not None and age_bars > int(BREAKOUT_RESET_SETUP_TTL_BARS):
        return True
    expires_at = setup_doc.get("expires_at")
    if expires_at is not None:
        try:
            expires_ts = pd.Timestamp(expires_at)
            now_ts = pd.Timestamp(_utcnow_naive())
            if expires_ts.tzinfo is not None:
                expires_ts = expires_ts.tz_convert("UTC").tz_localize(None)
            return now_ts > expires_ts
        except Exception:
            return False
    return False


def _setup_still_structurally_valid(df: pd.DataFrame, setup_doc: Dict[str, Any], last: pd.Series) -> Tuple[bool, str]:
    direction = str(setup_doc.get("direction") or "").upper().strip()
    level = float(setup_doc.get("level") or 0.0)
    atr = max(float(last.get("atr", setup_doc.get("atr", 0.0)) or setup_doc.get("atr", 0.0) or 0.0), 1e-9)
    close_price = float(last.get("close", 0.0) or 0.0)
    if level <= 0 or close_price <= 0:
        return False, "stateful_setup_invalid_price"
    if direction == "LONG" and close_price <= level:
        return False, "stateful_setup_back_inside_range"
    if direction == "SHORT" and close_price >= level:
        return False, "stateful_setup_back_inside_range"
    pos = _locate_breakout_position(df, setup_doc)
    if pos is not None:
        invalidated, reason = _post_breakout_invalidated(df, pos, level, direction, atr)
        if invalidated:
            return False, reason or "stateful_setup_invalidated"
    return True, ""


def _load_stateful_setup(symbol: Optional[str], direction: str, df: pd.DataFrame, profile: Dict, debug_counts: Optional[Dict[str, int]]) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, Any]]]:
    if not BREAKOUT_RESET_STATEFUL_ENABLED or not symbol:
        return None, None
    last = df.iloc[-1]
    for setup_doc in breakout_reset_setup_store.find_waiting_setups(str(symbol), direction=direction, limit=5):
        setup_id = str(setup_doc.get("setup_id") or "")
        if _is_setup_expired(df, setup_doc):
            breakout_reset_setup_store.mark_setup_status(setup_id, "expired", "expired_no_reset")
            _record_reject(debug_counts, "breakout_setup_expired")
            continue
        valid, reason = _setup_still_structurally_valid(df, setup_doc, last)
        if not valid:
            breakout_reset_setup_store.mark_setup_status(setup_id, "invalidated", reason)
            _record_reject(debug_counts, reason or "breakout_setup_invalidated")
            continue
        quality = _quality_from_setup_doc(setup_doc, last)
        min_ext = float(profile.get("min_extension_atr", 0.15))
        max_ext = float(profile.get("max_extension_atr", 0.95))
        ext = float(quality.get("extension_atr", 0.0) or 0.0)
        if ext < min_ext or ext > max_ext:
            # Do not invalidate yet: it may pull back into the usable reset band before TTL.
            _record_reject(debug_counts, "breakout_stateful_extension_wait")
        quality["stateful_loaded"] = 1.0
        quality["setup_age_bars"] = float(_setup_age_bars(df, setup_doc) or quality.get("breakout_age_bars", 0.0) or 0.0)
        return quality, setup_doc
    return None, None


def _persist_waiting_setup(
    *,
    symbol: Optional[str],
    direction: str,
    profile: Dict,
    quality: Dict[str, float],
    entry_price: float,
    zone_low: float,
    zone_high: float,
    atr: float,
    atr_pct: float,
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    if not BREAKOUT_RESET_STATEFUL_ENABLED or not symbol:
        return None
    level = float(quality.get("level") or 0.0)
    breakout_pos_raw = quality.get("breakout_candle_pos")
    breakout_pos = int(float(breakout_pos_raw)) if breakout_pos_raw is not None else None
    breakout_time = None
    if breakout_pos is not None and 0 <= breakout_pos < len(df):
        breakout_time = _row_time_value(df.iloc[breakout_pos])
    setup_id = _setup_id(str(symbol), direction, level, breakout_time, breakout_pos)
    now = _utcnow_naive()
    expires_at = now + timedelta(minutes=int(BREAKOUT_RESET_SETUP_TTL_MINUTES))
    payload = {
        "setup_id": setup_id,
        "symbol": str(symbol).upper().strip(),
        "direction": str(direction).upper().strip(),
        "status": "waiting_reset",
        "level": level,
        "entry_model_price": float(entry_price),
        "zone_low": float(zone_low),
        "zone_high": float(zone_high),
        "atr": float(atr),
        "atr_pct": float(atr_pct),
        "profile": str(profile.get("name") or "unknown"),
        "quality": dict(quality),
        "breakout_candle_pos": breakout_pos,
        "breakout_candle_time": breakout_time,
        "expires_at": expires_at,
        "ttl_bars": int(BREAKOUT_RESET_SETUP_TTL_BARS),
        "ttl_minutes": int(BREAKOUT_RESET_SETUP_TTL_MINUTES),
        "strategy_version": SCORE_CALIBRATION_VERSION,
    }
    return breakout_reset_setup_store.upsert_waiting_setup(payload)

def _confirm_breakout_prereset(
    df: pd.DataFrame,
    direction: str,
    profile: Dict,
    reference_market_price: Optional[float],
) -> Tuple[bool, Dict[str, float], Optional[str]]:
    """
    Detect a recent confirmed breakout that is still alive and waiting for a
    first live reset.

    Phase 4 change:
    - old model: previous candle must be the breakout and the last closed candle
      must be the continuation candle;
    - new model: search the last N closed candles for a valid breakout, then
      verify it has not been invalidated or already reset before publishing.

    This keeps the first-touch safety model, but stops missing valid setups that
    take 2-6 candles to pull back into the reset zone.
    """
    if len(df) < max(BREAKOUT_LOOKBACK + 3, 32):
        return False, {}, "insufficient_history"

    direction = str(direction).upper().strip()
    last = df.iloc[-1]
    atr = float(last.get("atr", 0.0) or 0.0)
    setup_reference_price = float(last.get("close", 0.0) or 0.0)
    current_price = float(reference_market_price or setup_reference_price or 0.0)

    if atr <= 0 or current_price <= 0 or setup_reference_price <= 0:
        return False, {}, "invalid_reference_price"

    min_ext = float(profile.get("min_extension_atr", 0.15))
    max_ext = float(profile.get("max_extension_atr", 0.95))
    min_body = float(profile.get("min_body_ratio_breakout", 0.0))
    min_overshoot_atr = float(profile.get("min_breakout_overshoot_atr", 0.0) or 0.0)
    min_pre_reset_space_atr = float(profile.get("min_pre_reset_space_atr", 0.0) or 0.0)

    positions = _recent_breakout_candidate_positions(df)
    if not positions:
        return False, {}, "breakout_shape"

    fallback_reason = "breakout_shape"

    for pos in positions:
        breakout = df.iloc[pos]
        level = _breakout_level_for_candle(df, pos, direction)
        if level is None or not math.isfinite(float(level)):
            continue

        body_ratio = float(breakout.get("body_ratio", 0.0) or 0.0)
        hard_body_min = min_body * float(BREAKOUT_RESET_BREAKOUT_BODY_HARD_FRACTION)
        soft_gate_reasons: List[str] = []
        if body_ratio < hard_body_min:
            if fallback_reason not in {"breakout_invalidated", "breakout_drifted_back_inside"}:
                fallback_reason = "breakout_shape"
            continue
        if BREAKOUT_RESET_SOFT_GATES_ENABLED and body_ratio < min_body:
            soft_gate_reasons.append("breakout_body_ratio")

        if direction == "LONG":
            breakout_ok = float(breakout["close"]) > float(level) and float(breakout["high"]) > float(level)
            overshoot_atr = max(0.0, float(breakout["close"]) - float(level)) / atr
            extension_atr = max(0.0, setup_reference_price - float(level)) / atr
            post_window = df.iloc[pos + 1:] if pos < len(df) - 1 else df.iloc[0:0]
            if not post_window.empty:
                pre_reset_space_atr = max(0.0, float(post_window["low"].astype(float).min()) - float(level)) / atr
            else:
                pre_reset_space_atr = max(0.0, float(breakout["low"]) - float(level)) / atr
            still_on_breakout_side = setup_reference_price > float(level)
        else:
            breakout_ok = float(breakout["close"]) < float(level) and float(breakout["low"]) < float(level)
            overshoot_atr = max(0.0, float(level) - float(breakout["close"])) / atr
            extension_atr = max(0.0, float(level) - setup_reference_price) / atr
            post_window = df.iloc[pos + 1:] if pos < len(df) - 1 else df.iloc[0:0]
            if not post_window.empty:
                pre_reset_space_atr = max(0.0, float(level) - float(post_window["high"].astype(float).max())) / atr
            else:
                pre_reset_space_atr = max(0.0, float(level) - float(breakout["high"])) / atr
            still_on_breakout_side = setup_reference_price < float(level)

        if not breakout_ok or not still_on_breakout_side:
            if fallback_reason not in {"breakout_invalidated", "breakout_drifted_back_inside", "breakout_overshoot_hard", "breakout_extension_hard", "reset_freshness_hard"}:
                fallback_reason = "breakout_shape"
            continue

        invalidated, invalid_reason = _post_breakout_invalidated(df, pos, float(level), direction, atr)
        if invalidated:
            fallback_reason = invalid_reason or "breakout_invalidated"
            continue

        if overshoot_atr < float(BREAKOUT_RESET_MIN_OVERSHOOT_HARD_ATR):
            if fallback_reason not in {"breakout_invalidated", "breakout_drifted_back_inside"}:
                fallback_reason = "breakout_overshoot_hard"
            continue
        if BREAKOUT_RESET_SOFT_GATES_ENABLED and overshoot_atr < min_overshoot_atr:
            soft_gate_reasons.append("breakout_overshoot")

        if extension_atr < float(BREAKOUT_RESET_MIN_EXTENSION_HARD_ATR) or extension_atr > float(BREAKOUT_RESET_MAX_EXTENSION_HARD_ATR):
            if fallback_reason not in {"breakout_invalidated", "breakout_drifted_back_inside"}:
                fallback_reason = "breakout_extension_hard"
            continue
        if BREAKOUT_RESET_SOFT_GATES_ENABLED and (extension_atr < min_ext or extension_atr > max_ext):
            soft_gate_reasons.append("breakout_extension")

        if pre_reset_space_atr < float(BREAKOUT_RESET_MIN_PRE_RESET_SPACE_HARD_ATR):
            if fallback_reason not in {"breakout_invalidated", "breakout_drifted_back_inside"}:
                fallback_reason = "reset_freshness_hard"
            continue
        if BREAKOUT_RESET_SOFT_GATES_ENABLED and pre_reset_space_atr < min_pre_reset_space_atr:
            soft_gate_reasons.append("reset_freshness")

        quality = {
            "level": float(level),
            "breakout_body_ratio": float(body_ratio),
            "continuation_body_ratio": float(last.get("body_ratio", 0.0) or 0.0),
            "extension_atr": float(extension_atr),
            "overshoot_atr": float(overshoot_atr),
            "reference_price": float(setup_reference_price),
            "pre_reset_space_atr": float(pre_reset_space_atr),
            "breakout_age_bars": float((len(df) - 1) - pos),
            "breakout_candle_pos": float(pos),
            "recent_breakout_window_bars": float(max(1, int(BREAKOUT_RESET_RECENT_BARS))),
            "post_breakout_bars": float(max(0, (len(df) - 1) - pos)),
            "breakout_candle_open": float(breakout.get("open", 0.0) or 0.0),
            "breakout_candle_high": float(breakout.get("high", 0.0) or 0.0),
            "breakout_candle_low": float(breakout.get("low", 0.0) or 0.0),
            "breakout_candle_close": float(breakout.get("close", 0.0) or 0.0),
            "soft_gate_count": float(len(soft_gate_reasons)),
        }
        for reason in soft_gate_reasons:
            quality[f"soft_gate_{reason}"] = 1.0
        return True, quality, None

    return False, {}, fallback_reason


def _relative_volume_ratio(last: pd.Series) -> float:
    vol_ma = float(last.get("vol_ma", 0.0) or 0.0)
    volume = float(last.get("volume", 0.0) or 0.0)
    if vol_ma <= 1e-9:
        return 0.0
    return max(0.0, volume / vol_ma)


def _close_position_ratio(last: pd.Series, direction: str) -> float:
    high = float(last.get("high", 0.0) or 0.0)
    low = float(last.get("low", 0.0) or 0.0)
    close = float(last.get("close", 0.0) or 0.0)
    candle_range = max(high - low, 1e-9)
    if direction == "LONG":
        return _clamp((close - low) / candle_range, 0.0, 1.0)
    return _clamp((high - close) / candle_range, 0.0, 1.0)


def _post_breakout_progress_atr(last: pd.Series, level: float, direction: str) -> float:
    atr = max(float(last.get("atr", 0.0) or 0.0), 1e-9)
    close_price = float(last.get("close", 0.0) or 0.0)
    if direction == "LONG":
        return max(0.0, close_price - float(level)) / atr
    return max(0.0, float(level) - close_price) / atr


def _continuation_ok(last: pd.Series, direction: str, profile: Dict, quality: Optional[Dict[str, float]] = None) -> bool:
    """Tiered hard gate for continuation quality.

    Free keeps broad coverage but stops publishing continuation candles that show
    no real directional evidence at all. Plus requires a cleaner follow-through
    profile. Premium remains strict and demands that all continuation quality
    metrics pass.
    """
    if direction == "LONG":
        if float(last["close"]) <= float(last["open"]):
            return False
    else:
        if float(last["close"]) >= float(last["open"]):
            return False

    if float(last["body_ratio"]) < float(profile["min_body_ratio_continuation"]):
        return False

    profile_name = str(profile.get("name") or "").strip().lower()
    if quality is None:
        return profile_name != PREMIUM_PROFILE["name"]

    flags = {
        "close_position": _close_position_ratio(last, direction) >= float(profile.get("min_close_position_continuation", 0.0)),
        "relative_volume": _relative_volume_ratio(last) >= float(profile.get("min_rel_volume_continuation", 0.0)),
        "progress_atr": _post_breakout_progress_atr(
            last,
            float(quality.get("level", 0.0) or 0.0),
            direction,
        ) >= float(profile.get("min_post_breakout_progress_atr", 0.0)),
    }

    passed = sum(1 for ok in flags.values() if ok)

    if profile_name == PREMIUM_PROFILE["name"]:
        return passed == len(flags)

    if profile_name == PLUS_PROFILE["name"]:
        return passed >= 2 and (flags["close_position"] or flags["progress_atr"])

    return passed >= 1



def _breakout_score(quality: Dict[str, float], profile: Dict) -> float:
    body = quality["breakout_body_ratio"]
    min_body = float(profile["min_body_ratio_breakout"])
    body_quality = _clamp((body - min_body) / max(0.40, 1e-9), 0.0, 1.0)

    overshoot_atr = quality["overshoot_atr"]
    # Mejor cuando rompe entre 0.08 y 0.70 ATR. Exceso o falta penalizan.
    if overshoot_atr < 0.08:
        overshoot_quality = overshoot_atr / 0.08
    elif overshoot_atr <= 0.70:
        overshoot_quality = 1.0
    else:
        overshoot_quality = _clamp(1.0 - ((overshoot_atr - 0.70) / 1.20), 0.0, 1.0)

    return _clamp(((body_quality * 0.6) + (overshoot_quality * 0.4)) * 18.0, 0.0, 18.0)



def _retest_score(quality: Dict[str, float], profile: Dict) -> float:
    extension_atr = float(quality.get("extension_atr", 0.0) or 0.0)
    min_ext = float(profile.get("min_extension_atr", 0.15))
    max_ext = float(profile.get("max_extension_atr", 0.95))
    if max_ext <= min_ext:
        return 0.0

    ideal = min_ext + ((max_ext - min_ext) * 0.35)
    span = max((max_ext - min_ext) * 0.85, 1e-9)
    quality_score = _clamp(1.0 - (abs(extension_atr - ideal) / span), 0.0, 1.0)
    return quality_score * 16.0



def _continuation_score(last: pd.Series, profile: Dict, direction: str, quality: Optional[Dict[str, float]] = None) -> float:
    body = float(last["body_ratio"])
    min_body = float(profile["min_body_ratio_continuation"])
    body_quality = _clamp((body - min_body) / max(0.28, 1e-9), 0.0, 1.0)

    close_quality = _clamp(
        (_close_position_ratio(last, direction) - float(profile.get("min_close_position_continuation", 0.0))) / 0.32,
        0.0,
        1.0,
    )
    volume_quality = _clamp(
        (_relative_volume_ratio(last) - float(profile.get("min_rel_volume_continuation", 0.0))) / 0.75,
        0.0,
        1.0,
    )

    progress_quality = 0.0
    if quality is not None:
        progress_quality = _clamp(
            (_post_breakout_progress_atr(last, float(quality.get("level", 0.0) or 0.0), direction) - float(profile.get("min_post_breakout_progress_atr", 0.0))) / 0.45,
            0.0,
            1.0,
        )

    composite = (body_quality * 0.35) + (close_quality * 0.25) + (volume_quality * 0.20) + (progress_quality * 0.20)
    return _clamp(composite * 12.0, 0.0, 12.0)



def _entry_freshness_score(level: float, close_price: float, atr: float) -> float:
    if atr <= 0:
        return 0.0

    extension_atr = abs(close_price - level) / atr
    if extension_atr <= 0.18:
        quality = 0.6
    elif extension_atr <= 0.60:
        quality = 1.0
    elif extension_atr <= 0.95:
        quality = 1.0 - ((extension_atr - 0.60) / 0.35)
    else:
        quality = 0.0

    return _clamp(quality * 10.0, 0.0, 10.0)



def _reset_entry_price(level: float, last: pd.Series, direction: str) -> float:
    """
    Precio ancla del reset.

    La señal pública ya no se libera antes del reset: se activa únicamente en el
    primer toque vivo de la zona. Aun así, el modelo conserva este nivel como
    referencia estructural para construir la banda de entrada y el riesgo.
    """
    del last, direction
    return round(float(level), 8)



def _build_trade_profiles(entry_price: float, direction: str, atr_pct: float) -> Dict[str, Dict]:
    profiles: Dict[str, Dict] = {}

    for name, cfg in TRADING_PROFILES.items():
        stop_pct = _adaptive_stop_pct(atr_pct, cfg)
        tp1_rr = max(float(cfg["tp1_rr"]), 0.1)
        tp2_rr = max(float(cfg["tp2_rr"]), tp1_rr + 0.1)
        tp1_pct = stop_pct * tp1_rr
        tp2_pct = stop_pct * tp2_rr

        if direction == "LONG":
            stop_loss = _round_price_dynamic(entry_price * (1 - stop_pct))
            tp1 = _round_price_dynamic(entry_price * (1 + tp1_pct))
            tp2 = _round_price_dynamic(entry_price * (1 + tp2_pct))
        else:
            stop_loss = _round_price_dynamic(entry_price * (1 + stop_pct))
            tp1 = _round_price_dynamic(entry_price * (1 - tp1_pct))
            tp2 = _round_price_dynamic(entry_price * (1 - tp2_pct))

        profiles[name] = {
            "stop_loss": stop_loss,
            "take_profits": [tp1, tp2],
            "leverage": cfg["leverage"],
        }

    return profiles



def _build_score_components(
    df: pd.DataFrame,
    direction: str,
    score_profile: Dict,
    quality: Dict[str, float],
) -> List[Tuple[str, float]]:
    last = df.iloc[-1]

    trend_points = _trend_strength_score(last)
    adx_points = _adx_score(float(last["adx"]), float(score_profile["adx_min"]))
    atr_points = _atr_score(float(last["atr_pct"]), score_profile)
    breakout_points = _breakout_score(quality, score_profile)
    retest_points = _retest_score(quality, score_profile)
    continuation_points = _continuation_score(last, score_profile, direction, quality)
    volume_points = _volume_score(last)
    entry_points = _entry_freshness_score(
        quality["level"],
        float(quality.get("reference_price") or last["close"]),
        float(last["atr"]),
    )

    components = [
        ("trend_structure", round(trend_points, 2)),
        ("adx_strength", round(adx_points, 2)),
        ("atr_quality", round(atr_points, 2)),
        ("breakout_quality", round(breakout_points, 2)),
        ("retest_quality", round(retest_points, 2)),
        ("continuation_quality", round(continuation_points, 2)),
        ("volume_quality", round(volume_points, 2)),
        ("entry_freshness", round(entry_points, 2)),
    ]

    soft_gate_count = float(quality.get("soft_gate_count", 0.0) or 0.0)
    if soft_gate_count > 0:
        components.append(("soft_gate_penalty", round(-min(10.0, soft_gate_count * 2.0), 2)))

    return components



def _sum_components(components: List[Tuple[str, float]]) -> float:
    return round(_clamp(sum(points for _, points in components), 0.0, MAX_SCORE), 2)



def _compute_raw_score(
    df: pd.DataFrame,
    direction: str,
    profile: Dict,
    quality: Dict[str, float],
) -> Tuple[float, List[Tuple[str, float]]]:
    components = _build_score_components(df, direction, profile, quality)
    return _sum_components(components), components



def _min_raw_score_for_profile(profile_name: str) -> float:
    if profile_name == PREMIUM_PROFILE["name"]:
        return PREMIUM_RAW_SCORE_MIN
    if profile_name == PLUS_PROFILE["name"]:
        return PLUS_RAW_SCORE_MIN
    return FREE_RAW_SCORE_MIN


def _passes_profile_score_floor(result: Optional[Dict], profile_name: str) -> bool:
    if not result:
        return False
    try:
        return float(result.get("raw_score", 0.0)) >= _min_raw_score_for_profile(profile_name)
    except Exception:
        return False


def _compute_normalized_score(
    df: pd.DataFrame,
    direction: str,
    setup_group: str,
    quality: Dict[str, float],
) -> Tuple[float, List[Tuple[str, float]]]:
    """
    Produce un score comparable entre perfiles.

    Regla de calibración:
    - siempre se evalúa con el perfil estricto SHARED_PROFILE
    - si la señal viene del perfil FREE, se aplica además una penalización
      fija porque ya sabemos que falló al menos una puerta del shared

    Así evitamos comparar como equivalentes dos señales aprobadas con
    criterios distintos.
    """
    comparable_components = _build_score_components(df, direction, SHARED_PROFILE, quality)
    normalized_score = _sum_components(comparable_components)

    normalization_components = list(comparable_components)
    profile_penalty = 0.0

    if setup_group == FREE_PROFILE["name"]:
        profile_penalty = FREE_NORMALIZATION_PENALTY
        normalization_components.append(("profile_penalty", round(-profile_penalty, 2)))
        normalized_score = _clamp(normalized_score - profile_penalty, 0.0, MAX_SCORE)

    return round(normalized_score, 2), normalization_components



def _evaluate_profile(
    df: pd.DataFrame,
    profile: Dict,
    df_15m: Optional[pd.DataFrame] = None,
    df_1h: Optional[pd.DataFrame] = None,
    reference_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
    symbol: Optional[str] = None,
) -> Optional[Dict]:
    if len(df) < 3:
        _record_reject(debug_counts, 'insufficient_history')
        return None

    setup_df = df.iloc[:-1].copy()
    live_row = df.iloc[-1]
    if len(setup_df) < max(BREAKOUT_LOOKBACK + 4, 32):
        setup_df = df
        live_row = df.iloc[-1]

    last = setup_df.iloc[-1]

    if not _indicators_ready(last):
        _record_reject(debug_counts, 'indicator_warmup')
        return None

    direction = _trend_direction(last)
    if not direction:
        _record_reject(debug_counts, "trend_structure")
        return None

    higher_tf_context: Dict[str, float] = {}
    htf_soft_block_reason: Optional[str] = None
    if direction == "SHORT" and df_15m is not None and df_1h is not None:
        higher_tf_ok, higher_tf_context = _higher_tf_short_context_ok(df_15m, df_1h)
        if not higher_tf_ok:
            htf_soft_block_reason = "htf_context"
            _record_reject(debug_counts, "soft_gate_htf_context")

    if direction == "LONG" and df_15m is not None and df_1h is not None:
        higher_tf_ok, higher_tf_context = _higher_tf_long_context_ok(df_15m, df_1h)
        if not higher_tf_ok:
            htf_soft_block_reason = "htf_context_long"
            _record_reject(debug_counts, "soft_gate_htf_context_long")

    adx_value = float(last["adx"])
    if adx_value < float(BREAKOUT_RESET_HARD_ADX_MIN):
        _record_reject(debug_counts, "adx_strength_hard")
        return None
    adx_soft = adx_value < float(profile["adx_min"])
    if adx_soft:
        _record_reject(debug_counts, "soft_gate_adx_strength")

    atr_pct = float(last["atr_pct"])
    if not _atr_pct_within_hard_safety_band(atr_pct, profile):
        _record_reject(debug_counts, "atr_pct_hard")
        return None
    atr_soft = not (float(profile["atr_pct_min"]) <= atr_pct <= float(profile["atr_pct_max"]))
    if atr_soft:
        _record_reject(debug_counts, "soft_gate_atr_pct")

    setup_doc: Optional[Dict[str, Any]] = None
    quality, setup_doc = _load_stateful_setup(symbol, direction, setup_df, profile, debug_counts)

    if quality is None:
        breakout_ok, quality, breakout_reason = _confirm_breakout_prereset(
            setup_df,
            direction,
            profile,
            reference_market_price=None,
        )
        if not breakout_ok:
            _record_reject(debug_counts, breakout_reason or "breakout_retest")
            return None

        if not _continuation_hard_gate(last, direction, profile):
            _record_reject(debug_counts, "continuation_candle_hard")
            return None
        if not _continuation_ok(last, direction, profile, quality):
            _record_soft_gate(debug_counts, quality, "continuation_candle")
    else:
        _record_reject(debug_counts, "breakout_stateful_setup_loaded")

    if adx_soft:
        _record_soft_gate(debug_counts, quality, "adx_strength")
    if atr_soft:
        _record_soft_gate(debug_counts, quality, "atr_pct")
    if htf_soft_block_reason:
        _record_soft_gate(debug_counts, quality, htf_soft_block_reason)
    for q_key, q_value in list((quality or {}).items()):
        if str(q_key).startswith("soft_gate_") and float(q_value or 0.0) > 0:
            _record_reject(debug_counts, str(q_key))

    level = float(quality["level"])
    close_price = float(quality.get("reference_price") or last["close"])

    live_price = float(reference_market_price or float(live_row.get("close", close_price)) or close_price or level)
    live_high = float(live_row.get("high", live_price) or live_price)
    live_low = float(live_row.get("low", live_price) or live_price)
    selected_reset, live_stage = _select_live_reset_model(
        quality=quality,
        last=last,
        direction=direction,
        atr=float(last["atr"]),
        atr_pct=atr_pct,
        live_price=live_price,
        live_high=live_high,
        live_low=live_low,
        debug_counts=debug_counts,
    )

    if selected_reset is None:
        reject_reason = "breakout_waiting_live_reset" if live_stage == SETUP_STAGE_PRE_RESET_WAITING_RETEST else (
            "breakout_reset_rebounded_before_publish" if live_stage == SETUP_STAGE_RESET_REBOUNDED_BEFORE_PUBLISH else "breakout_reset_late"
        )
        # Persist with the classic level band as the structural waiting anchor.
        # Multi-reset publication models are recomputed live on every cycle so
        # stored setups do not become stale when EMA20 or extension changes.
        model_entry_price = _reset_entry_price(level, last, direction)
        model_trade_profiles = _build_trade_profiles(model_entry_price, direction, atr_pct)
        model_conservative = model_trade_profiles.get("conservador") or {}
        model_stop_loss = float(model_conservative.get("stop_loss") or 0.0)
        if model_stop_loss <= 0:
            _record_reject(debug_counts, "breakout_trade_profile")
            return None
        base_zone_low, base_zone_high = _calculate_entry_zone(model_entry_price, model_stop_loss)
        zone_low, zone_high = _expand_reset_zone_with_atr(base_zone_low, base_zone_high, float(last["atr"]))
        if live_stage == SETUP_STAGE_PRE_RESET_WAITING_RETEST:
            _persist_waiting_setup(
                symbol=symbol,
                direction=direction,
                profile=profile,
                quality=quality,
                entry_price=model_entry_price,
                zone_low=zone_low,
                zone_high=zone_high,
                atr=float(last["atr"]),
                atr_pct=atr_pct,
                df=setup_df,
            )
            _record_reject(debug_counts, "breakout_setup_armed_waiting_reset")
        elif setup_doc is not None:
            breakout_reset_setup_store.mark_setup_status(str(setup_doc.get("setup_id") or ""), "invalidated", reject_reason)
        _record_reject(debug_counts, reject_reason)
        return None

    model_entry_price = float(selected_reset["entry_model_price"])
    zone_low = float(selected_reset["zone_low"])
    zone_high = float(selected_reset["zone_high"])
    base_zone_low = float(selected_reset["base_zone_low"])
    base_zone_high = float(selected_reset["base_zone_high"])
    reset_model = str(selected_reset.get("reset_model") or "level")
    entry_price = _round_price_dynamic(_clamp(live_price, zone_low, zone_high))
    trade_profiles = _build_trade_profiles(entry_price, direction, atr_pct)

    timing_ok, timing_reason, timing_diag = _publication_timing_guard(
        direction=direction,
        live_price=live_price,
        live_high=live_high,
        live_low=live_low,
        entry_model_price=model_entry_price,
        entry_price=entry_price,
        atr=float(last["atr"]),
        trade_profiles=trade_profiles,
        debug_counts=debug_counts,
    )
    if not timing_ok:
        _record_reject(debug_counts, "publication_timing_rejected")
        if setup_doc is not None:
            # Do not mark the setup invalidated: a late/unsafe touch is not the
            # same as structural failure. Keep waiting until TTL unless price
            # actually breaks the setup rules.
            _record_reject(debug_counts, "breakout_setup_kept_after_timing_reject")
        return None

    quality["reset_model_" + reset_model] = 1.0
    quality["reset_model_used"] = 1.0

    if setup_doc is None:
        setup_doc = _persist_waiting_setup(
            symbol=symbol,
            direction=direction,
            profile=profile,
            quality=quality,
            entry_price=model_entry_price,
            zone_low=zone_low,
            zone_high=zone_high,
            atr=float(last["atr"]),
            atr_pct=atr_pct,
            df=setup_df,
        )

    raw_score, raw_components = _compute_raw_score(setup_df, direction, profile, quality)
    normalized_score, normalized_components = _compute_normalized_score(
        df=setup_df,
        direction=direction,
        setup_group=str(profile["name"]),
        quality=quality,
    )

    return {
        "direction": direction,
        "entry_price": round(float(entry_price), 4),
        "raw_score": raw_score,
        "score": normalized_score,
        "normalized_score": normalized_score,
        "raw_components": raw_components,
        "normalized_components": normalized_components,
        "components": normalized_components,
        "trade_profiles": trade_profiles,
        "setup_group": str(profile["name"]),
        "atr_pct": round(atr_pct, 6),
        "score_profile": str(profile["name"]),
        "score_calibration": SCORE_CALIBRATION_VERSION,
        "higher_tf_context": higher_tf_context,
        "send_mode": "market_on_close",
        "setup_stage": SETUP_STAGE_RESET_TOUCH_LIVE,
        "entry_model": ENTRY_MODEL_NAME,
        "entry_model_price": round(float(model_entry_price), 8),
        "entry_sent_price": round(float(entry_price), 8),
        "reset_zone_low": round(float(zone_low), 8),
        "reset_zone_high": round(float(zone_high), 8),
        "reset_base_zone_low": round(float(base_zone_low), 8),
        "reset_base_zone_high": round(float(base_zone_high), 8),
        "reset_zone_padding_atr": round(float(BREAKOUT_RESET_RESET_ZONE_PADDING_ATR if BREAKOUT_RESET_ADAPTIVE_RESET_ZONE_ENABLED else 0.0), 4),
        "reset_model": reset_model,
        "publication_timing": timing_diag,
        "reset_level": round(float(level), 8),
        "reset_close_price": round(float(close_price), 8),
        "signal_reference_price": round(float(live_price), 8),
        "stateful_setup_id": str((setup_doc or {}).get("setup_id") or quality.get("setup_id") or ""),
        "stateful_setup_status": "published",
        "stateful_setup_age_bars": round(float(quality.get("setup_age_bars", quality.get("breakout_age_bars", 0.0)) or 0.0), 2),
    }




def _mark_result_published(result: Optional[Dict], profile_name: str) -> None:
    if not result:
        return
    setup_id = str(result.get("stateful_setup_id") or "").strip()
    if not setup_id:
        return
    breakout_reset_setup_store.mark_setup_status(
        setup_id,
        "published",
        "reset_touched",
        extra={"published_at": _utcnow_naive(), "published_profile": str(profile_name or "unknown")},
    )

# =======================================
# ESTRATEGIA 5M
# =======================================


def mtf_strategy(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: pd.DataFrame,
    reference_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
    symbol: Optional[str] = None,
) -> Optional[Dict]:
    # Mantenemos la firma para no romper el scanner actual.
    # La lógica operativa final vive en 5M.
    required_bars = _required_history_bars()

    if len(df_5m) < required_bars:
        _record_reject(debug_counts, "insufficient_history")
        return None

    df = add_indicators(df_5m)

    if len(df) < required_bars:
        _record_reject(debug_counts, "insufficient_history")
        return None

    last = df.iloc[-1]
    if not _indicators_ready(last):
        _record_reject(debug_counts, 'indicator_warmup')
        return None

    # 1) PREMIUM primero: misma estrategia, pero con puertas algo más altas que PLUS.
    premium_result = _evaluate_profile(df, PREMIUM_PROFILE, df_15m=df_15m, df_1h=df_1h, reference_market_price=reference_market_price, debug_counts=debug_counts, symbol=symbol)
    if premium_result and _passes_profile_score_floor(premium_result, PREMIUM_PROFILE["name"]):
        _mark_result_published(premium_result, PREMIUM_PROFILE["name"])
        return {
            "direction": premium_result["direction"],
            "entry_price": premium_result["entry_price"],
            "stop_loss": premium_result["trade_profiles"]["conservador"]["stop_loss"],
            "take_profits": list(premium_result["trade_profiles"]["conservador"]["take_profits"]),
            "profiles": premium_result["trade_profiles"],
            "score": premium_result["score"],
            "raw_score": premium_result["raw_score"],
            "normalized_score": premium_result["normalized_score"],
            "components": premium_result["components"],
            "raw_components": premium_result["raw_components"],
            "normalized_components": premium_result["normalized_components"],
            "timeframes": ["5M"],
            "setup_group": "premium",
            "atr_pct": premium_result["atr_pct"],
            "score_profile": "premium",
            "score_calibration": premium_result["score_calibration"],
            "higher_tf_context": premium_result["higher_tf_context"],
            "send_mode": premium_result["send_mode"],
            "setup_stage": premium_result["setup_stage"],
            "entry_model": premium_result["entry_model"],
            "entry_model_price": premium_result["entry_model_price"],
            "reset_level": premium_result["reset_level"],
            "reset_close_price": premium_result["reset_close_price"],
            "signal_reference_price": premium_result.get("signal_reference_price"),
            "stateful_setup_id": premium_result.get("stateful_setup_id"),
            "stateful_setup_status": premium_result.get("stateful_setup_status"),
            "stateful_setup_age_bars": premium_result.get("stateful_setup_age_bars"),
        }

    if premium_result:
        _record_reject(debug_counts, "score_floor_premium")

    # 2) PLUS después: sigue siendo setup bueno, pero algo menos exigente que PREMIUM.
    plus_result = _evaluate_profile(df, PLUS_PROFILE, df_15m=df_15m, df_1h=df_1h, reference_market_price=reference_market_price, debug_counts=debug_counts, symbol=symbol)
    if plus_result and _passes_profile_score_floor(plus_result, PLUS_PROFILE["name"]):
        _mark_result_published(plus_result, PLUS_PROFILE["name"])
        return {
            "direction": plus_result["direction"],
            "entry_price": plus_result["entry_price"],
            "stop_loss": plus_result["trade_profiles"]["conservador"]["stop_loss"],
            "take_profits": list(plus_result["trade_profiles"]["conservador"]["take_profits"]),
            "profiles": plus_result["trade_profiles"],
            "score": plus_result["score"],
            "raw_score": plus_result["raw_score"],
            "normalized_score": plus_result["normalized_score"],
            "components": plus_result["components"],
            "raw_components": plus_result["raw_components"],
            "normalized_components": plus_result["normalized_components"],
            "timeframes": ["5M"],
            "setup_group": "plus",
            "atr_pct": plus_result["atr_pct"],
            "score_profile": "plus",
            "score_calibration": plus_result["score_calibration"],
            "higher_tf_context": plus_result["higher_tf_context"],
            "send_mode": plus_result["send_mode"],
            "setup_stage": plus_result["setup_stage"],
            "entry_model": plus_result["entry_model"],
            "entry_model_price": plus_result["entry_model_price"],
            "reset_level": plus_result["reset_level"],
            "reset_close_price": plus_result["reset_close_price"],
            "signal_reference_price": plus_result.get("signal_reference_price"),
            "stateful_setup_id": plus_result.get("stateful_setup_id"),
            "stateful_setup_status": plus_result.get("stateful_setup_status"),
            "stateful_setup_age_bars": plus_result.get("stateful_setup_age_bars"),
        }

    if plus_result:
        _record_reject(debug_counts, "score_floor_plus")

    # 3) Si no pasa premium/plus, intenta el perfil flexible de FREE.
    free_result = _evaluate_profile(df, FREE_PROFILE, df_15m=df_15m, df_1h=df_1h, reference_market_price=reference_market_price, debug_counts=debug_counts, symbol=symbol)
    if free_result and _passes_profile_score_floor(free_result, FREE_PROFILE["name"]):
        _mark_result_published(free_result, FREE_PROFILE["name"])
        return {
            "direction": free_result["direction"],
            "entry_price": free_result["entry_price"],
            "stop_loss": free_result["trade_profiles"]["conservador"]["stop_loss"],
            "take_profits": list(free_result["trade_profiles"]["conservador"]["take_profits"]),
            "profiles": free_result["trade_profiles"],
            "score": free_result["score"],
            "raw_score": free_result["raw_score"],
            "normalized_score": free_result["normalized_score"],
            "components": free_result["components"],
            "raw_components": free_result["raw_components"],
            "normalized_components": free_result["normalized_components"],
            "timeframes": ["5M"],
            "setup_group": "free",
            "atr_pct": free_result["atr_pct"],
            "score_profile": "free",
            "score_calibration": free_result["score_calibration"],
            "higher_tf_context": free_result["higher_tf_context"],
            "send_mode": free_result["send_mode"],
            "setup_stage": free_result["setup_stage"],
            "entry_model": free_result["entry_model"],
            "entry_model_price": free_result["entry_model_price"],
            "reset_level": free_result["reset_level"],
            "reset_close_price": free_result["reset_close_price"],
            "signal_reference_price": free_result.get("signal_reference_price"),
            "stateful_setup_id": free_result.get("stateful_setup_id"),
            "stateful_setup_status": free_result.get("stateful_setup_status"),
            "stateful_setup_age_bars": free_result.get("stateful_setup_age_bars"),
        }

    if free_result:
        _record_reject(debug_counts, "score_floor_free")

    return None
