from __future__ import annotations

from typing import Optional, Dict, Tuple, List

import math
import os
import pandas as pd

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
FREE_NORMALIZATION_PENALTY = 6.0
SCORE_CALIBRATION_VERSION = "v7_breakout_reset_live_reset_touch_release"
ENTRY_MODEL_NAME = "breakout_reset_live_reset_touch_v1"
SETUP_STAGE_PRE_RESET_WAITING_RETEST = "pre_reset_waiting_retest"
SETUP_STAGE_RESET_TOUCH_LIVE = "reset_touch_live"
SEND_MODE_PENDING_ENTRY = "entry_zone_pending"
ENTRY_ZONE_MIN_PCT = float(os.getenv("ENTRY_ZONE_MIN_PCT", "0.0015"))
ENTRY_ZONE_MAX_PCT = float(os.getenv("ENTRY_ZONE_MAX_PCT", "0.0035"))
ENTRY_ZONE_RISK_FRACTION = float(os.getenv("ENTRY_ZONE_RISK_FRACTION", "0.22"))
PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "83"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "76"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "69"))


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
    "adx_min": _env_float("PLUS_ADX_MIN", 18.9),
    "atr_pct_min": _env_float("PLUS_ATR_PCT_MIN", 0.0028),
    "atr_pct_max": _env_float("PLUS_ATR_PCT_MAX", 0.0116),
    "min_body_ratio_breakout": _env_float("PLUS_MIN_BODY_RATIO_BREAKOUT", 0.35),
    "min_body_ratio_continuation": _env_float("PLUS_MIN_BODY_RATIO_CONTINUATION", 0.24),
    "min_extension_atr": _env_float("PLUS_MIN_EXTENSION_ATR", 0.22),
    "max_extension_atr": _env_float("PLUS_MAX_EXTENSION_ATR", 0.78),
    "min_rel_volume_continuation": _env_float("PLUS_MIN_REL_VOLUME_CONTINUATION", 1.05),
    "min_close_position_continuation": _env_float("PLUS_MIN_CLOSE_POSITION_CONTINUATION", 0.61),
    "min_post_breakout_progress_atr": _env_float("PLUS_MIN_POST_BREAKOUT_PROGRESS_ATR", 0.06),
}

FREE_PROFILE = {
    "name": "free",
    "adx_min": _env_float("FREE_ADX_MIN", 17.2),
    "atr_pct_min": _env_float("FREE_ATR_PCT_MIN", 0.0025),
    "atr_pct_max": _env_float("FREE_ATR_PCT_MAX", 0.0128),
    "min_body_ratio_breakout": _env_float("FREE_MIN_BODY_RATIO_BREAKOUT", 0.30),
    "min_body_ratio_continuation": _env_float("FREE_MIN_BODY_RATIO_CONTINUATION", 0.20),
    "min_extension_atr": _env_float("FREE_MIN_EXTENSION_ATR", 0.18),
    "max_extension_atr": _env_float("FREE_MAX_EXTENSION_ATR", 0.86),
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
    "adx_min": _env_float("PREMIUM_ADX_MIN", 19.6),
    "atr_pct_min": _env_float("PREMIUM_ATR_PCT_MIN", 0.0030),
    "atr_pct_max": _env_float("PREMIUM_ATR_PCT_MAX", 0.0110),
    "min_body_ratio_breakout": _env_float("PREMIUM_MIN_BODY_RATIO_BREAKOUT", 0.38),
    "min_body_ratio_continuation": _env_float("PREMIUM_MIN_BODY_RATIO_CONTINUATION", 0.27),
    "min_extension_atr": _env_float("PREMIUM_MIN_EXTENSION_ATR", 0.26),
    "max_extension_atr": _env_float("PREMIUM_MAX_EXTENSION_ATR", 0.70),
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


def _classify_live_reset_state(direction: str, current_price: float, zone_low: float, zone_high: float) -> str:
    direction = str(direction).upper().strip()
    current_price = float(current_price)
    if direction == "LONG":
        if current_price > zone_high:
            return SETUP_STAGE_PRE_RESET_WAITING_RETEST
        if zone_low <= current_price <= zone_high:
            return SETUP_STAGE_RESET_TOUCH_LIVE
        return "reset_late_or_lost"

    if current_price < zone_low:
        return SETUP_STAGE_PRE_RESET_WAITING_RETEST
    if zone_low <= current_price <= zone_high:
        return SETUP_STAGE_RESET_TOUCH_LIVE
    return "reset_late_or_lost"


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

    # Regla original estricta primero.
    if ema20 > ema50 > ema200:
        return "LONG"
    if ema20 < ema50 < ema200:
        return "SHORT"

    # Relajación mínima del filtro de estructura:
    # seguimos exigiendo alineación EMA20/EMA50, pero permitimos que EMA50 y EMA200
    # todavía estén muy cerca siempre que el precio ya esté del lado correcto de EMA200.
    if ema20 > ema50 and close > ema200:
        return "LONG"
    if ema20 < ema50 and close < ema200:
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



def _confirm_breakout_prereset(
    df: pd.DataFrame,
    direction: str,
    profile: Dict,
    reference_market_price: Optional[float],
) -> Tuple[bool, Dict[str, float]]:
    """
    Detecta un breakout ya confirmado y una extensión suficiente para ANTICIPAR
    el reset futuro.

    No buscamos la vela que ya hizo el retest. Buscamos exactamente lo contrario:
    una estructura que ya rompió y se alejó del nivel, pero que todavía no ha vuelto
    a tocar la zona donde esperamos el reset.
    """
    last = df.iloc[-1]
    prev = df.iloc[-2]

    level = breakout_level(df, direction)
    atr = float(last["atr"])
    setup_reference_price = float(last["close"] or 0.0)
    current_price = float(reference_market_price or setup_reference_price or 0.0)

    if atr <= 0 or current_price <= 0 or setup_reference_price <= 0:
        return False, {}

    min_ext = float(profile.get("min_extension_atr", 0.15))
    max_ext = float(profile.get("max_extension_atr", 0.95))

    if direction == "LONG":
        breakout_ok = (
            float(prev["close"]) > level
            and float(prev["high"]) > level
            and float(prev["body_ratio"]) >= float(profile["min_body_ratio_breakout"])
        )
        continuation_ok = (
            float(last["close"]) > float(last["open"])
            and float(last["close"]) > level
            and float(last["body_ratio"]) >= float(profile["min_body_ratio_continuation"])
        )
        no_reset_yet = float(last["low"]) > level
        extension_atr = max(0.0, setup_reference_price - level) / atr
        overshoot_atr = max(0.0, float(prev["close"]) - level) / atr
    else:
        breakout_ok = (
            float(prev["close"]) < level
            and float(prev["low"]) < level
            and float(prev["body_ratio"]) >= float(profile["min_body_ratio_breakout"])
        )
        continuation_ok = (
            float(last["close"]) < float(last["open"])
            and float(last["close"]) < level
            and float(last["body_ratio"]) >= float(profile["min_body_ratio_continuation"])
        )
        no_reset_yet = float(last["high"]) < level
        extension_atr = max(0.0, level - setup_reference_price) / atr
        overshoot_atr = max(0.0, level - float(prev["close"])) / atr

    if not breakout_ok or not continuation_ok or not no_reset_yet:
        return False, {}

    if extension_atr < min_ext or extension_atr > max_ext:
        return False, {}

    quality = {
        "level": float(level),
        "breakout_body_ratio": float(prev["body_ratio"]),
        "continuation_body_ratio": float(last["body_ratio"]),
        "extension_atr": float(extension_atr),
        "overshoot_atr": float(overshoot_atr),
        "reference_price": float(setup_reference_price),
        "pre_reset_space_atr": float(abs(float(last["low"]) - level) / atr) if direction == "LONG" else float(abs(level - float(last["high"])) / atr),
    }
    return True, quality



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
    Modelo predictivo de entrada: la señal se envía ANTES del reset, así que la
    entrada debe permanecer anclada al nivel reclamado, no al precio actual.
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

    return [
        ("trend_structure", round(trend_points, 2)),
        ("adx_strength", round(adx_points, 2)),
        ("atr_quality", round(atr_points, 2)),
        ("breakout_quality", round(breakout_points, 2)),
        ("retest_quality", round(retest_points, 2)),
        ("continuation_quality", round(continuation_points, 2)),
        ("volume_quality", round(volume_points, 2)),
        ("entry_freshness", round(entry_points, 2)),
    ]



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
) -> Optional[Dict]:
    last = df.iloc[-1]

    if not _indicators_ready(last):
        _record_reject(debug_counts, 'indicator_warmup')
        return None

    direction = _trend_direction(last)
    if not direction:
        _record_reject(debug_counts, "trend_structure")
        return None

    higher_tf_context: Dict[str, float] = {}
    if direction == "SHORT" and df_15m is not None and df_1h is not None:
        higher_tf_ok, higher_tf_context = _higher_tf_short_context_ok(df_15m, df_1h)
        if not higher_tf_ok:
            _record_reject(debug_counts, "htf_context")
            return None

    adx_value = float(last["adx"])
    if adx_value < float(profile["adx_min"]):
        _record_reject(debug_counts, "adx_strength")
        return None

    atr_pct = float(last["atr_pct"])
    if not (float(profile["atr_pct_min"]) <= atr_pct <= float(profile["atr_pct_max"])):
        _record_reject(debug_counts, "atr_pct")
        return None

    breakout_ok, quality = _confirm_breakout_prereset(
        df,
        direction,
        profile,
        reference_market_price=reference_market_price,
    )
    if not breakout_ok:
        _record_reject(debug_counts, "breakout_retest")
        return None

    if not _continuation_ok(last, direction, profile, quality):
        _record_reject(debug_counts, "continuation_candle")
        return None

    level = float(quality["level"])
    close_price = float(quality.get("reference_price") or last["close"])

    model_entry_price = _reset_entry_price(level, last, direction)
    model_trade_profiles = _build_trade_profiles(model_entry_price, direction, atr_pct)
    model_conservative = model_trade_profiles.get("conservador") or {}
    model_stop_loss = float(model_conservative.get("stop_loss") or 0.0)
    if model_stop_loss <= 0:
        _record_reject(debug_counts, "breakout_trade_profile")
        return None

    zone_low, zone_high = _calculate_entry_zone(model_entry_price, model_stop_loss)
    live_price = float(reference_market_price or close_price or model_entry_price)
    live_stage = _classify_live_reset_state(direction, live_price, zone_low, zone_high)
    if live_stage != SETUP_STAGE_RESET_TOUCH_LIVE:
        _record_reject(debug_counts, "breakout_waiting_live_reset" if live_stage == SETUP_STAGE_PRE_RESET_WAITING_RETEST else "breakout_reset_late")
        return None

    entry_price = _round_price_dynamic(live_price)
    trade_profiles = _build_trade_profiles(entry_price, direction, atr_pct)

    raw_score, raw_components = _compute_raw_score(df, direction, profile, quality)
    normalized_score, normalized_components = _compute_normalized_score(
        df=df,
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
        "reset_level": round(float(level), 8),
        "reset_close_price": round(float(close_price), 8),
        "signal_reference_price": round(float(close_price), 8),
    }


# =======================================
# ESTRATEGIA 5M
# =======================================


def mtf_strategy(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: pd.DataFrame,
    reference_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
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
    premium_result = _evaluate_profile(df, PREMIUM_PROFILE, df_15m=df_15m, df_1h=df_1h, reference_market_price=reference_market_price, debug_counts=debug_counts)
    if premium_result and _passes_profile_score_floor(premium_result, PREMIUM_PROFILE["name"]):
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
        }

    # 2) PLUS después: sigue siendo setup bueno, pero algo menos exigente que PREMIUM.
    plus_result = _evaluate_profile(df, PLUS_PROFILE, df_15m=df_15m, df_1h=df_1h, reference_market_price=reference_market_price, debug_counts=debug_counts)
    if plus_result and _passes_profile_score_floor(plus_result, PLUS_PROFILE["name"]):
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
        }

    # 3) Si no pasa premium/plus, intenta el perfil flexible de FREE.
    free_result = _evaluate_profile(df, FREE_PROFILE, df_15m=df_15m, df_1h=df_1h, reference_market_price=reference_market_price, debug_counts=debug_counts)
    if free_result and _passes_profile_score_floor(free_result, FREE_PROFILE["name"]):
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
        }

    return None
