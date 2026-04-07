from __future__ import annotations

from typing import Optional, Dict, Tuple, List

import math
import pandas as pd
import ta

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
SCORE_CALIBRATION_VERSION = "v2_2_breakout_reset_pending_entry"
ENTRY_MODEL_NAME = "breakout_reset_retest_pending_v1"
SETUP_STAGE_RESET_CONFIRMED_WAITING_ENTRY = "reset_confirmed_waiting_entry"
SEND_MODE_PENDING_ENTRY = "entry_zone_pending"


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
    "adx_min": 17.8,
    "atr_pct_min": 0.0026,
    "atr_pct_max": 0.0121,
    "retest_tol_atr": 0.44,
    "min_body_ratio_breakout": 0.32,
    "min_body_ratio_continuation": 0.24,
}

FREE_PROFILE = {
    "name": "free",
    "adx_min": 16.0,
    "atr_pct_min": 0.0022,
    "atr_pct_max": 0.0136,
    "retest_tol_atr": 0.58,
    "min_body_ratio_breakout": 0.24,
    "min_body_ratio_continuation": 0.18,
    "score": 76.0,
}

PLUS_PROFILE = {
    **SHARED_PROFILE,
    "name": "plus",
    "score": 84.0,
}

PREMIUM_PROFILE = {
    **SHARED_PROFILE,
    "name": "premium",
    "adx_min": 18.8,
    "atr_pct_min": 0.0028,
    "atr_pct_max": 0.0116,
    "retest_tol_atr": 0.40,
    "min_body_ratio_breakout": 0.35,
    "min_body_ratio_continuation": 0.26,
    "score": 90.0,
}

# =======================================
# PERFILES DE TRADING POR APALANCAMIENTO
# =======================================

TRADING_PROFILES = {
    "conservador": {
        "leverage": "20x-30x",
        "sl_pct": 0.0080,
        "tp1_pct": 0.0090,
        "tp2_pct": 0.0160,
    },
    "moderado": {
        "leverage": "30x-40x",
        "sl_pct": 0.0068,
        "tp1_pct": 0.0080,
        "tp2_pct": 0.0140,
    },
    "agresivo": {
        "leverage": "40x-50x",
        "sl_pct": 0.0058,
        "tp1_pct": 0.0070,
        "tp2_pct": 0.0120,
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



def _confirm_breakout_retest(df: pd.DataFrame, direction: str, profile: Dict) -> Tuple[bool, Dict[str, float]]:
    last = df.iloc[-1]
    prev = df.iloc[-2]

    level = breakout_level(df, direction)
    atr = float(last["atr"])
    tol_atr = float(profile["retest_tol_atr"])

    if atr <= 0:
        return False, {}

    if direction == "LONG":
        breakout_ok = (
            float(prev["close"]) > level
            and float(prev["high"]) > level
            and float(prev["body_ratio"]) >= float(profile["min_body_ratio_breakout"])
        )
        retest_distance = max(0.0, float(last["low"]) - level)
        retest_ok = (
            float(last["low"]) <= level + (atr * tol_atr)
            and float(last["close"]) >= level
        )
        overshoot = max(0.0, float(prev["close"]) - level)
    else:
        breakout_ok = (
            float(prev["close"]) < level
            and float(prev["low"]) < level
            and float(prev["body_ratio"]) >= float(profile["min_body_ratio_breakout"])
        )
        retest_distance = max(0.0, level - float(last["high"]))
        retest_ok = (
            float(last["high"]) >= level - (atr * tol_atr)
            and float(last["close"]) <= level
        )
        overshoot = max(0.0, level - float(prev["close"]))

    if not breakout_ok or not retest_ok:
        return False, {}

    overshoot_atr = overshoot / atr if atr > 0 else 0.0
    retest_distance_atr = abs(retest_distance) / atr if atr > 0 else 0.0

    quality = {
        "level": float(level),
        "breakout_body_ratio": float(prev["body_ratio"]),
        "continuation_body_ratio": float(last["body_ratio"]),
        "overshoot_atr": float(overshoot_atr),
        "retest_distance_atr": float(retest_distance_atr),
    }
    return True, quality



def _continuation_ok(last: pd.Series, direction: str, profile: Dict) -> bool:
    if direction == "LONG":
        if float(last["close"]) <= float(last["open"]):
            return False
    else:
        if float(last["close"]) >= float(last["open"]):
            return False

    if float(last["body_ratio"]) < float(profile["min_body_ratio_continuation"]):
        return False

    return True



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
    retest_dist = quality["retest_distance_atr"]
    tol = float(profile["retest_tol_atr"])
    retest_quality = _clamp(1.0 - (retest_dist / max(tol, 1e-9)), 0.0, 1.0)
    return retest_quality * 16.0



def _continuation_score(last: pd.Series, profile: Dict) -> float:
    body = float(last["body_ratio"])
    min_body = float(profile["min_body_ratio_continuation"])
    body_quality = _clamp((body - min_body) / max(0.35, 1e-9), 0.0, 1.0)
    return body_quality * 10.0



def _entry_freshness_score(level: float, close_price: float, atr: float) -> float:
    if atr <= 0:
        return 0.0

    extension_atr = abs(close_price - level) / atr

    # Cerca del nivel = más fresco. Muy extendido penaliza.
    if extension_atr <= 0.25:
        quality = 1.0
    elif extension_atr <= 0.90:
        quality = 1.0 - ((extension_atr - 0.25) / 0.65)
    else:
        quality = 0.0

    return _clamp(quality * 10.0, 0.0, 10.0)



def _reset_entry_price(level: float, last: pd.Series, direction: str) -> float:
    """
    Entry model for breakout + reset.

    We send the alert after the reset candle is confirmed, but the operational
    entry must stay close to the reclaimed breakout level. That keeps the signal
    consistent with a retest entry instead of chasing the close.
    """
    close_price = float(last["close"])
    atr = max(float(last["atr"]), 1e-9)

    if direction == "LONG":
        reset_extreme = float(last["low"])
        reclaim_distance = max(0.0, reset_extreme - level)
        entry = level + min(reclaim_distance * 0.35, atr * 0.18)
        entry = min(entry, close_price)
    else:
        reset_extreme = float(last["high"])
        reclaim_distance = max(0.0, level - reset_extreme)
        entry = level - min(reclaim_distance * 0.35, atr * 0.18)
        entry = max(entry, close_price)

    return round(float(entry), 8)



def _build_trade_profiles(entry_price: float, direction: str) -> Dict[str, Dict]:
    profiles: Dict[str, Dict] = {}

    for name, cfg in TRADING_PROFILES.items():
        sl_pct = float(cfg["sl_pct"])
        tp1_pct = float(cfg["tp1_pct"])
        tp2_pct = float(cfg["tp2_pct"])

        if direction == "LONG":
            stop_loss = round(entry_price * (1 - sl_pct), 4)
            tp1 = round(entry_price * (1 + tp1_pct), 4)
            tp2 = round(entry_price * (1 + tp2_pct), 4)
        else:
            stop_loss = round(entry_price * (1 + sl_pct), 4)
            tp1 = round(entry_price * (1 - tp1_pct), 4)
            tp2 = round(entry_price * (1 - tp2_pct), 4)

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
    continuation_points = _continuation_score(last, score_profile)
    volume_points = _volume_score(last)
    entry_points = _entry_freshness_score(
        quality["level"],
        float(last["close"]),
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

    breakout_ok, quality = _confirm_breakout_retest(df, direction, profile)
    if not breakout_ok:
        _record_reject(debug_counts, "breakout_retest")
        return None

    if not _continuation_ok(last, direction, profile):
        _record_reject(debug_counts, "continuation_candle")
        return None

    level = float(quality["level"])
    close_price = float(last["close"])

    # Breakout + reset real: la entrada debe quedar anclada al nivel reclamado,
    # no perseguir el cierre de confirmación.
    entry_price = _reset_entry_price(level, last, direction)
    trade_profiles = _build_trade_profiles(entry_price, direction)

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
        "send_mode": SEND_MODE_PENDING_ENTRY,
        "setup_stage": SETUP_STAGE_RESET_CONFIRMED_WAITING_ENTRY,
        "entry_model": ENTRY_MODEL_NAME,
        "entry_model_price": round(float(entry_price), 8),
        "reset_level": round(float(level), 8),
        "reset_close_price": round(float(close_price), 8),
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
    del reference_market_price  # Adaptación mínima: el scanner puede pasarlo, esta estrategia no lo usa.

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
    premium_result = _evaluate_profile(df, PREMIUM_PROFILE, df_15m=df_15m, df_1h=df_1h, debug_counts=debug_counts)
    if premium_result:
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
        }

    # 2) PLUS después: sigue siendo setup bueno, pero algo menos exigente que PREMIUM.
    plus_result = _evaluate_profile(df, PLUS_PROFILE, df_15m=df_15m, df_1h=df_1h, debug_counts=debug_counts)
    if plus_result:
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
        }

    # 3) Si no pasa premium/plus, intenta el perfil flexible de FREE.
    free_result = _evaluate_profile(df, FREE_PROFILE, df_15m=df_15m, df_1h=df_1h, debug_counts=debug_counts)
    if free_result:
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
        }

    return None
