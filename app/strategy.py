import pandas as pd
from typing import Optional, Dict, Tuple, List
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
SCORE_CALIBRATION_VERSION = "v2_strict_shared_normalization"

# =======================================
# PERFILES DE VALIDACIÓN
# =======================================
# SHARED_PROFILE:
#   setup bueno, usado para PLUS y PREMIUM.
# FREE_PROFILE:
#   setup más flexible, usado como capa adicional para FREE.

SHARED_PROFILE = {
    "name": "shared",
    "adx_min": 17.0,
    "atr_pct_min": 0.0024,
    "atr_pct_max": 0.0125,
    "retest_tol_atr": 0.48,
    "min_body_ratio_breakout": 0.30,
    "min_body_ratio_continuation": 0.22,
    "min_rel_volume": 1.00,
    "max_close_extension_atr": 0.96,
    # SHORT necesita más intención real porque hoy concentra demasiadas
    # expiradas y demasiado follow-through flojo.
    "short_min_body_ratio_breakout": 0.34,
    "short_min_body_ratio_continuation": 0.27,
    "short_min_rel_volume": 1.16,
    "short_max_close_extension_atr": 0.76,
    # Entrada adaptativa: si el retest queda superficial y la continuación
    # sale con desplazamiento, acercamos la entrada al precio actual para no
    # dejar demasiadas señales sin fill por retroceso flojo.
    "entry_blend_min": 0.28,
    "entry_blend_max": 0.74,
    "entry_blend_neutral_atr": 0.18,
    "entry_blend_ramp_atr": 0.92,
    "short_entry_blend_min": 0.36,
    "short_entry_blend_max": 0.82,
}

FREE_PROFILE = {
    "name": "free",
    "adx_min": 15.0,
    "atr_pct_min": 0.0020,
    "atr_pct_max": 0.0140,
    "retest_tol_atr": 0.62,
    "min_body_ratio_breakout": 0.22,
    "min_body_ratio_continuation": 0.16,
    "min_rel_volume": 0.95,
    "max_close_extension_atr": 1.02,
    "short_min_body_ratio_breakout": 0.26,
    "short_min_body_ratio_continuation": 0.20,
    "short_min_rel_volume": 1.08,
    "short_max_close_extension_atr": 0.72,
    "entry_blend_min": 0.24,
    "entry_blend_max": 0.68,
    "entry_blend_neutral_atr": 0.22,
    "entry_blend_ramp_atr": 1.05,
    "short_entry_blend_min": 0.32,
    "short_entry_blend_max": 0.76,
}

# =======================================
# ESTRATEGIA PREMIUM — LIQUIDITY SWEEP REVERSAL
# Usa la calibración del perfil PLUS del bot alternativo, pero vive solo
# en el tier Premium del bot principal.
# =======================================

PREMIUM_LSR_STRATEGY_NAME = "LIQUIDITY_SWEEP_REVERSAL"
PREMIUM_LSR_SCORE_CALIBRATION_VERSION = "v1_premium_lsr_plus_profile_live_guard"
PREMIUM_LSR_LIQUIDITY_LOOKBACK = 36
PREMIUM_LSR_TARGET_LOOKBACK = 30
PREMIUM_LSR_HTF_LOOKBACK = 48
PREMIUM_LSR_PIVOT_WINDOW = 3
PREMIUM_LSR_MIN_HISTORY_BARS = max(PREMIUM_LSR_LIQUIDITY_LOOKBACK + 8, ATR_PERIOD + 20 + 8)

PREMIUM_LSR_PROFILE = {
    "name": "premium",
    "score": 82.0,
    "atr_pct_min": 0.0018,
    "atr_pct_max": 0.0145,
    "liquidity_tolerance_atr": 0.27,
    "min_sweep_atr": 0.10,
    "min_rel_volume": 1.00,
    "min_confirm_rel_volume": 0.68,
    "min_wick_body_ratio": 1.00,
    "min_wick_range_ratio": 0.28,
    "min_confirm_body_ratio": 0.17,
    "entry_offset_atr": 0.08,
    "sl_buffer_atr": 0.13,
    "min_rr": 1.25,
    "min_barrier_rr": 0.75,
    "ema_reclaim_buffer_atr": 0.16,
    "max_countertrend_atr": 0.56,
    "min_pivots": 2,
    "min_sweep_range_atr": 0.52,
    "max_sweep_range_atr": 3.10,
    "max_risk_pct": 0.0125,
    # Corrección de timing: confirmación utilizable antes y guard de frescura
    # para no emitir señales ya consumidas.
    "early_confirm_body_relax": 0.72,
    "early_confirm_volume_relax": 0.82,
    "min_confirm_progress_atr": 0.18,
    "max_tp1_progress_before_emit": 0.38,
}

PREMIUM_LSR_TRADING_PROFILES = {
    "conservador": {"leverage": "20x-30x", "tp1_rr": 1.50, "tp2_rr": 1.95},
    "moderado": {"leverage": "30x-40x", "tp1_rr": 1.75, "tp2_rr": 2.35},
    "agresivo": {"leverage": "40x-50x", "tp1_rr": 2.05, "tp2_rr": 2.75},
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
    df["rel_volume"] = df["volume"] / df["vol_ma"].replace(0, 1e-9)
    df["upper_wick"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["lower_wick"] = df[["open", "close"]].min(axis=1) - df["low"]

    return df


# =======================================
# HELPERS
# =======================================


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _profile_value(profile: Dict, key: str, direction: str, default: Optional[float] = None) -> float:
    direction_prefix = str(direction or "").lower()
    directional_key = f"{direction_prefix}_{key}"
    if directional_key in profile:
        return float(profile[directional_key])
    if key in profile:
        return float(profile[key])
    if default is None:
        raise KeyError(key)
    return float(default)


def breakout_level(df: pd.DataFrame, direction: str) -> float:
    ref = df.iloc[-(BREAKOUT_LOOKBACK + 2):-2]

    if direction == "LONG":
        return float(ref["high"].max())

    return float(ref["low"].min())


def _trend_direction(last: pd.Series) -> Optional[str]:
    if float(last["ema20"]) > float(last["ema50"]) > float(last["ema200"]):
        return "LONG"
    if float(last["ema20"]) < float(last["ema50"]) < float(last["ema200"]):
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
    min_breakout_body_ratio = _profile_value(profile, "min_body_ratio_breakout", direction)

    if atr <= 0:
        return False, {}

    if direction == "LONG":
        breakout_ok = (
            float(prev["close"]) > level
            and float(prev["high"]) > level
            and float(prev["body_ratio"]) >= min_breakout_body_ratio
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
            and float(prev["body_ratio"]) >= min_breakout_body_ratio
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
        "close_extension_atr": float(abs(float(last["close"]) - float(level)) / atr),
    }
    return True, quality


def _continuation_ok(last: pd.Series, direction: str, profile: Dict) -> bool:
    if direction == "LONG":
        if float(last["close"]) <= float(last["open"]):
            return False
    else:
        if float(last["close"]) >= float(last["open"]):
            return False

    min_body_ratio = _profile_value(profile, "min_body_ratio_continuation", direction)
    if float(last["body_ratio"]) < min_body_ratio:
        return False

    min_rel_volume = _profile_value(profile, "min_rel_volume", direction, default=0.0)
    if float(last.get("rel_volume", 0.0) or 0.0) < min_rel_volume:
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


def _adaptive_entry_blend(quality: Dict[str, float], profile: Dict, direction: str = "LONG") -> float:
    """
    Determina qué tan cerca del cierre dejamos la entrada.

    0.0 = entrada pegada al nivel.
    1.0 = entrada pegada al cierre de continuidad.

    Objetivo: reducir expiradas por falta de fill cuando el retroceso posterior
    es demasiado flojo, sin convertir la estrategia en market-chasing bruto.
    """
    retest_tol = max(float(profile.get("retest_tol_atr", 0.5)), 1e-9)
    retest_distance = float(quality.get("retest_distance_atr", 0.0))
    continuation_body = float(quality.get("continuation_body_ratio", 0.0))
    min_cont_body = _profile_value(profile, "min_body_ratio_continuation", direction, default=0.15)
    extension_atr = float(quality.get("close_extension_atr", 0.0))

    # Si el retest queda lejos del nivel, el pullback fue flojo.
    pullback_weakness = _clamp(retest_distance / retest_tol, 0.0, 1.0)

    # Si la vela de continuidad sale con cuerpo fuerte, es más probable que el
    # precio no regale un retroceso profundo antes de seguir.
    continuation_pressure = _clamp(
        (continuation_body - min_cont_body) / max(0.30, 1e-9),
        0.0,
        1.0,
    )

    # Si el cierre ya se alejó varios ATR del nivel, perseguir una entrada muy
    # baja vuelve demasiadas señales no ejecutables.
    extension_pressure = _clamp(
        (extension_atr - _profile_value(profile, "entry_blend_neutral_atr", direction, default=0.20))
        / max(_profile_value(profile, "entry_blend_ramp_atr", direction, default=1.0), 1e-9),
        0.0,
        1.0,
    )

    aggressiveness = _clamp(
        (pullback_weakness * 0.45)
        + (continuation_pressure * 0.30)
        + (extension_pressure * 0.25),
        0.0,
        1.0,
    )

    blend_min = _profile_value(profile, "entry_blend_min", direction, default=0.25)
    blend_max = _profile_value(profile, "entry_blend_max", direction, default=0.70)
    return round(blend_min + ((blend_max - blend_min) * aggressiveness), 4)


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
) -> Optional[Dict]:
    last = df.iloc[-1]

    direction = _trend_direction(last)
    if not direction:
        return None

    higher_tf_context: Dict[str, float] = {}
    if direction == "SHORT" and df_15m is not None and df_1h is not None:
        higher_tf_ok, higher_tf_context = _higher_tf_short_context_ok(df_15m, df_1h)
        if not higher_tf_ok:
            return None

    adx_value = float(last["adx"])
    if adx_value < float(profile["adx_min"]):
        return None

    atr_pct = float(last["atr_pct"])
    if not (float(profile["atr_pct_min"]) <= atr_pct <= float(profile["atr_pct_max"])):
        return None

    breakout_ok, quality = _confirm_breakout_retest(df, direction, profile)
    if not breakout_ok:
        return None

    max_close_extension_atr = _profile_value(profile, "max_close_extension_atr", direction, default=10.0)
    if float(quality.get("close_extension_atr", 0.0) or 0.0) > max_close_extension_atr:
        return None

    if not _continuation_ok(last, direction, profile):
        return None

    level = float(quality["level"])
    close_price = float(last["close"])

    # Entrada adaptativa: si el retroceso posterior suele quedarse corto,
    # acercamos la entrada al cierre de continuidad. Si el retest fue limpio,
    # mantenemos una entrada más paciente cerca del nivel.
    entry_blend = _adaptive_entry_blend(quality, profile, direction=direction)
    entry_price = level + ((close_price - level) * entry_blend)
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
        "entry_blend": entry_blend,
        "entry_model": "adaptive_retest_strength_v1",
    }




def _premium_normalize_price(value: float, decimals: int = 6) -> float:
    return round(float(value), decimals)


def _premium_find_pivots(series: pd.Series, mode: str, window: int) -> List[Tuple[int, float]]:
    values = [float(x) for x in series.tolist()]
    pivots: List[Tuple[int, float]] = []
    if len(values) < (window * 2) + 1:
        return pivots

    for idx in range(window, len(values) - window):
        left = values[idx - window:idx]
        center = values[idx]
        right = values[idx + 1:idx + 1 + window]
        if mode == "high":
            if center > max(left) and center >= max(right):
                pivots.append((idx, center))
        else:
            if center < min(left) and center <= min(right):
                pivots.append((idx, center))
    return pivots


def _premium_cluster_pivots(pivots: List[Tuple[int, float]], tolerance: float) -> List[Dict]:
    zones: List[Dict] = []
    for idx, price in sorted(pivots, key=lambda item: item[1]):
        matched = False
        for zone in zones:
            if abs(price - zone["price"]) <= tolerance:
                zone["prices"].append(price)
                zone["indices"].append(idx)
                zone["price"] = sum(zone["prices"]) / len(zone["prices"])
                matched = True
                break
        if not matched:
            zones.append({"price": price, "prices": [price], "indices": [idx]})
    for zone in zones:
        zone["count"] = len(zone["prices"])
        zone["latest_index"] = max(zone["indices"])
    return zones


def _premium_select_liquidity_zone(historical: pd.DataFrame, direction: str, sweep_candle: pd.Series, profile: Dict) -> Optional[Dict]:
    atr = float(sweep_candle["atr"])
    if atr <= 0:
        return None
    tolerance = atr * float(profile["liquidity_tolerance_atr"])
    min_sweep = atr * float(profile["min_sweep_atr"])
    pivots = _premium_find_pivots(historical["high" if direction == "SHORT" else "low"], "high" if direction == "SHORT" else "low", PREMIUM_LSR_PIVOT_WINDOW)
    if len(pivots) < int(profile["min_pivots"]):
        return None

    candidates: List[Dict] = []
    for zone in _premium_cluster_pivots(pivots, tolerance):
        if zone["count"] < int(profile["min_pivots"]):
            continue
        zone_price = float(zone["price"])
        if direction == "SHORT":
            sweep_ok = float(sweep_candle["high"]) >= zone_price + min_sweep and float(sweep_candle["close"]) < zone_price
            distance = abs(float(sweep_candle["high"]) - zone_price)
        else:
            sweep_ok = float(sweep_candle["low"]) <= zone_price - min_sweep and float(sweep_candle["close"]) > zone_price
            distance = abs(zone_price - float(sweep_candle["low"]))
        if not sweep_ok:
            continue
        zone["distance"] = distance
        candidates.append(zone)

    if not candidates:
        return None
    candidates.sort(key=lambda zone: (-int(zone["count"]), float(zone["distance"]), -int(zone["latest_index"])))
    return candidates[0]


def _premium_recovery_candle_ok(sweep_candle: pd.Series, direction: str, profile: Dict, zone_price: float) -> bool:
    body = max(float(sweep_candle["body"]), 1e-9)
    candle_range = float(sweep_candle["range"])
    if direction == "SHORT":
        wick = float(sweep_candle["upper_wick"])
        if float(sweep_candle["close"]) >= zone_price:
            return False
    else:
        wick = float(sweep_candle["lower_wick"])
        if float(sweep_candle["close"]) <= zone_price:
            return False
    if wick < body * float(profile["min_wick_body_ratio"]):
        return False
    if (wick / max(candle_range, 1e-9)) < float(profile["min_wick_range_ratio"]):
        return False
    return True


def _premium_confirmation_candle_ok(confirm_candle: pd.Series, sweep_candle: pd.Series, direction: str, profile: Dict, zone_price: float) -> bool:
    body_ratio = float(confirm_candle["body_ratio"])
    rel_volume = float(confirm_candle.get("rel_volume", 0.0) or 0.0)
    min_body_ratio = float(profile["min_confirm_body_ratio"])
    min_rel_volume = float(profile["min_confirm_rel_volume"])
    early_body_ratio = max(0.10, min_body_ratio * float(profile.get("early_confirm_body_relax", 0.72)))
    early_rel_volume = max(0.50, min_rel_volume * float(profile.get("early_confirm_volume_relax", 0.82)))
    atr = max(float(confirm_candle["atr"]), 1e-9)

    body_ok = body_ratio >= min_body_ratio
    hybrid_ok = rel_volume >= min_rel_volume and body_ratio >= max(0.10, min_body_ratio * 0.70)
    early_ok = rel_volume >= early_rel_volume and body_ratio >= early_body_ratio

    if not (body_ok or hybrid_ok or early_ok):
        return False

    if direction == "SHORT":
        bearish_close = float(confirm_candle["close"]) < float(confirm_candle["open"])
        follow_through = float(confirm_candle["close"]) <= float(sweep_candle["close"]) or float(confirm_candle["low"]) < float(sweep_candle["low"])
        progress_atr = max(0.0, zone_price - float(confirm_candle["close"])) / atr
        return bearish_close and follow_through and progress_atr >= float(profile.get("min_confirm_progress_atr", 0.18))

    bullish_close = float(confirm_candle["close"]) > float(confirm_candle["open"])
    follow_through = float(confirm_candle["close"]) >= float(sweep_candle["close"]) or float(confirm_candle["high"]) > float(sweep_candle["high"])
    progress_atr = max(0.0, float(confirm_candle["close"]) - zone_price) / atr
    return bullish_close and follow_through and progress_atr >= float(profile.get("min_confirm_progress_atr", 0.18))


def _premium_ema_reclaim_ok(confirm_candle: pd.Series, direction: str, profile: Dict) -> bool:
    atr = max(float(confirm_candle["atr"]), 1e-9)
    buffer = atr * float(profile["ema_reclaim_buffer_atr"])
    ema20 = float(confirm_candle["ema20"])
    if direction == "LONG":
        return float(confirm_candle["close"]) >= (ema20 - buffer)
    return float(confirm_candle["close"]) <= (ema20 + buffer)


def _premium_higher_timeframe_context_ok(df_1h: pd.DataFrame, direction: str, profile: Dict) -> bool:
    if len(df_1h) < max(PREMIUM_LSR_HTF_LOOKBACK, 60):
        return False
    htf = add_indicators(df_1h)
    recent = htf.iloc[-2]
    prev = htf.iloc[-3]
    atr = max(float(recent["atr"]), 1e-9)
    close = float(recent["close"])
    ema20 = float(recent["ema20"])
    ema50 = float(recent["ema50"])
    slope20 = ema20 - float(prev["ema20"])
    countertrend = atr * float(profile["max_countertrend_atr"])
    if direction == "LONG":
        heavy_bearish_bias = ema20 < ema50 and slope20 < 0
        clearly_against = close < ema50 - (countertrend * 1.85) and close < ema20 - (countertrend * 1.35)
        stretched_against = close < ema20 - (countertrend * 2.20)
        return not (heavy_bearish_bias and (clearly_against or stretched_against))
    heavy_bullish_bias = ema20 > ema50 and slope20 > 0
    clearly_against = close > ema50 + (countertrend * 1.85) and close > ema20 + (countertrend * 1.35)
    stretched_against = close > ema20 + (countertrend * 2.20)
    return not (heavy_bullish_bias and (clearly_against or stretched_against))


def _premium_room_to_target(entry_price: float, stop_loss: float, structure_target: float, direction: str) -> float:
    risk = abs(stop_loss - entry_price)
    if risk <= 0:
        return 0.0
    room = (entry_price - structure_target) if direction == "SHORT" else (structure_target - entry_price)
    return max(0.0, room / risk)


def _premium_nearest_barrier_price(historical: pd.DataFrame, df_1h: pd.DataFrame, entry_price: float, direction: str) -> Optional[float]:
    candidates: List[float] = []
    htf = df_1h if "ema20" in df_1h.columns and "ema50" in df_1h.columns else add_indicators(df_1h)
    if direction == "LONG":
        pivot_prices = [price for _, price in _premium_find_pivots(historical["high"], "high", max(2, PREMIUM_LSR_PIVOT_WINDOW - 1)) if price > entry_price]
        candidates.extend(pivot_prices)
        for value in [float(htf.iloc[-2]["ema20"]), float(htf.iloc[-2]["ema50"])] :
            if value > entry_price:
                candidates.append(value)
        return min(candidates) if candidates else None
    pivot_prices = [price for _, price in _premium_find_pivots(historical["low"], "low", max(2, PREMIUM_LSR_PIVOT_WINDOW - 1)) if price < entry_price]
    candidates.extend(pivot_prices)
    for value in [float(htf.iloc[-2]["ema20"]), float(htf.iloc[-2]["ema50"])] :
        if value < entry_price:
            candidates.append(value)
    return max(candidates) if candidates else None


def _premium_tp_from_rr(entry_price: float, risk: float, rr: float, direction: str) -> float:
    return entry_price + (risk * rr) if direction == "LONG" else entry_price - (risk * rr)


def _premium_build_trade_profiles(entry_price: float, direction: str, stop_loss: float, max_room_rr: float) -> Dict[str, Dict]:
    risk = abs(stop_loss - entry_price)
    profiles: Dict[str, Dict] = {}
    capped_max_rr = max(1.20, max_room_rr - 0.05)
    for name, cfg in PREMIUM_LSR_TRADING_PROFILES.items():
        tp1_rr = min(float(cfg["tp1_rr"]), capped_max_rr)
        tp2_rr = min(float(cfg["tp2_rr"]), capped_max_rr)
        if tp2_rr <= tp1_rr:
            tp2_rr = min(capped_max_rr, tp1_rr + 0.25)
        profiles[name] = {
            "stop_loss": _premium_normalize_price(stop_loss),
            "take_profits": [
                _premium_normalize_price(_premium_tp_from_rr(entry_price, risk, tp1_rr, direction)),
                _premium_normalize_price(_premium_tp_from_rr(entry_price, risk, tp2_rr, direction)),
            ],
            "leverage": cfg["leverage"],
        }
    return profiles


def _premium_progress_to_tp1(entry_price: float, current_price: float, tp1_price: float, direction: str) -> float:
    total = abs(tp1_price - entry_price)
    if total <= 1e-9:
        return 1.0
    progressed = (current_price - entry_price) if direction == "LONG" else (entry_price - current_price)
    return max(0.0, progressed / total)


def _premium_emit_is_fresh(entry_price: float, current_price: float, tp1_price: float, direction: str, profile: Dict) -> bool:
    progress = _premium_progress_to_tp1(entry_price, current_price, tp1_price, direction)
    if progress >= 1.0:
        return False
    return progress <= float(profile.get("max_tp1_progress_before_emit", 0.38))


def _premium_evaluate_direction(df: pd.DataFrame, df_1h: pd.DataFrame, direction: str, profile: Dict) -> Optional[Tuple[Dict, Tuple]]:
    sweep_candle = df.iloc[-2]
    confirm_candle = df.iloc[-1]
    historical = df.iloc[:-2].tail(PREMIUM_LSR_LIQUIDITY_LOOKBACK)
    if len(historical) < PREMIUM_LSR_LIQUIDITY_LOOKBACK:
        return None

    atr = float(sweep_candle["atr"])
    atr_pct = float(sweep_candle["atr_pct"])
    if atr <= 0 or not (float(profile["atr_pct_min"]) <= atr_pct <= float(profile["atr_pct_max"])):
        return None

    rel_volume = max(float(sweep_candle.get("rel_volume", 0.0) or 0.0), float(confirm_candle.get("rel_volume", 0.0) or 0.0))
    if rel_volume < float(profile["min_rel_volume"]):
        return None

    sweep_range_atr = float(sweep_candle["range"]) / atr
    if not (float(profile["min_sweep_range_atr"]) <= sweep_range_atr <= float(profile["max_sweep_range_atr"])):
        return None

    if not _premium_higher_timeframe_context_ok(df_1h, direction, profile):
        return None

    zone = _premium_select_liquidity_zone(historical, direction, sweep_candle, profile)
    if not zone:
        return None
    zone_price = float(zone["price"])

    if not _premium_recovery_candle_ok(sweep_candle, direction, profile, zone_price):
        return None
    if not _premium_confirmation_candle_ok(confirm_candle, sweep_candle, direction, profile, zone_price):
        return None
    if not _premium_ema_reclaim_ok(confirm_candle, direction, profile):
        return None

    entry_offset = atr * float(profile["entry_offset_atr"])
    if direction == "SHORT":
        entry_price = zone_price - entry_offset
        stop_loss = float(sweep_candle["high"]) + (atr * float(profile["sl_buffer_atr"]))
        structure_target = float(historical.tail(PREMIUM_LSR_TARGET_LOOKBACK)["low"].min())
    else:
        entry_price = zone_price + entry_offset
        stop_loss = float(sweep_candle["low"]) - (atr * float(profile["sl_buffer_atr"]))
        structure_target = float(historical.tail(PREMIUM_LSR_TARGET_LOOKBACK)["high"].max())

    risk = abs(stop_loss - entry_price)
    if risk <= 0:
        return None
    risk_pct = risk / max(entry_price, 1e-9)
    if risk_pct > float(profile["max_risk_pct"]):
        return None

    room_rr = _premium_room_to_target(entry_price, stop_loss, structure_target, direction)
    if room_rr < float(profile["min_rr"]):
        return None
    nearest_barrier = _premium_nearest_barrier_price(historical, df_1h, entry_price, direction)
    barrier_rr = room_rr
    if nearest_barrier is not None:
        barrier_rr = _premium_room_to_target(entry_price, stop_loss, nearest_barrier, direction)
        if barrier_rr < float(profile["min_barrier_rr"]):
            return None

    trade_profiles = _premium_build_trade_profiles(entry_price, direction, stop_loss, room_rr)
    tp1_price = float(trade_profiles["conservador"]["take_profits"][0])
    current_price = float(confirm_candle["close"])
    if not _premium_emit_is_fresh(entry_price, current_price, tp1_price, direction, profile):
        return None

    progress_to_tp1 = _premium_progress_to_tp1(entry_price, current_price, tp1_price, direction)
    components = [
        ("liquidity_zone", round(float(zone["count"]) * 2.0, 2)),
        ("relative_volume", round(min(rel_volume, 3.0) * 3.0, 2)),
        ("barrier_room", round(min(barrier_rr, 3.0) * 4.0, 2)),
        ("timing_guard", round(max(0.0, 10.0 * (1.0 - progress_to_tp1)), 2)),
    ]
    score = round(float(profile["score"]), 2)
    result = {
        "strategy_name": PREMIUM_LSR_STRATEGY_NAME,
        "direction": direction,
        "entry_price": _premium_normalize_price(entry_price),
        "stop_loss": trade_profiles["conservador"]["stop_loss"],
        "take_profits": list(trade_profiles["conservador"]["take_profits"]),
        "profiles": trade_profiles,
        "score": score,
        "raw_score": score,
        "normalized_score": score,
        "components": components,
        "raw_components": components,
        "normalized_components": components,
        "timeframes": ["15M"],
        "setup_group": str(profile["name"]),
        "atr_pct": round(atr_pct, 6),
        "score_profile": "premium_lsr_plus_filters",
        "score_calibration": PREMIUM_LSR_SCORE_CALIBRATION_VERSION,
        "entry_model": "lsr_live_confirm_v1",
        "tp1_progress_at_emit": round(progress_to_tp1, 4),
    }
    ranking = (int(zone["count"]), round(room_rr, 4), round(barrier_rr, 4), round(rel_volume, 4))
    return result, ranking


def premium_liquidity_sweep_reversal_strategy(df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> Optional[Dict]:
    if len(df_15m) < PREMIUM_LSR_MIN_HISTORY_BARS or len(df_1h) < 60:
        return None
    df = add_indicators(df_15m)
    if len(df) < PREMIUM_LSR_MIN_HISTORY_BARS:
        return None
    required_cols = ["atr", "atr_pct", "rel_volume", "body_ratio", "ema20", "ema50", "upper_wick", "lower_wick"]
    if df[required_cols].tail(5).isnull().any().any():
        return None
    best_result: Optional[Dict] = None
    best_rank: Optional[Tuple] = None
    for direction in ("SHORT", "LONG"):
        evaluated = _premium_evaluate_direction(df, df_1h, direction, PREMIUM_LSR_PROFILE)
        if not evaluated:
            continue
        result, rank = evaluated
        if best_rank is None or rank > best_rank:
            best_result = result
            best_rank = rank
    return best_result


# =======================================
# ESTRATEGIA 5M
# =======================================


def mtf_strategy(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: pd.DataFrame,
) -> Optional[Dict]:
    # 0) Primero intenta la estrategia separada de Premium.
    premium_result = premium_liquidity_sweep_reversal_strategy(df_1h=df_1h, df_15m=df_15m)
    if premium_result:
        return premium_result

    # Mantenemos la firma para no romper el scanner actual.
    # La lógica operativa final de Free/Plus vive en 5M.
    if len(df_5m) < BREAKOUT_LOOKBACK + 30:
        return None

    df = add_indicators(df_5m)

    if len(df) < BREAKOUT_LOOKBACK + 30:
        return None

    # 1) Primero intenta el setup bueno compartido por FREE/PLUS.
    shared_result = _evaluate_profile(df, SHARED_PROFILE, df_15m=df_15m, df_1h=df_1h)
    if shared_result:
        return {
            "direction": shared_result["direction"],
            "entry_price": shared_result["entry_price"],
            "stop_loss": shared_result["trade_profiles"]["conservador"]["stop_loss"],
            "take_profits": list(shared_result["trade_profiles"]["conservador"]["take_profits"]),
            "profiles": shared_result["trade_profiles"],
            "score": shared_result["score"],
            "raw_score": shared_result["raw_score"],
            "normalized_score": shared_result["normalized_score"],
            "components": shared_result["components"],
            "raw_components": shared_result["raw_components"],
            "normalized_components": shared_result["normalized_components"],
            "timeframes": ["5M"],
            "setup_group": shared_result["setup_group"],
            "atr_pct": shared_result["atr_pct"],
            "score_profile": shared_result["score_profile"],
            "score_calibration": shared_result["score_calibration"],
            "higher_tf_context": shared_result["higher_tf_context"],
        }

    # 2) Si no pasa el setup bueno, intenta el más flexible para FREE.
    free_result = _evaluate_profile(df, FREE_PROFILE, df_15m=df_15m, df_1h=df_1h)
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
            "setup_group": free_result["setup_group"],
            "atr_pct": free_result["atr_pct"],
            "score_profile": free_result["score_profile"],
            "score_calibration": free_result["score_calibration"],
            "higher_tf_context": free_result["higher_tf_context"],
        }

    return None
