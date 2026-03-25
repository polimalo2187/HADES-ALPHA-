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
}

FREE_PROFILE = {
    "name": "free",
    "adx_min": 15.0,
    "atr_pct_min": 0.0020,
    "atr_pct_max": 0.0140,
    "retest_tol_atr": 0.62,
    "min_body_ratio_breakout": 0.22,
    "min_body_ratio_continuation": 0.16,
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


# =======================================
# HELPERS
# =======================================


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


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
) -> Optional[Dict]:
    last = df.iloc[-1]

    direction = _trend_direction(last)
    if not direction:
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

    if not _continuation_ok(last, direction, profile):
        return None

    level = float(quality["level"])
    close_price = float(last["close"])

    # Entrada menos perseguida: más cerca del nivel de retest que del cierre.
    entry_price = level + ((close_price - level) * 0.25)
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
    }


# =======================================
# ESTRATEGIA 5M
# =======================================


def mtf_strategy(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: pd.DataFrame,
) -> Optional[Dict]:
    # Mantenemos la firma para no romper el scanner actual.
    # La lógica operativa final vive en 5M.
    if len(df_5m) < BREAKOUT_LOOKBACK + 30:
        return None

    df = add_indicators(df_5m)

    if len(df) < BREAKOUT_LOOKBACK + 30:
        return None

    # 1) Primero intenta el setup bueno compartido por PLUS y PREMIUM.
    shared_result = _evaluate_profile(df, SHARED_PROFILE)
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
        }

    # 2) Si no pasa el setup bueno, intenta el más flexible para FREE.
    free_result = _evaluate_profile(df, FREE_PROFILE)
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
        }

    return None
