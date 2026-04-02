from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

import pandas as pd

# =======================================
# LIQUIDITY SWEEP REVERSAL — ESTRATEGIA ÚNICA
# =======================================
# Arquitectura final:
# - una sola familia de liquidez
# - tres perfiles nativos: premium / plus / free
# - routing jerárquico se hace fuera de este archivo
# - capa de ejecución incluida para evitar señales tardías
# =======================================

ATR_PERIOD = 14
VOLUME_PERIOD = 20
LIQUIDITY_LOOKBACK = 36
TARGET_LOOKBACK = 30
HTF_LOOKBACK = 48
PIVOT_WINDOW = 3
MIN_HISTORY_BARS = max(LIQUIDITY_LOOKBACK + 8, ATR_PERIOD + VOLUME_PERIOD + 8)

STRATEGY_NAME = "LIQUIDITY_SWEEP_REVERSAL"
SCORE_CALIBRATION_VERSION = "v3_liquidity_unified_live_execution"

LIVE_CONFIRM_MIN_PROGRESS = 0.35
SEND_STRUCTURAL_PROGRESS_TO_TP1_MAX = 0.20
SEND_STRUCTURAL_PROGRESS_R_MAX = 0.20
REPRICE_PROGRESS_TO_TP1_MAX = 0.35
REPRICE_PROGRESS_R_MAX = 0.35
ABSOLUTE_LATE_PROGRESS_TO_TP1_MAX = 0.50

PREMIUM_PROFILE = {
    "name": "premium",
    "score": 90.0,
    "atr_pct_min": 0.0021,
    "atr_pct_max": 0.0135,
    "liquidity_tolerance_atr": 0.22,
    "min_sweep_atr": 0.12,
    "min_rel_volume": 1.15,
    "min_confirm_rel_volume": 0.75,
    "min_wick_body_ratio": 1.25,
    "min_wick_range_ratio": 0.34,
    "min_confirm_body_ratio": 0.22,
    "entry_offset_atr": 0.06,
    "sl_buffer_atr": 0.11,
    "min_rr": 1.35,
    "min_barrier_rr": 0.85,
    "ema_reclaim_buffer_atr": 0.12,
    "max_countertrend_atr": 0.42,
    "min_pivots": 2,
    "min_sweep_range_atr": 0.65,
    "max_sweep_range_atr": 2.80,
    "max_risk_pct": 0.0105,
    "live_min_body_ratio": 0.26,
    "live_volume_multiplier": 1.10,
}

PLUS_PROFILE = {
    "name": "plus",
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
    "live_min_body_ratio": 0.20,
    "live_volume_multiplier": 1.08,
}

FREE_PROFILE = {
    "name": "free",
    "score": 74.0,
    "atr_pct_min": 0.0015,
    "atr_pct_max": 0.0160,
    "liquidity_tolerance_atr": 0.33,
    "min_sweep_atr": 0.07,
    "min_rel_volume": 0.90,
    "min_confirm_rel_volume": 0.58,
    "min_wick_body_ratio": 0.82,
    "min_wick_range_ratio": 0.24,
    "min_confirm_body_ratio": 0.12,
    "entry_offset_atr": 0.09,
    "sl_buffer_atr": 0.15,
    "min_rr": 1.15,
    "min_barrier_rr": 0.68,
    "ema_reclaim_buffer_atr": 0.22,
    "max_countertrend_atr": 0.68,
    "min_pivots": 2,
    "min_sweep_range_atr": 0.45,
    "max_sweep_range_atr": 3.40,
    "max_risk_pct": 0.0145,
    "live_min_body_ratio": 0.16,
    "live_volume_multiplier": 1.05,
}

PROFILES = [PREMIUM_PROFILE, PLUS_PROFILE, FREE_PROFILE]

TRADING_PROFILES = {
    "conservador": {"leverage": "20x-30x", "tp1_rr": 1.50, "tp2_rr": 1.95},
    "moderado": {"leverage": "30x-40x", "tp1_rr": 1.75, "tp2_rr": 2.35},
    "agresivo": {"leverage": "40x-50x", "tp1_rr": 2.05, "tp2_rr": 2.75},
}


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    prev_close = df["close"].shift(1)
    tr_components = pd.concat(
        [
            (df["high"] - df["low"]),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    )
    df["atr"] = tr_components.max(axis=1).rolling(ATR_PERIOD).mean()
    df["atr_pct"] = df["atr"] / df["close"].clip(lower=1e-9)

    df["vol_sma"] = df["volume"].rolling(VOLUME_PERIOD).mean()
    df["rel_volume"] = df["volume"] / df["vol_sma"].clip(lower=1e-9)

    df["body"] = (df["close"] - df["open"]).abs()
    df["range"] = (df["high"] - df["low"]).clip(lower=1e-9)
    df["body_ratio"] = df["body"] / df["range"]

    df["upper_wick"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["lower_wick"] = df[["open", "close"]].min(axis=1) - df["low"]

    df["ema10"] = df["close"].ewm(span=10, adjust=False).mean()
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
    return df


def _normalize_price(value: float, decimals: int = 6) -> float:
    if value is None:
        return 0.0
    quant = Decimal("1").scaleb(-decimals)
    return float(Decimal(str(float(value))).quantize(quant, rounding=ROUND_HALF_UP))


def _find_pivots(series: pd.Series, mode: str, window: int) -> List[Tuple[int, float]]:
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


def _cluster_pivots(pivots: List[Tuple[int, float]], tolerance: float) -> List[Dict]:
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


def _select_liquidity_zone(historical: pd.DataFrame, direction: str, sweep_candle: pd.Series, profile: Dict) -> Optional[Dict]:
    atr = float(sweep_candle["atr"])
    if atr <= 0:
        return None
    tolerance = atr * float(profile["liquidity_tolerance_atr"])
    min_sweep = atr * float(profile["min_sweep_atr"])
    pivots = _find_pivots(
        historical["high"] if direction == "SHORT" else historical["low"],
        "high" if direction == "SHORT" else "low",
        PIVOT_WINDOW,
    )
    if len(pivots) < int(profile["min_pivots"]):
        return None

    candidates: List[Dict] = []
    for zone in _cluster_pivots(pivots, tolerance):
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


def _recovery_candle_ok(sweep_candle: pd.Series, direction: str, profile: Dict, zone_price: float) -> bool:
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


def _current_candle_progress(candle: pd.Series) -> float:
    open_time = candle.get("open_time")
    close_time = candle.get("close_time")
    if open_time is None or close_time is None:
        return 1.0
    try:
        if not isinstance(open_time, pd.Timestamp):
            open_time = pd.to_datetime(open_time, utc=True)
        if not isinstance(close_time, pd.Timestamp):
            close_time = pd.to_datetime(close_time, utc=True)
        now = datetime.now(timezone.utc)
        total = max((close_time.to_pydatetime() - open_time.to_pydatetime()).total_seconds(), 1.0)
        elapsed = (now - open_time.to_pydatetime()).total_seconds()
        return max(0.0, min(1.0, elapsed / total))
    except Exception:
        return 1.0


def _projected_rel_volume(confirm_candle: pd.Series) -> float:
    base_rel = float(confirm_candle.get("rel_volume", 0.0) or 0.0)
    progress = max(_current_candle_progress(confirm_candle), 0.05)
    return base_rel / progress


def _confirmation_candle_ok(confirm_candle: pd.Series, sweep_candle: pd.Series, direction: str, profile: Dict) -> bool:
    body_ratio = float(confirm_candle["body_ratio"])
    rel_volume = float(confirm_candle.get("rel_volume", 0.0) or 0.0)
    min_body_ratio = float(profile["min_confirm_body_ratio"])
    min_rel_volume = float(profile["min_confirm_rel_volume"])

    body_ok = body_ratio >= min_body_ratio
    hybrid_ok = rel_volume >= min_rel_volume and body_ratio >= max(0.10, min_body_ratio * 0.70)
    if not (body_ok or hybrid_ok):
        return False

    if direction == "SHORT":
        bearish_close = float(confirm_candle["close"]) < float(confirm_candle["open"])
        follow_through = float(confirm_candle["close"]) <= float(sweep_candle["close"]) or float(confirm_candle["low"]) < float(sweep_candle["low"])
        return bearish_close and follow_through

    bullish_close = float(confirm_candle["close"]) > float(confirm_candle["open"])
    follow_through = float(confirm_candle["close"]) >= float(sweep_candle["close"]) or float(confirm_candle["high"]) > float(sweep_candle["high"])
    return bullish_close and follow_through


def _live_confirmation_ready(confirm_candle: pd.Series, sweep_candle: pd.Series, direction: str, profile: Dict) -> bool:
    progress = _current_candle_progress(confirm_candle)
    if progress < LIVE_CONFIRM_MIN_PROGRESS:
        return False

    body_ratio = float(confirm_candle.get("body_ratio", 0.0) or 0.0)
    projected_rel_volume = _projected_rel_volume(confirm_candle)
    min_body_ratio = max(float(profile["live_min_body_ratio"]), float(profile["min_confirm_body_ratio"]) * 1.25)
    min_projected_rel_volume = float(profile["min_confirm_rel_volume"]) * float(profile["live_volume_multiplier"])

    if body_ratio < min_body_ratio:
        return False
    if projected_rel_volume < min_projected_rel_volume:
        return False

    if direction == "SHORT":
        directional = float(confirm_candle["close"]) < float(confirm_candle["open"])
        follow_through = float(confirm_candle["close"]) <= float(sweep_candle["close"]) or float(confirm_candle["low"]) < float(sweep_candle["low"])
        return directional and follow_through

    directional = float(confirm_candle["close"]) > float(confirm_candle["open"])
    follow_through = float(confirm_candle["close"]) >= float(sweep_candle["close"]) or float(confirm_candle["high"]) > float(sweep_candle["high"])
    return directional and follow_through


def _ema_reclaim_ok(confirm_candle: pd.Series, direction: str, profile: Dict) -> bool:
    atr = max(float(confirm_candle["atr"]), 1e-9)
    buffer = atr * float(profile["ema_reclaim_buffer_atr"])
    ema20 = float(confirm_candle["ema20"])
    if direction == "LONG":
        return float(confirm_candle["close"]) >= (ema20 - buffer)
    return float(confirm_candle["close"]) <= (ema20 + buffer)


def _higher_timeframe_context_ok(df_1h: pd.DataFrame, direction: str, profile: Dict) -> bool:
    if len(df_1h) < max(HTF_LOOKBACK, 60):
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
        if heavy_bearish_bias and (clearly_against or stretched_against):
            return False
        return True

    heavy_bullish_bias = ema20 > ema50 and slope20 > 0
    clearly_against = close > ema50 + (countertrend * 1.85) and close > ema20 + (countertrend * 1.35)
    stretched_against = close > ema20 + (countertrend * 2.20)
    if heavy_bullish_bias and (clearly_against or stretched_against):
        return False
    return True


def _room_to_target(entry_price: float, stop_loss: float, structure_target: float, direction: str) -> float:
    risk = abs(stop_loss - entry_price)
    if risk <= 0:
        return 0.0
    room = (entry_price - structure_target) if direction == "SHORT" else (structure_target - entry_price)
    return max(0.0, room / risk)


def _nearest_barrier_price(historical: pd.DataFrame, df_1h: pd.DataFrame, entry_price: float, direction: str) -> Optional[float]:
    candidates: List[float] = []
    htf = df_1h if {"ema20", "ema50"}.issubset(set(df_1h.columns)) else add_indicators(df_1h)
    if direction == "LONG":
        pivot_prices = [price for _, price in _find_pivots(historical["high"], "high", max(2, PIVOT_WINDOW - 1)) if price > entry_price]
        candidates.extend(pivot_prices)
        for value in [float(htf.iloc[-2]["ema20"]), float(htf.iloc[-2]["ema50"] )]:
            if value > entry_price:
                candidates.append(value)
        return min(candidates) if candidates else None
    pivot_prices = [price for _, price in _find_pivots(historical["low"], "low", max(2, PIVOT_WINDOW - 1)) if price < entry_price]
    candidates.extend(pivot_prices)
    for value in [float(htf.iloc[-2]["ema20"]), float(htf.iloc[-2]["ema50"] )]:
        if value < entry_price:
            candidates.append(value)
    return max(candidates) if candidates else None


def _tp_from_rr(entry_price: float, risk: float, rr: float, direction: str) -> float:
    return entry_price + (risk * rr) if direction == "LONG" else entry_price - (risk * rr)


def _build_trade_profiles(entry_price: float, direction: str, stop_loss: float, max_room_rr: float) -> Dict[str, Dict]:
    risk = abs(stop_loss - entry_price)
    profiles: Dict[str, Dict] = {}
    capped_max_rr = max(1.20, max_room_rr - 0.05)
    for name, cfg in TRADING_PROFILES.items():
        tp1_rr = min(float(cfg["tp1_rr"]), capped_max_rr)
        tp2_rr = min(float(cfg["tp2_rr"]), capped_max_rr)
        if tp2_rr <= tp1_rr:
            tp2_rr = min(capped_max_rr, tp1_rr + 0.25)
        profiles[name] = {
            "stop_loss": _normalize_price(stop_loss),
            "take_profits": [
                _normalize_price(_tp_from_rr(entry_price, risk, tp1_rr, direction)),
                _normalize_price(_tp_from_rr(entry_price, risk, tp2_rr, direction)),
            ],
            "leverage": cfg["leverage"],
        }
    return profiles


def _progress_to_tp1(entry_price: float, current_price: float, tp1_price: float, direction: str) -> float:
    total = abs(tp1_price - entry_price)
    if total <= 1e-9:
        return 1.0
    progressed = (current_price - entry_price) if direction == "LONG" else (entry_price - current_price)
    return max(0.0, progressed / total)


def _progress_in_r(entry_price: float, current_price: float, stop_loss: float, direction: str) -> float:
    risk = abs(stop_loss - entry_price)
    if risk <= 1e-9:
        return 1.0
    progressed = (current_price - entry_price) if direction == "LONG" else (entry_price - current_price)
    return max(0.0, progressed / risk)


def _decide_send_mode(entry_price: float, current_price: float, tp1_price: float, stop_loss: float, direction: str) -> str:
    progress_to_tp1 = _progress_to_tp1(entry_price, current_price, tp1_price, direction)
    progress_r = _progress_in_r(entry_price, current_price, stop_loss, direction)
    if progress_to_tp1 > ABSOLUTE_LATE_PROGRESS_TO_TP1_MAX:
        return "discarded_late"
    if progress_to_tp1 <= SEND_STRUCTURAL_PROGRESS_TO_TP1_MAX and progress_r <= SEND_STRUCTURAL_PROGRESS_R_MAX:
        return "structural"
    if progress_to_tp1 <= REPRICE_PROGRESS_TO_TP1_MAX and progress_r <= REPRICE_PROGRESS_R_MAX:
        return "repriced"
    return "discarded_late"


def _reprice_candidate(current_price: float, stop_loss: float, direction: str, structure_target: float, nearest_barrier: Optional[float], profile: Dict) -> Optional[Tuple[float, Dict[str, Dict], float, float]]:
    entry_price = float(current_price)
    risk = abs(stop_loss - entry_price)
    if risk <= 0:
        return None
    risk_pct = risk / max(entry_price, 1e-9)
    if risk_pct > float(profile["max_risk_pct"]):
        return None
    room_rr = _room_to_target(entry_price, stop_loss, structure_target, direction)
    if room_rr < float(profile["min_rr"]):
        return None
    barrier_rr = room_rr
    if nearest_barrier is not None:
        barrier_rr = _room_to_target(entry_price, stop_loss, nearest_barrier, direction)
        if barrier_rr < float(profile["min_barrier_rr"]):
            return None
    trade_profiles = _build_trade_profiles(entry_price, direction, stop_loss, room_rr)
    return entry_price, trade_profiles, room_rr, barrier_rr


def _evaluate_direction(df: pd.DataFrame, df_1h: pd.DataFrame, direction: str, profile: Dict) -> Optional[Tuple[Dict, Tuple]]:
    sweep_candle = df.iloc[-2]
    confirm_candle = df.iloc[-1]
    historical = df.iloc[:-2].tail(LIQUIDITY_LOOKBACK)
    if len(historical) < LIQUIDITY_LOOKBACK:
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

    if not _higher_timeframe_context_ok(df_1h, direction, profile):
        return None

    zone = _select_liquidity_zone(historical, direction, sweep_candle, profile)
    if not zone:
        return None
    zone_price = float(zone["price"])

    if not _recovery_candle_ok(sweep_candle, direction, profile, zone_price):
        return None

    live_ready = _live_confirmation_ready(confirm_candle, sweep_candle, direction, profile)
    closed_ready = _confirmation_candle_ok(confirm_candle, sweep_candle, direction, profile)
    if not (live_ready or closed_ready):
        return None

    if not _ema_reclaim_ok(confirm_candle, direction, profile):
        return None

    entry_offset = atr * float(profile["entry_offset_atr"])
    if direction == "SHORT":
        model_entry = zone_price - entry_offset
        stop_loss = float(sweep_candle["high"]) + (atr * float(profile["sl_buffer_atr"]))
        structure_target = float(historical.tail(TARGET_LOOKBACK)["low"].min())
    else:
        model_entry = zone_price + entry_offset
        stop_loss = float(sweep_candle["low"]) - (atr * float(profile["sl_buffer_atr"]))
        structure_target = float(historical.tail(TARGET_LOOKBACK)["high"].max())

    risk = abs(stop_loss - model_entry)
    if risk <= 0:
        return None
    risk_pct = risk / max(model_entry, 1e-9)
    if risk_pct > float(profile["max_risk_pct"]):
        return None

    room_rr = _room_to_target(model_entry, stop_loss, structure_target, direction)
    if room_rr < float(profile["min_rr"]):
        return None
    nearest_barrier = _nearest_barrier_price(historical, df_1h, model_entry, direction)
    barrier_rr = room_rr
    if nearest_barrier is not None:
        barrier_rr = _room_to_target(model_entry, stop_loss, nearest_barrier, direction)
        if barrier_rr < float(profile["min_barrier_rr"]):
            return None

    trade_profiles = _build_trade_profiles(model_entry, direction, stop_loss, room_rr)
    current_price = float(confirm_candle["close"])
    tp1_price = float(trade_profiles["conservador"]["take_profits"][0])
    send_mode = _decide_send_mode(model_entry, current_price, tp1_price, stop_loss, direction)

    effective_entry = model_entry
    effective_profiles = trade_profiles
    effective_room_rr = room_rr
    effective_barrier_rr = barrier_rr
    if send_mode == "repriced":
        repriced = _reprice_candidate(current_price, stop_loss, direction, structure_target, nearest_barrier, profile)
        if not repriced:
            return None
        effective_entry, effective_profiles, effective_room_rr, effective_barrier_rr = repriced
        tp1_price = float(effective_profiles["conservador"]["take_profits"][0])
    elif send_mode == "discarded_late":
        return None

    progress_to_tp1 = _progress_to_tp1(model_entry, current_price, tp1_price if send_mode == "repriced" else float(trade_profiles["conservador"]["take_profits"][0]), direction)
    progress_r = _progress_in_r(model_entry, current_price, stop_loss, direction)

    components = [
        ("liquidity_zone", round(float(zone["count"]) * 2.0, 2)),
        ("minimum_sweep", round(min(sweep_range_atr, 3.0) * 2.0, 2)),
        ("relative_volume", round(min(rel_volume, 3.0) * 3.0, 2)),
        ("confirmation_candle", round((float(confirm_candle.get("body_ratio", 0.0) or 0.0)) * 12.0, 2)),
        ("barrier_room", round(min(effective_barrier_rr, 3.0) * 4.0, 2)),
        ("entry_freshness", round(max(0.0, 10.0 * (1.0 - min(progress_to_tp1, 1.0))), 2)),
    ]
    score = round(float(profile["score"]), 2)
    result = {
        "strategy_name": STRATEGY_NAME,
        "direction": direction,
        "entry_price": _normalize_price(effective_entry),
        "entry_model_price": _normalize_price(model_entry),
        "entry_sent_price": _normalize_price(effective_entry),
        "stop_loss": effective_profiles["conservador"]["stop_loss"],
        "take_profits": list(effective_profiles["conservador"]["take_profits"]),
        "profiles": effective_profiles,
        "score": score,
        "raw_score": score,
        "normalized_score": score,
        "components": components,
        "raw_components": components,
        "normalized_components": components,
        "timeframes": ["15M"],
        "setup_group": str(profile["name"]),
        "candidate_tier": str(profile["name"]),
        "final_tier": str(profile["name"]),
        "atr_pct": round(atr_pct, 6),
        "score_profile": str(profile["name"]),
        "score_calibration": SCORE_CALIBRATION_VERSION,
        "entry_model": "liquidity_zone_offset_v1",
        "send_mode": send_mode,
        "setup_stage": "live_confirmed" if live_ready else "closed_confirmed",
        "tp1_progress_at_send_pct": round(progress_to_tp1 * 100.0, 2),
        "r_progress_at_send": round(progress_r, 4),
        "live_confirm_progress": round(_current_candle_progress(confirm_candle), 4),
    }
    ranking = (int(zone["count"]), round(effective_room_rr, 4), round(effective_barrier_rr, 4), round(rel_volume, 4))
    return result, ranking


def _evaluate_profile(df: pd.DataFrame, df_1h: pd.DataFrame, profile: Dict) -> Optional[Dict]:
    best_result: Optional[Dict] = None
    best_rank: Optional[Tuple] = None
    for direction in ("SHORT", "LONG"):
        evaluated = _evaluate_direction(df, df_1h, direction, profile)
        if not evaluated:
            continue
        result, rank = evaluated
        if best_rank is None or rank > best_rank:
            best_result = result
            best_rank = rank
    return best_result


def liquidity_sweep_reversal_strategy(df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> Optional[Dict]:
    if len(df_15m) < MIN_HISTORY_BARS or len(df_1h) < 60:
        return None
    df = add_indicators(df_15m)
    if len(df) < MIN_HISTORY_BARS:
        return None
    required_cols = ["atr", "atr_pct", "rel_volume", "body_ratio", "ema20", "ema50", "upper_wick", "lower_wick"]
    if df[required_cols].tail(5).isnull().any().any():
        return None
    for profile in PROFILES:
        result = _evaluate_profile(df, df_1h, profile)
        if result:
            return result
    return None


def mtf_strategy(df_1h: pd.DataFrame, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> Optional[Dict]:
    _ = df_5m
    return liquidity_sweep_reversal_strategy(df_1h, df_15m)
