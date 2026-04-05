from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

import pandas as pd

# =======================================
# CONFIGURACIÓN BASE — LIQUIDITY SWEEP REVERSAL
# =======================================
# Regla operativa acordada:
# - filtros de estrategia idénticos a la versión original
# - confirmación por cierre de vela
# - envío inmediato tras el cierre con precio real de mercado
# - SL estructural intacto
# - TPs recalculados desde el precio real de envío
# =======================================

ATR_PERIOD = 14
VOLUME_PERIOD = 20
LIQUIDITY_LOOKBACK = 36
TARGET_LOOKBACK = 30
HTF_LOOKBACK = 48
PIVOT_WINDOW = 3
MIN_HISTORY_BARS = max(LIQUIDITY_LOOKBACK + 8, ATR_PERIOD + VOLUME_PERIOD + 8)

STRATEGY_NAME = "LIQUIDITY_SWEEP_REVERSAL"
SCORE_CALIBRATION_VERSION = "v6_liquidity_close_market_rebalanced"

# =======================================
# PERFILES POR PLAN
# Premium sigue siendo el más estricto, pero con rebalance para recuperar frecuencia sin volver a setups basura.
# =======================================

PREMIUM_PROFILE = {
    "name": "premium",
    "score": 90.0,
    "atr_pct_min": 0.0014,
    "atr_pct_max": 0.0165,
    "liquidity_tolerance_atr": 0.28,
    "min_sweep_atr": 0.085,
    "min_rel_volume": 0.78,
    "min_confirm_rel_volume": 0.65,
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
    "components": [
        "liquidity_zone",
        "minimum_sweep",
        "recovery_close",
        "relative_volume",
        "confirmation_candle",
        "ema_reclaim_filter",
        "htf_context",
        "barrier_room",
        "rr_filter",
    ],
}

PLUS_PROFILE = {
    "name": "plus",
    "score": 82.0,
    "atr_pct_min": 0.0011,
    "atr_pct_max": 0.0175,
    "liquidity_tolerance_atr": 0.34,
    "min_sweep_atr": 0.085,
    "min_rel_volume": 0.78,
    "min_confirm_rel_volume": 0.48,
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
    "components": [
        "liquidity_zone",
        "minimum_sweep",
        "recovery_close",
        "relative_volume",
        "confirmation_candle",
        "ema_reclaim_filter",
        "htf_context",
        "barrier_room",
        "rr_filter",
    ],
}

FREE_PROFILE = {
    "name": "free",
    "score": 74.0,
    "atr_pct_min": 0.0008,
    "atr_pct_max": 0.0195,
    "liquidity_tolerance_atr": 0.40,
    "min_sweep_atr": 0.06,
    "min_rel_volume": 0.78,
    "min_confirm_rel_volume": 0.48,
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
    "components": [
        "liquidity_zone",
        "minimum_sweep",
        "recovery_close",
        "relative_volume",
        "confirmation_candle",
        "ema_reclaim_filter",
        "htf_context",
        "barrier_room",
        "rr_filter",
    ],
}

PROFILES = [PREMIUM_PROFILE, PLUS_PROFILE, FREE_PROFILE]

# =======================================
# PERFILES DE TRADING
# Mismo SL estructural; cambian objetivos y apalancamiento.
# =======================================

TRADING_PROFILES = {
    "conservador": {
        "leverage": "20x-30x",
        "tp1_rr": 1.50,
        "tp2_rr": 1.95,
    },
    "moderado": {
        "leverage": "30x-40x",
        "tp1_rr": 1.75,
        "tp2_rr": 2.35,
    },
    "agresivo": {
        "leverage": "40x-50x",
        "tp1_rr": 2.05,
        "tp2_rr": 2.75,
    },
}


# =======================================
# INDICADORES Y HELPERS DE VELA
# =======================================

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


def _normalize_price(value: float, decimals: int = 8) -> float:
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
            zones.append(
                {
                    "price": price,
                    "prices": [price],
                    "indices": [idx],
                }
            )

    for zone in zones:
        zone["count"] = len(zone["prices"])
        zone["latest_index"] = max(zone["indices"])

    return zones


def _select_liquidity_zone(
    historical: pd.DataFrame,
    direction: str,
    sweep_candle: pd.Series,
    profile: Dict,
) -> Optional[Dict]:
    atr = float(sweep_candle["atr"])
    if atr <= 0:
        return None

    tolerance = atr * float(profile["liquidity_tolerance_atr"])
    min_sweep = atr * float(profile["min_sweep_atr"])

    if direction == "SHORT":
        pivots = _find_pivots(historical["high"], "high", PIVOT_WINDOW)
    else:
        pivots = _find_pivots(historical["low"], "low", PIVOT_WINDOW)

    if len(pivots) < int(profile["min_pivots"]):
        return None

    candidates: List[Dict] = []
    for zone in _cluster_pivots(pivots, tolerance):
        if zone["count"] < int(profile["min_pivots"]):
            continue

        zone_price = float(zone["price"])
        if direction == "SHORT":
            sweep_ok = (
                float(sweep_candle["high"]) >= zone_price + min_sweep
                and float(sweep_candle["close"]) < zone_price
            )
            distance = abs(float(sweep_candle["high"]) - zone_price)
        else:
            sweep_ok = (
                float(sweep_candle["low"]) <= zone_price - min_sweep
                and float(sweep_candle["close"]) > zone_price
            )
            distance = abs(zone_price - float(sweep_candle["low"]))

        if not sweep_ok:
            continue

        zone["distance"] = distance
        candidates.append(zone)

    if not candidates:
        return None

    candidates.sort(
        key=lambda zone: (
            -int(zone["count"]),
            float(zone["distance"]),
            -int(zone["latest_index"]),
        )
    )
    return candidates[0]


def _recovery_candle_ok(
    sweep_candle: pd.Series,
    direction: str,
    profile: Dict,
    zone_price: float,
) -> bool:
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


def _confirmation_candle_ok(confirm_candle: pd.Series, sweep_candle: pd.Series, direction: str, profile: Dict) -> bool:
    body_ratio = float(confirm_candle["body_ratio"])
    rel_volume = float(confirm_candle["rel_volume"])
    min_body_ratio = float(profile["min_confirm_body_ratio"])
    min_rel_volume = float(profile["min_confirm_rel_volume"])

    body_ok = body_ratio >= min_body_ratio
    hybrid_ok = rel_volume >= min_rel_volume and body_ratio >= max(0.10, min_body_ratio * 0.70)

    if not (body_ok or hybrid_ok):
        return False

    if direction == "SHORT":
        bearish_close = float(confirm_candle["close"]) < float(confirm_candle["open"])
        follow_through = (
            float(confirm_candle["close"]) <= float(sweep_candle["close"])
            or float(confirm_candle["low"]) < float(sweep_candle["low"])
        )
        return bearish_close and follow_through

    bullish_close = float(confirm_candle["close"]) > float(confirm_candle["open"])
    follow_through = (
        float(confirm_candle["close"]) >= float(sweep_candle["close"])
        or float(confirm_candle["high"]) > float(sweep_candle["high"])
    )
    return bullish_close and follow_through


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
        clearly_against = (
            close < ema50 - (countertrend * 1.85)
            and close < ema20 - (countertrend * 1.35)
        )
        stretched_against = close < ema20 - (countertrend * 2.20)
        if heavy_bearish_bias and (clearly_against or stretched_against):
            return False
        return True

    heavy_bullish_bias = ema20 > ema50 and slope20 > 0
    clearly_against = (
        close > ema50 + (countertrend * 1.85)
        and close > ema20 + (countertrend * 1.35)
    )
    stretched_against = close > ema20 + (countertrend * 2.20)
    if heavy_bullish_bias and (clearly_against or stretched_against):
        return False
    return True


def _room_to_target(entry_price: float, stop_loss: float, structure_target: float, direction: str) -> float:
    risk = abs(stop_loss - entry_price)
    if risk <= 0:
        return 0.0

    if direction == "SHORT":
        room = entry_price - structure_target
    else:
        room = structure_target - entry_price

    return max(0.0, room / risk)


def _nearest_barrier_price(
    historical: pd.DataFrame,
    df_1h: pd.DataFrame,
    entry_price: float,
    direction: str,
) -> Optional[float]:
    candidates: List[float] = []

    htf = df_1h
    if "ema20" not in htf.columns or "ema50" not in htf.columns:
        htf = add_indicators(df_1h)

    if direction == "LONG":
        pivot_prices = [
            price
            for _, price in _find_pivots(historical["high"], "high", max(2, PIVOT_WINDOW - 1))
            if price > entry_price
        ]
        candidates.extend(pivot_prices)
        for value in [
            float(htf.iloc[-2]["ema20"]),
            float(htf.iloc[-2]["ema50"]),
        ]:
            if value > entry_price:
                candidates.append(value)
        return min(candidates) if candidates else None

    pivot_prices = [
        price
        for _, price in _find_pivots(historical["low"], "low", max(2, PIVOT_WINDOW - 1))
        if price < entry_price
    ]
    candidates.extend(pivot_prices)
    for value in [
        float(htf.iloc[-2]["ema20"]),
        float(htf.iloc[-2]["ema50"]),
    ]:
        if value < entry_price:
            candidates.append(value)
    return max(candidates) if candidates else None


def _tp_from_rr(entry_price: float, risk: float, rr: float, direction: str) -> float:
    if direction == "LONG":
        return entry_price + (risk * rr)
    return entry_price - (risk * rr)


def _build_trade_profiles(
    entry_price: float,
    direction: str,
    stop_loss: float,
    max_room_rr: float,
) -> Dict[str, Dict]:
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


def _closed_15m_frame(df_15m: pd.DataFrame) -> pd.DataFrame:
    if df_15m.empty or "close_time" not in df_15m.columns:
        return df_15m.copy()

    now_utc = pd.Timestamp.now(tz="UTC")
    closed = df_15m[df_15m["close_time"] <= now_utc].copy()
    return closed if not closed.empty else df_15m.iloc[:-1].copy()




def _debug_fail(debug_counts: Optional[Dict[str, int]], reason: str) -> None:
    if debug_counts is None:
        return
    debug_counts[reason] = int(debug_counts.get(reason, 0)) + 1

def _market_entry_candidate(
    current_price: float,
    stop_loss: float,
    direction: str,
    structure_target: float,
    nearest_barrier: Optional[float],
    profile: Dict,
) -> Optional[Tuple[float, Dict[str, Dict], float, float]]:
    entry_price = float(current_price)

    if direction == "LONG":
        if entry_price <= stop_loss or entry_price >= structure_target:
            return None
    else:
        if entry_price >= stop_loss or entry_price <= structure_target:
            return None

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


def _safe_ratio(value: float, baseline: float) -> float:
    if baseline <= 1e-9:
        return 0.0
    return round(float(value) / float(baseline), 2)


def _htf_context_snapshot(df_1h: pd.DataFrame, direction: str, profile: Dict) -> Dict[str, float | bool]:
    if len(df_1h) < max(HTF_LOOKBACK, 60):
        return {"ok": False, "raw": 0.0, "normalized": 0.0}

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
        raw = (close - ema20) / atr
        heavy_bearish_bias = ema20 < ema50 and slope20 < 0
        clearly_against = (
            close < ema50 - (countertrend * 1.85)
            and close < ema20 - (countertrend * 1.35)
        )
        stretched_against = close < ema20 - (countertrend * 2.20)
        ok = not (heavy_bearish_bias and (clearly_against or stretched_against))
        normalized = round(max(0.0, ((close - (ema20 - countertrend)) / atr)), 2)
        return {"ok": ok, "raw": round(raw, 2), "normalized": normalized}

    raw = (ema20 - close) / atr
    heavy_bullish_bias = ema20 > ema50 and slope20 > 0
    clearly_against = (
        close > ema50 + (countertrend * 1.85)
        and close > ema20 + (countertrend * 1.35)
    )
    stretched_against = close > ema20 + (countertrend * 2.20)
    ok = not (heavy_bullish_bias and (clearly_against or stretched_against))
    normalized = round(max(0.0, (((ema20 + countertrend) - close) / atr)), 2)
    return {"ok": ok, "raw": round(raw, 2), "normalized": normalized}


def _build_component_breakdown(
    *,
    direction: str,
    profile: Dict,
    zone: Dict,
    zone_price: float,
    sweep_candle: pd.Series,
    confirm_candle: pd.Series,
    atr: float,
    rel_volume: float,
    room_rr: float,
    barrier_rr: float,
    htf_snapshot: Dict[str, float | bool],
) -> tuple[list[Dict[str, float | str]], list[Dict[str, float | str]], list[Dict[str, float | str]]]:
    if direction == "SHORT":
        sweep_distance_atr = abs(float(sweep_candle["high"]) - zone_price) / max(atr, 1e-9)
        reclaim_margin_atr = (float(confirm_candle["ema20"]) + (atr * float(profile["ema_reclaim_buffer_atr"])) - float(confirm_candle["close"])) / max(atr, 1e-9)
    else:
        sweep_distance_atr = abs(zone_price - float(sweep_candle["low"])) / max(atr, 1e-9)
        reclaim_margin_atr = (float(confirm_candle["close"]) - (float(confirm_candle["ema20"]) - (atr * float(profile["ema_reclaim_buffer_atr"])))) / max(atr, 1e-9)

    wick_body_ratio = max(
        float(sweep_candle["upper_wick"] if direction == "SHORT" else sweep_candle["lower_wick"]),
        0.0,
    ) / max(float(sweep_candle["body"]), 1e-9)
    wick_range_ratio = max(
        float(sweep_candle["upper_wick"] if direction == "SHORT" else sweep_candle["lower_wick"]),
        0.0,
    ) / max(float(sweep_candle["range"]), 1e-9)
    recovery_strength = round(min(wick_body_ratio, wick_range_ratio), 2)
    recovery_normalized = round(
        (wick_body_ratio / max(float(profile["min_wick_body_ratio"]), 1e-9)
         + wick_range_ratio / max(float(profile["min_wick_range_ratio"]), 1e-9)) / 2.0,
        2,
    )

    confirm_body_ratio = float(confirm_candle["body_ratio"])
    confirm_rel_volume = float(confirm_candle["rel_volume"])
    confirmation_strength = round(confirm_body_ratio, 2)
    confirmation_normalized = round(
        (confirm_body_ratio / max(float(profile["min_confirm_body_ratio"]), 1e-9)
         + confirm_rel_volume / max(float(profile["min_confirm_rel_volume"]), 1e-9)) / 2.0,
        2,
    )

    raw_components = [
        {"label": "liquidity_zone", "points": round(float(zone.get("count") or 0), 2)},
        {"label": "minimum_sweep", "points": round(float(sweep_distance_atr), 2)},
        {"label": "recovery_close", "points": recovery_strength},
        {"label": "relative_volume", "points": round(float(rel_volume), 2)},
        {"label": "confirmation_candle", "points": confirmation_strength},
        {"label": "ema_reclaim_filter", "points": round(float(reclaim_margin_atr), 2)},
        {"label": "htf_context", "points": round(float(htf_snapshot.get("raw") or 0.0), 2)},
        {"label": "barrier_room", "points": round(float(barrier_rr), 2)},
        {"label": "rr_filter", "points": round(float(room_rr), 2)},
    ]

    normalized_components = [
        {"label": "liquidity_zone", "points": _safe_ratio(float(zone.get("count") or 0.0), float(profile["min_pivots"]))},
        {"label": "minimum_sweep", "points": _safe_ratio(float(sweep_distance_atr), float(profile["min_sweep_atr"]))},
        {"label": "recovery_close", "points": recovery_normalized},
        {"label": "relative_volume", "points": _safe_ratio(float(rel_volume), float(profile["min_rel_volume"]))},
        {"label": "confirmation_candle", "points": confirmation_normalized},
        {"label": "ema_reclaim_filter", "points": round(max(0.0, float(reclaim_margin_atr)), 2)},
        {"label": "htf_context", "points": round(float(htf_snapshot.get("normalized") or 0.0), 2)},
        {"label": "barrier_room", "points": _safe_ratio(float(barrier_rr), float(profile["min_barrier_rr"]))},
        {"label": "rr_filter", "points": _safe_ratio(float(room_rr), float(profile["min_rr"]))},
    ]
    return normalized_components, raw_components, normalized_components


def _evaluate_direction(
    df: pd.DataFrame,
    df_1h: pd.DataFrame,
    direction: str,
    profile: Dict,
    current_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Tuple[Dict, Tuple]]:
    sweep_candle = df.iloc[-2]
    confirm_candle = df.iloc[-1]
    historical = df.iloc[:-2].tail(LIQUIDITY_LOOKBACK)

    if len(historical) < LIQUIDITY_LOOKBACK:
        _debug_fail(debug_counts, "history")
        return None

    atr = float(sweep_candle["atr"])
    atr_pct = float(sweep_candle["atr_pct"])
    if atr <= 0 or not (float(profile["atr_pct_min"]) <= atr_pct <= float(profile["atr_pct_max"])):
        _debug_fail(debug_counts, "atr_pct")
        return None

    rel_volume = max(float(sweep_candle["rel_volume"]), float(confirm_candle["rel_volume"]))
    if rel_volume < float(profile["min_rel_volume"]):
        _debug_fail(debug_counts, "rel_volume")
        return None

    sweep_range_atr = float(sweep_candle["range"]) / atr
    if not (float(profile["min_sweep_range_atr"]) <= sweep_range_atr <= float(profile["max_sweep_range_atr"])):
        _debug_fail(debug_counts, "sweep_range")
        return None

    htf_snapshot = _htf_context_snapshot(df_1h, direction, profile)
    if not bool(htf_snapshot.get("ok")):
        _debug_fail(debug_counts, "htf_context")
        return None

    zone = _select_liquidity_zone(historical, direction, sweep_candle, profile)
    if not zone:
        _debug_fail(debug_counts, "liquidity_zone")
        return None

    zone_price = float(zone["price"])

    if not _recovery_candle_ok(sweep_candle, direction, profile, zone_price):
        _debug_fail(debug_counts, "recovery_close")
        return None

    if not _confirmation_candle_ok(confirm_candle, sweep_candle, direction, profile):
        _debug_fail(debug_counts, "confirmation_candle")
        return None

    if not _ema_reclaim_ok(confirm_candle, direction, profile):
        _debug_fail(debug_counts, "ema_reclaim")
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
        _debug_fail(debug_counts, "risk_invalid")
        return None

    risk_pct = risk / max(model_entry, 1e-9)
    if risk_pct > float(profile["max_risk_pct"]):
        _debug_fail(debug_counts, "risk_pct")
        return None

    room_rr = _room_to_target(model_entry, stop_loss, structure_target, direction)
    if room_rr < float(profile["min_rr"]):
        _debug_fail(debug_counts, "rr_filter")
        return None

    model_nearest_barrier = _nearest_barrier_price(historical, df_1h, model_entry, direction)
    model_barrier_rr = room_rr
    if model_nearest_barrier is not None:
        model_barrier_rr = _room_to_target(model_entry, stop_loss, model_nearest_barrier, direction)
        if model_barrier_rr < float(profile["min_barrier_rr"]):
            _debug_fail(debug_counts, "barrier_room")
            return None

    market_price = float(current_market_price) if current_market_price is not None else float(confirm_candle["close"])
    market_nearest_barrier = _nearest_barrier_price(historical, df_1h, market_price, direction)
    market_candidate = _market_entry_candidate(
        current_price=market_price,
        stop_loss=stop_loss,
        direction=direction,
        structure_target=structure_target,
        nearest_barrier=market_nearest_barrier,
        profile=profile,
    )
    if market_candidate is None:
        _debug_fail(debug_counts, "market_entry")
        return None

    entry_price, trade_profiles, active_room_rr, active_barrier_rr = market_candidate
    model_trade_profiles = _build_trade_profiles(model_entry, direction, stop_loss, room_rr)
    model_tp1 = float(model_trade_profiles["conservador"]["take_profits"][0])

    components, raw_components, normalized_components = _build_component_breakdown(
        direction=direction,
        profile=profile,
        zone=zone,
        zone_price=zone_price,
        sweep_candle=sweep_candle,
        confirm_candle=confirm_candle,
        atr=atr,
        rel_volume=rel_volume,
        room_rr=room_rr,
        barrier_rr=model_barrier_rr,
        htf_snapshot=htf_snapshot,
    )

    result = {
        "strategy_name": STRATEGY_NAME,
        "direction": direction,
        "entry_price": _normalize_price(entry_price),
        "entry_model_price": _normalize_price(model_entry),
        "entry_sent_price": _normalize_price(entry_price),
        "stop_loss": trade_profiles["conservador"]["stop_loss"],
        "take_profits": list(trade_profiles["conservador"]["take_profits"]),
        "profiles": trade_profiles,
        "score": round(float(profile["score"]), 2),
        "raw_score": round(float(profile["score"]), 2),
        "normalized_score": round(float(profile["score"]), 2),
        "components": components,
        "raw_components": raw_components,
        "normalized_components": normalized_components,
        "timeframes": ["15M"],
        "setup_group": str(profile["name"]),
        "candidate_tier": str(profile["name"]),
        "final_tier": str(profile["name"]),
        "atr_pct": round(atr_pct, 6),
        "score_profile": str(profile["name"]),
        "score_calibration": SCORE_CALIBRATION_VERSION,
        "entry_model": "liquidity_zone_offset_v1",
        "send_mode": "market_on_close",
        "setup_stage": "closed_confirmed",
        "tp1_progress_at_send_pct": _progress_from_model_to_tp1_pct(model_entry, model_tp1, entry_price, direction),
        "r_progress_at_send": _r_progress_from_model_entry(model_entry, stop_loss, entry_price, direction),
    }

    ranking = (
        int(zone["count"]),
        round(active_room_rr, 4),
        round(active_barrier_rr, 4),
        round(rel_volume, 4),
    )
    return result, ranking


def _evaluate_profile(
    df: pd.DataFrame,
    df_1h: pd.DataFrame,
    profile: Dict,
    current_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Dict]:
    best_result: Optional[Dict] = None
    best_rank: Optional[Tuple] = None

    for direction in ("SHORT", "LONG"):
        evaluated = _evaluate_direction(df, df_1h, direction, profile, current_market_price=current_market_price, debug_counts=debug_counts)
        if not evaluated:
            continue

        result, rank = evaluated
        if best_rank is None or rank > best_rank:
            best_result = result
            best_rank = rank

    return best_result


# =======================================
# ESTRATEGIA PRINCIPAL
# =======================================

def liquidity_sweep_reversal_strategy(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame] = None,
    reference_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Dict]:
    closed_15m = _closed_15m_frame(df_15m)
    if len(closed_15m) < MIN_HISTORY_BARS or len(df_1h) < 60:
        return None

    df = add_indicators(closed_15m)
    if len(df) < MIN_HISTORY_BARS:
        return None

    if df[["atr", "atr_pct", "rel_volume", "body_ratio", "ema20", "ema50"]].tail(5).isnull().any().any():
        return None

    current_market_price: Optional[float] = None
    if reference_market_price is not None:
        try:
            current_market_price = float(reference_market_price)
        except Exception:
            current_market_price = None
    if current_market_price is None and df_5m is not None and len(df_5m) > 0:
        try:
            current_market_price = float(df_5m.iloc[-1]["close"])
        except Exception:
            current_market_price = None

    for profile in PROFILES:
        result = _evaluate_profile(df, df_1h, profile, current_market_price=current_market_price, debug_counts=debug_counts)
        if result:
            return result

    return None


# =======================================
# COMPATIBILIDAD HACIA ATRÁS
# El scanner viejo sigue llamando mtf_strategy().
# Internamente ya no usa MTF como estrategia, pero sí usa 1H
# como filtro de contexto y 15M como timeframe operativo.
# =======================================

def mtf_strategy(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame] = None,
    reference_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Dict]:
    return liquidity_sweep_reversal_strategy(
        df_1h,
        df_15m,
        df_5m,
        reference_market_price=reference_market_price,
        debug_counts=debug_counts,
    )
