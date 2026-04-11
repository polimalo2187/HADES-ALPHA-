from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import os
import pandas as pd

from app import strategy_breakout_reset as breakout

LIQUIDITY_LOOKBACK = max(24, int(os.getenv("LIQUIDITY_LOOKBACK", "36")))
LIQUIDITY_PIVOT_LEFT = max(1, int(os.getenv("LIQUIDITY_PIVOT_LEFT", "2")))
LIQUIDITY_PIVOT_RIGHT = max(1, int(os.getenv("LIQUIDITY_PIVOT_RIGHT", "2")))
SWEEP_SEARCH_BARS = max(2, int(os.getenv("LIQUIDITY_SWEEP_SEARCH_BARS", "4")))
MIN_HISTORY_BARS = max(LIQUIDITY_LOOKBACK + 4, 40)
SCORE_CALIBRATION_VERSION = "v1_liquidity_sweep_regime_router"
ENTRY_MODEL_NAME = "liquidity_sweep_reversal_close_confirm_v1"
SETUP_STAGE_CLOSED_CONFIRMED = "closed_confirmed"
SEND_MODE_MARKET_ON_CLOSE = "market_on_close"

PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "83"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "76"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "69"))


FREE_PROFILE = {
    "name": "free",
    "atr_pct_min": breakout._env_float("LSR_FREE_ATR_PCT_MIN", 0.0022),
    "atr_pct_max": breakout._env_float("LSR_FREE_ATR_PCT_MAX", 0.0200),
    "liquidity_tolerance_atr": breakout._env_float("LSR_FREE_LIQUIDITY_TOLERANCE_ATR", 0.38),
    "min_sweep_atr": breakout._env_float("LSR_FREE_MIN_SWEEP_ATR", 0.22),
    "min_rel_volume": breakout._env_float("LSR_FREE_MIN_REL_VOLUME", 0.95),
    "min_confirm_rel_volume": breakout._env_float("LSR_FREE_MIN_CONFIRM_REL_VOLUME", 1.00),
    "min_body_ratio_recovery": breakout._env_float("LSR_FREE_MIN_BODY_RATIO_RECOVERY", 0.26),
    "min_body_ratio_confirmation": breakout._env_float("LSR_FREE_MIN_BODY_RATIO_CONFIRMATION", 0.24),
    "min_close_position": breakout._env_float("LSR_FREE_MIN_CLOSE_POSITION", 0.58),
    "min_rr": breakout._env_float("LSR_FREE_MIN_RR", 1.00),
    "score": 78.0,
}

PLUS_PROFILE = {
    "name": "plus",
    "atr_pct_min": breakout._env_float("LSR_PLUS_ATR_PCT_MIN", 0.0025),
    "atr_pct_max": breakout._env_float("LSR_PLUS_ATR_PCT_MAX", 0.0180),
    "liquidity_tolerance_atr": breakout._env_float("LSR_PLUS_LIQUIDITY_TOLERANCE_ATR", 0.32),
    "min_sweep_atr": breakout._env_float("LSR_PLUS_MIN_SWEEP_ATR", 0.28),
    "min_rel_volume": breakout._env_float("LSR_PLUS_MIN_REL_VOLUME", 1.05),
    "min_confirm_rel_volume": breakout._env_float("LSR_PLUS_MIN_CONFIRM_REL_VOLUME", 1.10),
    "min_body_ratio_recovery": breakout._env_float("LSR_PLUS_MIN_BODY_RATIO_RECOVERY", 0.30),
    "min_body_ratio_confirmation": breakout._env_float("LSR_PLUS_MIN_BODY_RATIO_CONFIRMATION", 0.28),
    "min_close_position": breakout._env_float("LSR_PLUS_MIN_CLOSE_POSITION", 0.64),
    "min_rr": breakout._env_float("LSR_PLUS_MIN_RR", 1.10),
    "score": 86.0,
}

PREMIUM_PROFILE = {
    "name": "premium",
    "atr_pct_min": breakout._env_float("LSR_PREMIUM_ATR_PCT_MIN", 0.0028),
    "atr_pct_max": breakout._env_float("LSR_PREMIUM_ATR_PCT_MAX", 0.0160),
    "liquidity_tolerance_atr": breakout._env_float("LSR_PREMIUM_LIQUIDITY_TOLERANCE_ATR", 0.28),
    "min_sweep_atr": breakout._env_float("LSR_PREMIUM_MIN_SWEEP_ATR", 0.34),
    "min_rel_volume": breakout._env_float("LSR_PREMIUM_MIN_REL_VOLUME", 1.12),
    "min_confirm_rel_volume": breakout._env_float("LSR_PREMIUM_MIN_CONFIRM_REL_VOLUME", 1.18),
    "min_body_ratio_recovery": breakout._env_float("LSR_PREMIUM_MIN_BODY_RATIO_RECOVERY", 0.34),
    "min_body_ratio_confirmation": breakout._env_float("LSR_PREMIUM_MIN_BODY_RATIO_CONFIRMATION", 0.30),
    "min_close_position": breakout._env_float("LSR_PREMIUM_MIN_CLOSE_POSITION", 0.70),
    "min_rr": breakout._env_float("LSR_PREMIUM_MIN_RR", 1.18),
    "score": 90.0,
}


PROFILES = [PREMIUM_PROFILE, PLUS_PROFILE, FREE_PROFILE]


def _record_reject(debug_counts: Optional[Dict[str, int]], reason: str) -> None:
    if debug_counts is None:
        return
    key = str(reason or "unknown").strip() or "unknown"
    debug_counts[key] = int(debug_counts.get(key, 0)) + 1



def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame["ema20"] = frame["close"].astype(float).ewm(span=20, adjust=False, min_periods=20).mean()
    frame["ema50"] = frame["close"].astype(float).ewm(span=50, adjust=False, min_periods=30).mean()
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    close = frame["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    frame["atr"] = tr.rolling(window=14, min_periods=14).mean()
    frame["atr_pct"] = frame["atr"] / close.replace(0, pd.NA)
    frame["body"] = (close - frame["open"].astype(float)).abs()
    frame["range"] = (high - low).replace(0, 1e-9)
    frame["body_ratio"] = frame["body"] / frame["range"]
    frame["rel_volume"] = frame["volume"].astype(float) / frame["volume"].astype(float).rolling(20, min_periods=5).mean().replace(0, pd.NA)
    frame["upper_wick"] = (high - frame[["open", "close"]].max(axis=1)).clip(lower=0.0) / frame["range"]
    frame["lower_wick"] = (frame[["open", "close"]].min(axis=1) - low).clip(lower=0.0) / frame["range"]
    frame["close_position"] = (close - low) / frame["range"]
    return frame



def _indicators_ready(last: pd.Series) -> bool:
    try:
        for key in ["atr", "atr_pct", "ema20", "ema50", "body_ratio", "rel_volume", "close_position"]:
            value = float(last.get(key))
            if not pd.notna(value):
                return False
        return True
    except Exception:
        return False



def _closed_15m_frame(df_15m: pd.DataFrame) -> pd.DataFrame:
    if df_15m is None or df_15m.empty or "close_time" not in df_15m.columns:
        return (df_15m or pd.DataFrame()).copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    closed = df_15m[df_15m["close_time"] <= now_utc].copy()
    if not closed.empty:
        return closed
    if len(df_15m) > 1:
        return df_15m.iloc[:-1].copy()
    return df_15m.copy()



def _min_raw_score_for_profile(profile_name: str) -> float:
    if profile_name == PREMIUM_PROFILE["name"]:
        return PREMIUM_RAW_SCORE_MIN
    if profile_name == PLUS_PROFILE["name"]:
        return PLUS_RAW_SCORE_MIN
    return FREE_RAW_SCORE_MIN



def _higher_timeframe_context_ok(df_1h: pd.DataFrame, direction: str) -> bool:
    if df_1h is None or df_1h.empty or len(df_1h) < 60:
        return True
    enriched = add_indicators(df_1h)
    last = enriched.iloc[-1]
    if not _indicators_ready(last):
        return True
    close = float(last["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    if direction == "LONG":
        return close >= ema20 and ema20 >= ema50
    return close <= ema20 and ema20 <= ema50



def _is_pivot_low(df: pd.DataFrame, idx: int, left: int, right: int) -> bool:
    low = float(df.iloc[idx]["low"])
    window = df.iloc[idx - left: idx + right + 1]
    return low <= float(window["low"].min())



def _is_pivot_high(df: pd.DataFrame, idx: int, left: int, right: int) -> bool:
    high = float(df.iloc[idx]["high"])
    window = df.iloc[idx - left: idx + right + 1]
    return high >= float(window["high"].max())



def _select_liquidity_zone(df: pd.DataFrame, direction: str, profile: Dict) -> Optional[Dict]:
    if len(df) < max(LIQUIDITY_LOOKBACK, LIQUIDITY_PIVOT_LEFT + LIQUIDITY_PIVOT_RIGHT + 5):
        return None
    frame = df.iloc[-(LIQUIDITY_LOOKBACK + LIQUIDITY_PIVOT_RIGHT + 3):-2].copy()
    if frame.empty:
        return None
    atr_now = float(df.iloc[-1]["atr"])
    tolerance = max(1e-9, atr_now * float(profile["liquidity_tolerance_atr"]))
    pivot_rows: List[Dict] = []
    for idx in range(LIQUIDITY_PIVOT_LEFT, len(frame) - LIQUIDITY_PIVOT_RIGHT):
        absolute_idx = int(frame.index[idx])
        if direction == "LONG" and _is_pivot_low(frame.reset_index(drop=True), idx, LIQUIDITY_PIVOT_LEFT, LIQUIDITY_PIVOT_RIGHT):
            pivot_rows.append({"price": float(frame.iloc[idx]["low"]), "index": absolute_idx})
        elif direction == "SHORT" and _is_pivot_high(frame.reset_index(drop=True), idx, LIQUIDITY_PIVOT_LEFT, LIQUIDITY_PIVOT_RIGHT):
            pivot_rows.append({"price": float(frame.iloc[idx]["high"]), "index": absolute_idx})
    if not pivot_rows:
        if direction == "LONG":
            fallback_price = float(frame["low"].tail(12).min())
        else:
            fallback_price = float(frame["high"].tail(12).max())
        return {"price": fallback_price, "count": 1, "latest_index": int(frame.index[-1]), "tolerance": tolerance}

    pivot_rows.sort(key=lambda item: item["price"])
    clusters: List[Dict] = []
    for pivot in pivot_rows:
        matched = None
        for cluster in clusters:
            if abs(float(cluster["price"]) - float(pivot["price"])) <= tolerance:
                matched = cluster
                break
        if matched is None:
            clusters.append({
                "price": float(pivot["price"]),
                "count": 1,
                "latest_index": int(pivot["index"]),
                "members": [pivot],
            })
        else:
            matched["members"].append(pivot)
            matched["count"] += 1
            matched["latest_index"] = max(int(matched["latest_index"]), int(pivot["index"]))
            matched["price"] = sum(float(member["price"]) for member in matched["members"]) / len(matched["members"])

    clusters.sort(key=lambda c: (int(c["count"]), int(c["latest_index"])), reverse=True)
    best = clusters[0]
    return {
        "price": breakout._round_price_dynamic(float(best["price"])),
        "count": int(best["count"]),
        "latest_index": int(best["latest_index"]),
        "tolerance": tolerance,
    }



def _recovery_candle_ok(last: pd.Series, direction: str, level: float, profile: Dict) -> bool:
    body_ratio = float(last["body_ratio"])
    rel_volume = float(last.get("rel_volume") or 0.0)
    close = float(last["close"])
    close_position = float(last["close_position"])
    threshold = float(profile["min_close_position"])
    if direction == "LONG":
        return close > level and body_ratio >= float(profile["min_body_ratio_recovery"]) and rel_volume >= float(profile["min_rel_volume"]) and close_position >= threshold
    return close < level and body_ratio >= float(profile["min_body_ratio_recovery"]) and rel_volume >= float(profile["min_rel_volume"]) and (1.0 - close_position) >= threshold



def _confirmation_candle_ok(last: pd.Series, direction: str, level: float, profile: Dict) -> bool:
    body_ratio = float(last["body_ratio"])
    rel_volume = float(last.get("rel_volume") or 0.0)
    close = float(last["close"])
    close_position = float(last["close_position"])
    threshold = max(0.50, float(profile["min_close_position"]) - 0.04)
    if direction == "LONG":
        return close >= level and body_ratio >= float(profile["min_body_ratio_confirmation"]) and rel_volume >= float(profile["min_confirm_rel_volume"]) and close_position >= threshold
    return close <= level and body_ratio >= float(profile["min_body_ratio_confirmation"]) and rel_volume >= float(profile["min_confirm_rel_volume"]) and (1.0 - close_position) >= threshold



def _ema_reclaim_ok(last: pd.Series, direction: str) -> bool:
    close = float(last["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    if direction == "LONG":
        return close >= ema20 and close >= ema50 * 0.998
    return close <= ema20 and close <= ema50 * 1.002



def _find_recent_sweep(df: pd.DataFrame, direction: str, level: float, profile: Dict) -> Optional[Dict]:
    tail = df.tail(SWEEP_SEARCH_BARS).copy()
    if tail.empty:
        return None
    atr_now = float(df.iloc[-1]["atr"])
    min_sweep_distance = atr_now * float(profile["min_sweep_atr"])
    if direction == "LONG":
        low_price = float(tail["low"].min())
        if low_price >= (level - min_sweep_distance):
            return None
        idx = int(tail["low"].idxmin())
        candle = df.loc[idx]
        if float(candle["close"]) <= level:
            return None
        return {
            "index": idx,
            "sweep_distance_atr": round((level - low_price) / max(atr_now, 1e-9), 4),
            "wick_ratio": round(float(candle.get("lower_wick") or 0.0), 4),
        }
    high_price = float(tail["high"].max())
    if high_price <= (level + min_sweep_distance):
        return None
    idx = int(tail["high"].idxmax())
    candle = df.loc[idx]
    if float(candle["close"]) >= level:
        return None
    return {
        "index": idx,
        "sweep_distance_atr": round((high_price - level) / max(atr_now, 1e-9), 4),
        "wick_ratio": round(float(candle.get("upper_wick") or 0.0), 4),
    }



def _nearest_barrier_price(df: pd.DataFrame, direction: str, entry_price: float) -> Optional[float]:
    window = df.tail(18)
    if window.empty:
        return None
    if direction == "LONG":
        barrier = float(window["high"].max())
        return barrier if barrier > entry_price else None
    barrier = float(window["low"].min())
    return barrier if barrier < entry_price else None



def _safe_rr(entry_price: float, stop_loss: float, target_price: float) -> float:
    risk = abs(stop_loss - entry_price)
    if risk <= 1e-9:
        return 0.0
    return abs(target_price - entry_price) / risk



def _component(name: str, points: float, value: float, passed: bool) -> Dict:
    return {
        "name": name,
        "points": round(points if passed else 0.0, 2),
        "value": round(float(value), 4),
        "passed": bool(passed),
    }



def _score_components(last: pd.Series, direction: str, zone: Dict, sweep: Dict, barrier_rr: float, profile: Dict) -> List[Dict]:
    rel_volume = float(last.get("rel_volume") or 0.0)
    body_ratio = float(last.get("body_ratio") or 0.0)
    close_position = float(last.get("close_position") or 0.0)
    confirmation_volume_ok = rel_volume >= float(profile["min_confirm_rel_volume"])
    close_pos_value = close_position if direction == "LONG" else (1.0 - close_position)

    return [
        _component("liquidity_zone", 18.0, float(zone.get("count") or 0.0), float(zone.get("count") or 0.0) >= 1.0),
        _component("minimum_sweep", 18.0, float(sweep.get("sweep_distance_atr") or 0.0), float(sweep.get("sweep_distance_atr") or 0.0) >= float(profile["min_sweep_atr"])),
        _component("recovery_close", 16.0, body_ratio, body_ratio >= float(profile["min_body_ratio_recovery"])),
        _component("confirmation_volume", 14.0, rel_volume, confirmation_volume_ok),
        _component("close_position", 12.0, close_pos_value, close_pos_value >= float(profile["min_close_position"])),
        _component("ema_reclaim", 10.0, float(last["close"]), True),
        _component("barrier_room_rr", 12.0, barrier_rr, barrier_rr >= float(profile["min_rr"])),
    ]



def _sum_components(components: List[Dict]) -> float:
    total = sum(float(item.get("points") or 0.0) for item in components)
    return round(breakout._clamp(total, 0.0, 100.0), 2)



def _evaluate_direction(
    df: pd.DataFrame,
    df_1h: pd.DataFrame,
    direction: str,
    profile: Dict,
    *,
    current_market_price: Optional[float],
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Tuple[Dict, Tuple[float, float, float]]]:
    if len(df) < MIN_HISTORY_BARS:
        _record_reject(debug_counts, "liquidity_insufficient_history")
        return None

    last = df.iloc[-1]
    if not _indicators_ready(last):
        _record_reject(debug_counts, "liquidity_indicator_warmup")
        return None

    atr_pct = float(last["atr_pct"])
    if not (float(profile["atr_pct_min"]) <= atr_pct <= float(profile["atr_pct_max"])):
        _record_reject(debug_counts, "liquidity_atr_pct")
        return None

    if not _higher_timeframe_context_ok(df_1h, direction):
        _record_reject(debug_counts, "liquidity_htf_context")
        return None

    zone = _select_liquidity_zone(df, direction, profile)
    if not zone:
        _record_reject(debug_counts, "liquidity_zone_missing")
        return None

    level = float(zone["price"])
    sweep = _find_recent_sweep(df, direction, level, profile)
    if not sweep:
        _record_reject(debug_counts, "liquidity_minimum_sweep")
        return None

    if not _recovery_candle_ok(last, direction, level, profile):
        _record_reject(debug_counts, "liquidity_recovery_close")
        return None

    if not _confirmation_candle_ok(last, direction, level, profile):
        _record_reject(debug_counts, "liquidity_confirmation")
        return None

    if not _ema_reclaim_ok(last, direction):
        _record_reject(debug_counts, "liquidity_ema_reclaim")
        return None

    market_entry = float(current_market_price or last["close"])
    if market_entry <= 0:
        market_entry = float(last["close"])

    trade_profiles = breakout._build_trade_profiles(market_entry, direction, atr_pct)
    conservative = trade_profiles.get("conservador") or {}
    stop_loss = float(conservative.get("stop_loss") or 0.0)
    take_profits = list(conservative.get("take_profits") or [])
    if stop_loss <= 0 or not take_profits:
        _record_reject(debug_counts, "liquidity_trade_profile")
        return None

    barrier_price = _nearest_barrier_price(df, direction, market_entry)
    barrier_rr = _safe_rr(market_entry, stop_loss, barrier_price) if barrier_price is not None else float(profile["min_rr"])
    if barrier_rr < float(profile["min_rr"]):
        _record_reject(debug_counts, "liquidity_barrier_room")
        return None

    components = _score_components(last, direction, zone, sweep, barrier_rr, profile)
    raw_score = _sum_components(components)
    if raw_score < _min_raw_score_for_profile(str(profile["name"])):
        _record_reject(debug_counts, "liquidity_score_floor")
        return None

    entry_model_price = float(last["close"])
    payload = {
        "direction": direction,
        "entry_price": breakout._round_price_dynamic(market_entry),
        "stop_loss": stop_loss,
        "take_profits": take_profits,
        "profiles": trade_profiles,
        "score": raw_score,
        "raw_score": raw_score,
        "normalized_score": raw_score,
        "components": components,
        "raw_components": components,
        "normalized_components": components,
        "timeframes": ["15M"],
        "setup_group": str(profile["name"]),
        "atr_pct": round(atr_pct, 6),
        "score_profile": str(profile["name"]),
        "score_calibration": SCORE_CALIBRATION_VERSION,
        "send_mode": SEND_MODE_MARKET_ON_CLOSE,
        "entry_model_price": breakout._round_price_dynamic(entry_model_price),
        "entry_sent_price": breakout._round_price_dynamic(market_entry),
        "setup_stage": SETUP_STAGE_CLOSED_CONFIRMED,
        "entry_model": ENTRY_MODEL_NAME,
        "higher_tf_context": {"liquidity_zone": level, "sweep_distance_atr": sweep.get("sweep_distance_atr")},
        "liquidity_zone": breakout._round_price_dynamic(level),
        "liquidity_sweep_distance_atr": sweep.get("sweep_distance_atr"),
    }
    ranking = (raw_score, barrier_rr, float(zone.get("count") or 0.0))
    return payload, ranking



def mtf_strategy(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame] = None,
    reference_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Dict]:
    closed_15m = _closed_15m_frame(df_15m)
    if len(closed_15m) < MIN_HISTORY_BARS:
        _record_reject(debug_counts, "liquidity_insufficient_history")
        return None

    enriched_15m = add_indicators(closed_15m)
    enriched_1h = add_indicators(df_1h) if df_1h is not None and not df_1h.empty else df_1h

    for profile in PROFILES:
        candidates: List[Tuple[Dict, Tuple[float, float, float]]] = []
        for direction in ("LONG", "SHORT"):
            result = _evaluate_direction(
                enriched_15m,
                enriched_1h,
                direction,
                profile,
                current_market_price=reference_market_price,
                debug_counts=debug_counts,
            )
            if result is not None:
                candidates.append(result)
        if candidates:
            candidates.sort(key=lambda item: item[1], reverse=True)
            return candidates[0][0]
    return None
