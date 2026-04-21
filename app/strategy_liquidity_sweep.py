from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import os
import pandas as pd

from app import strategy_breakout_reset as breakout

LIQUIDITY_LOOKBACK = max(24, int(os.getenv("LIQUIDITY_LOOKBACK", "36")))
LIQUIDITY_PIVOT_LEFT = max(1, int(os.getenv("LIQUIDITY_PIVOT_LEFT", "2")))
LIQUIDITY_PIVOT_RIGHT = max(1, int(os.getenv("LIQUIDITY_PIVOT_RIGHT", "2")))
SWEEP_SEARCH_BARS = max(2, int(os.getenv("LIQUIDITY_SWEEP_SEARCH_BARS", "4")))
MIN_HISTORY_BARS = max(LIQUIDITY_LOOKBACK + 4, 36)
SCORE_CALIBRATION_VERSION = "v4_1_liquidity_sweep_expiry_quality_tightening"
ENTRY_MODEL_NAME = "liquidity_sweep_reversal_pullback_execution_v2"
SETUP_STAGE_WAITING_PULLBACK = "confirmed_waiting_pullback"
SEND_MODE_PENDING_ENTRY = breakout.SEND_MODE_PENDING_ENTRY

PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "83"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "76"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "69"))


FREE_PROFILE = {
    "name": "free",
    "atr_pct_min": breakout._env_float("LSR_FREE_ATR_PCT_MIN", 0.0018),
    "atr_pct_max": breakout._env_float("LSR_FREE_ATR_PCT_MAX", 0.0220),
    "liquidity_tolerance_atr": breakout._env_float("LSR_FREE_LIQUIDITY_TOLERANCE_ATR", 0.40),
    "min_sweep_atr": breakout._env_float("LSR_FREE_MIN_SWEEP_ATR", 0.18),
    "min_rel_volume": breakout._env_float("LSR_FREE_MIN_REL_VOLUME", 0.84),
    "min_confirm_rel_volume": breakout._env_float("LSR_FREE_MIN_CONFIRM_REL_VOLUME", 0.93),
    "min_body_ratio_recovery": breakout._env_float("LSR_FREE_MIN_BODY_RATIO_RECOVERY", 0.22),
    "min_body_ratio_confirmation": breakout._env_float("LSR_FREE_MIN_BODY_RATIO_CONFIRMATION", 0.22),
    "min_close_position": breakout._env_float("LSR_FREE_MIN_CLOSE_POSITION", 0.57),
    "min_rr": breakout._env_float("LSR_FREE_MIN_RR", 0.96),
    "htf_price_tolerance": breakout._env_float("LSR_FREE_HTF_PRICE_TOLERANCE", 0.0080),
    "htf_trend_tolerance": breakout._env_float("LSR_FREE_HTF_TREND_TOLERANCE", 0.0140),
    "htf_required_score": max(1, int(os.getenv("LSR_FREE_HTF_REQUIRED_SCORE", "1"))),
    "score": 78.0,
}

PLUS_PROFILE = {
    "name": "plus",
    "atr_pct_min": breakout._env_float("LSR_PLUS_ATR_PCT_MIN", 0.0021),
    "atr_pct_max": breakout._env_float("LSR_PLUS_ATR_PCT_MAX", 0.0200),
    "liquidity_tolerance_atr": breakout._env_float("LSR_PLUS_LIQUIDITY_TOLERANCE_ATR", 0.35),
    "min_sweep_atr": breakout._env_float("LSR_PLUS_MIN_SWEEP_ATR", 0.24),
    "min_rel_volume": breakout._env_float("LSR_PLUS_MIN_REL_VOLUME", 0.95),
    "min_confirm_rel_volume": breakout._env_float("LSR_PLUS_MIN_CONFIRM_REL_VOLUME", 1.02),
    "min_body_ratio_recovery": breakout._env_float("LSR_PLUS_MIN_BODY_RATIO_RECOVERY", 0.26),
    "min_body_ratio_confirmation": breakout._env_float("LSR_PLUS_MIN_BODY_RATIO_CONFIRMATION", 0.26),
    "min_close_position": breakout._env_float("LSR_PLUS_MIN_CLOSE_POSITION", 0.63),
    "min_rr": breakout._env_float("LSR_PLUS_MIN_RR", 1.06),
    "htf_price_tolerance": breakout._env_float("LSR_PLUS_HTF_PRICE_TOLERANCE", 0.0055),
    "htf_trend_tolerance": breakout._env_float("LSR_PLUS_HTF_TREND_TOLERANCE", 0.0100),
    "htf_required_score": max(1, int(os.getenv("LSR_PLUS_HTF_REQUIRED_SCORE", "2"))),
    "score": 86.0,
}

PREMIUM_PROFILE = {
    "name": "premium",
    "atr_pct_min": breakout._env_float("LSR_PREMIUM_ATR_PCT_MIN", 0.0024),
    "atr_pct_max": breakout._env_float("LSR_PREMIUM_ATR_PCT_MAX", 0.0185),
    "liquidity_tolerance_atr": breakout._env_float("LSR_PREMIUM_LIQUIDITY_TOLERANCE_ATR", 0.30),
    "min_sweep_atr": breakout._env_float("LSR_PREMIUM_MIN_SWEEP_ATR", 0.30),
    "min_rel_volume": breakout._env_float("LSR_PREMIUM_MIN_REL_VOLUME", 1.03),
    "min_confirm_rel_volume": breakout._env_float("LSR_PREMIUM_MIN_CONFIRM_REL_VOLUME", 1.10),
    "min_body_ratio_recovery": breakout._env_float("LSR_PREMIUM_MIN_BODY_RATIO_RECOVERY", 0.30),
    "min_body_ratio_confirmation": breakout._env_float("LSR_PREMIUM_MIN_BODY_RATIO_CONFIRMATION", 0.30),
    "min_close_position": breakout._env_float("LSR_PREMIUM_MIN_CLOSE_POSITION", 0.69),
    "min_rr": breakout._env_float("LSR_PREMIUM_MIN_RR", 1.14),
    "htf_price_tolerance": breakout._env_float("LSR_PREMIUM_HTF_PRICE_TOLERANCE", 0.0040),
    "htf_trend_tolerance": breakout._env_float("LSR_PREMIUM_HTF_TREND_TOLERANCE", 0.0080),
    "htf_required_score": max(1, int(os.getenv("LSR_PREMIUM_HTF_REQUIRED_SCORE", "2"))),
    "score": 90.0,
}


PROFILES = [PREMIUM_PROFILE, PLUS_PROFILE, FREE_PROFILE]


LONG_DIRECTIONAL_TUNING = {
    "free": {
        "min_sweep_atr": 0.04,
        "min_confirm_rel_volume": 0.06,
        "min_body_ratio_confirmation": 0.04,
        "min_close_position": 0.04,
        "min_rr": 0.10,
        "htf_required_score": 0,
        "htf_price_tolerance_mul": 0.96,
        "htf_trend_tolerance_mul": 0.96,
    },
    "plus": {
        "min_sweep_atr": 0.05,
        "min_confirm_rel_volume": 0.07,
        "min_body_ratio_confirmation": 0.05,
        "min_close_position": 0.05,
        "min_rr": 0.12,
        "htf_required_score": 1,
        "htf_price_tolerance_mul": 0.92,
        "htf_trend_tolerance_mul": 0.92,
    },
    "premium": {
        "min_sweep_atr": 0.07,
        "min_confirm_rel_volume": 0.09,
        "min_body_ratio_confirmation": 0.05,
        "min_close_position": 0.05,
        "min_rr": 0.16,
        "htf_required_score": 1,
        "htf_price_tolerance_mul": 0.88,
        "htf_trend_tolerance_mul": 0.88,
    },
}

PREMIUM_GLOBAL_TUNING = {
    "min_rel_volume": 0.03,
    "min_confirm_rel_volume": 0.04,
    "min_body_ratio_confirmation": 0.02,
    "min_close_position": 0.02,
    "min_rr": 0.05,
}

LONG_SCORE_FLOOR_BONUS = {
    "free": 1.0,
    "plus": 2.0,
    "premium": 4.0,
}
PREMIUM_SCORE_FLOOR_BONUS = 2.0


LIQUIDITY_TRADE_PROFILES = {
    "conservador": {
        "leverage": breakout.TRADING_PROFILES["conservador"]["leverage"],
        "stop_buffer_atr_mult": breakout._env_float("LSR_TRADE_CONSERVADOR_STOP_BUFFER_ATR_MULT", 0.14),
        "min_stop_pct": breakout._env_float("LSR_TRADE_CONSERVADOR_MIN_STOP_PCT", 0.0038),
        "max_stop_pct": breakout._env_float("LSR_TRADE_CONSERVADOR_MAX_STOP_PCT", 0.0088),
        "tp1_rr": breakout._env_float("LSR_TRADE_CONSERVADOR_TP1_RR", 0.82),
        "tp2_rr": breakout._env_float("LSR_TRADE_CONSERVADOR_TP2_RR", 1.36),
    },
    "moderado": {
        "leverage": breakout.TRADING_PROFILES["moderado"]["leverage"],
        "stop_buffer_atr_mult": breakout._env_float("LSR_TRADE_MODERADO_STOP_BUFFER_ATR_MULT", 0.11),
        "min_stop_pct": breakout._env_float("LSR_TRADE_MODERADO_MIN_STOP_PCT", 0.0032),
        "max_stop_pct": breakout._env_float("LSR_TRADE_MODERADO_MAX_STOP_PCT", 0.0076),
        "tp1_rr": breakout._env_float("LSR_TRADE_MODERADO_TP1_RR", 0.95),
        "tp2_rr": breakout._env_float("LSR_TRADE_MODERADO_TP2_RR", 1.58),
    },
    "agresivo": {
        "leverage": breakout.TRADING_PROFILES["agresivo"]["leverage"],
        "stop_buffer_atr_mult": breakout._env_float("LSR_TRADE_AGRESIVO_STOP_BUFFER_ATR_MULT", 0.09),
        "min_stop_pct": breakout._env_float("LSR_TRADE_AGRESIVO_MIN_STOP_PCT", 0.0028),
        "max_stop_pct": breakout._env_float("LSR_TRADE_AGRESIVO_MAX_STOP_PCT", 0.0066),
        "tp1_rr": breakout._env_float("LSR_TRADE_AGRESIVO_TP1_RR", 1.08),
        "tp2_rr": breakout._env_float("LSR_TRADE_AGRESIVO_TP2_RR", 1.82),
    },
}

PULLBACK_RETRACE_FRACTION = breakout._env_float("LSR_PULLBACK_RETRACE_FRACTION", 0.42)
PULLBACK_ATR_MIN = breakout._env_float("LSR_PULLBACK_ATR_MIN", 0.16)
PULLBACK_ATR_MAX = breakout._env_float("LSR_PULLBACK_ATR_MAX", 0.48)
PULLBACK_LEVEL_BUFFER_ATR = breakout._env_float("LSR_PULLBACK_LEVEL_BUFFER_ATR", 0.04)
PULLBACK_MARKET_GAP_ATR = breakout._env_float("LSR_PULLBACK_MARKET_GAP_ATR", 0.03)
POST_FILL_INVALIDATION_MINUTES = max(5, int(os.getenv("LSR_POST_FILL_INVALIDATION_MINUTES", "35")))
POST_FILL_MIN_TP1_PROGRESS_PCT = breakout._env_float("LSR_POST_FILL_MIN_TP1_PROGRESS_PCT", 18.0)


def _record_reject(debug_counts: Optional[Dict[str, int]], reason: str) -> None:
    if debug_counts is None:
        return
    key = str(reason or "unknown").strip() or "unknown"
    debug_counts[key] = int(debug_counts.get(key, 0)) + 1




def _profile_name(profile: Dict) -> str:
    return str(profile.get("name") or "free").strip().lower() or "free"


def _directional_profile(profile: Dict, direction: str) -> Dict:
    tuned = dict(profile)
    profile_name = _profile_name(profile)

    if profile_name == "premium":
        tuned["min_rel_volume"] = float(tuned["min_rel_volume"]) + PREMIUM_GLOBAL_TUNING["min_rel_volume"]
        tuned["min_confirm_rel_volume"] = float(tuned["min_confirm_rel_volume"]) + PREMIUM_GLOBAL_TUNING["min_confirm_rel_volume"]
        tuned["min_body_ratio_confirmation"] = breakout._clamp(
            float(tuned["min_body_ratio_confirmation"]) + PREMIUM_GLOBAL_TUNING["min_body_ratio_confirmation"],
            0.0,
            0.95,
        )
        tuned["min_close_position"] = breakout._clamp(
            float(tuned["min_close_position"]) + PREMIUM_GLOBAL_TUNING["min_close_position"],
            0.0,
            0.95,
        )
        tuned["min_rr"] = float(tuned["min_rr"]) + PREMIUM_GLOBAL_TUNING["min_rr"]

    if str(direction).upper() == "LONG":
        direction_tuning = LONG_DIRECTIONAL_TUNING.get(profile_name, {})
        tuned["min_sweep_atr"] = float(tuned["min_sweep_atr"]) + float(direction_tuning.get("min_sweep_atr", 0.0))
        tuned["min_confirm_rel_volume"] = float(tuned["min_confirm_rel_volume"]) + float(direction_tuning.get("min_confirm_rel_volume", 0.0))
        tuned["min_body_ratio_confirmation"] = breakout._clamp(
            float(tuned["min_body_ratio_confirmation"]) + float(direction_tuning.get("min_body_ratio_confirmation", 0.0)),
            0.0,
            0.95,
        )
        tuned["min_close_position"] = breakout._clamp(
            float(tuned["min_close_position"]) + float(direction_tuning.get("min_close_position", 0.0)),
            0.0,
            0.97,
        )
        tuned["min_rr"] = float(tuned["min_rr"]) + float(direction_tuning.get("min_rr", 0.0))
        tuned["htf_required_score"] = max(
            int(tuned.get("htf_required_score", 1)),
            int(profile.get("htf_required_score", 1)) + int(direction_tuning.get("htf_required_score", 0)),
        )
        tuned["htf_price_tolerance"] = max(0.0, float(tuned["htf_price_tolerance"]) * float(direction_tuning.get("htf_price_tolerance_mul", 1.0)))
        tuned["htf_trend_tolerance"] = max(0.0, float(tuned["htf_trend_tolerance"]) * float(direction_tuning.get("htf_trend_tolerance_mul", 1.0)))

    return tuned


def _directional_min_raw_score(profile_name: str, direction: str) -> float:
    minimum = _min_raw_score_for_profile(profile_name)
    normalized_profile = str(profile_name or "").strip().lower()
    if normalized_profile == "premium":
        minimum += PREMIUM_SCORE_FLOOR_BONUS
    if str(direction).upper() == "LONG":
        minimum += float(LONG_SCORE_FLOOR_BONUS.get(normalized_profile, 0.0))
    return float(minimum)


def _directional_context_ok(df: pd.DataFrame, direction: str, profile: Dict) -> bool:
    if df is None or df.empty or len(df) < 2:
        return True

    profile_name = _profile_name(profile)
    last = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(last["close"])
    prev_close = float(prev["close"])
    high = float(last["high"])
    prev_high = float(prev["high"])
    low = float(last["low"])
    prev_low = float(prev["low"])
    ema20 = float(last["ema20"])
    prev_ema20 = float(prev["ema20"])
    rel_volume = float(last.get("rel_volume") or 0.0)

    if str(direction).upper() == "SHORT":
        if profile_name != "premium":
            return True
        checks = [
            close <= (ema20 * 1.002),
            close <= (prev_close * 1.002) or low <= prev_low,
            rel_volume >= float(profile.get("min_confirm_rel_volume", 0.0)),
        ]
        return sum(bool(flag) for flag in checks) >= 2

    checks = [
        close >= (ema20 * (0.999 if profile_name == "free" else 1.0)),
        ema20 >= (prev_ema20 * (0.999 if profile_name == "free" else 0.9995 if profile_name == "plus" else 1.0)),
        close >= (prev_close * (0.999 if profile_name == "free" else 1.0)) or high >= prev_high,
        rel_volume >= float(profile.get("min_confirm_rel_volume", 0.0)),
    ]
    required = 2 if profile_name == "free" else 3
    return sum(bool(flag) for flag in checks) >= required

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



def _higher_timeframe_context_ok(df_1h: pd.DataFrame, direction: str, profile: Dict) -> bool:
    if df_1h is None or df_1h.empty or len(df_1h) < 40:
        return True
    enriched = add_indicators(_closed_15m_frame(df_1h))
    if enriched is None or enriched.empty:
        return True
    last = enriched.iloc[-1]
    if not _indicators_ready(last):
        return True
    prev = enriched.iloc[-2] if len(enriched) >= 2 else last
    recent = enriched.tail(3)

    close = float(last["close"])
    ema20 = float(last["ema20"])
    ema50 = float(last["ema50"])
    prev_close = float(prev["close"])
    prev_ema20 = float(prev["ema20"])
    tolerance_price = max(0.0, float(profile.get("htf_price_tolerance", 0.0)))
    tolerance_trend = max(0.0, float(profile.get("htf_trend_tolerance", 0.0)))
    required_score = max(1, int(profile.get("htf_required_score", 1)))

    if direction == "LONG":
        price_support = close >= (ema20 * (1.0 - tolerance_price))
        trend_support = ema20 >= (ema50 * (1.0 - tolerance_trend))
        momentum_support = close >= prev_close or ema20 >= (prev_ema20 * 0.999)
        reclaim_support = bool((recent["close"].astype(float) >= (recent["ema20"].astype(float) * (1.0 - tolerance_price))).any())
        score = sum(bool(flag) for flag in (price_support, trend_support, momentum_support, reclaim_support))
        if str(profile.get("name")) == "premium" and not price_support:
            return False
        return score >= required_score

    price_support = close <= (ema20 * (1.0 + tolerance_price))
    trend_support = ema20 <= (ema50 * (1.0 + tolerance_trend))
    momentum_support = close <= prev_close or ema20 <= (prev_ema20 * 1.001)
    reclaim_support = bool((recent["close"].astype(float) <= (recent["ema20"].astype(float) * (1.0 + tolerance_price))).any())
    score = sum(bool(flag) for flag in (price_support, trend_support, momentum_support, reclaim_support))
    if str(profile.get("name")) == "premium" and not price_support:
        return False
    return score >= required_score



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
            "extreme_price": breakout._round_price_dynamic(low_price),
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
        "extreme_price": breakout._round_price_dynamic(high_price),
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


def _build_pullback_entry(
    *,
    direction: str,
    level: float,
    confirmation_close: float,
    market_price: float,
    atr_now: float,
) -> Optional[float]:
    reference_price = float(market_price or confirmation_close)
    confirmation_close = float(confirmation_close)
    level = float(level)
    atr_now = max(float(atr_now or 0.0), max(abs(reference_price) * 0.0003, 1e-9))

    reclaim_distance = abs(confirmation_close - level)
    desired_pullback = breakout._clamp(
        max(reclaim_distance * PULLBACK_RETRACE_FRACTION, atr_now * PULLBACK_ATR_MIN),
        atr_now * PULLBACK_ATR_MIN,
        atr_now * PULLBACK_ATR_MAX,
    )
    level_buffer = atr_now * PULLBACK_LEVEL_BUFFER_ATR
    market_gap = atr_now * PULLBACK_MARKET_GAP_ATR

    if str(direction).upper() == "LONG":
        floor = level + level_buffer
        upper = reference_price - market_gap
        if upper <= floor:
            return None
        candidate = max(floor, reference_price - desired_pullback)
        return breakout._round_price_dynamic(min(candidate, upper))

    ceiling = level - level_buffer
    lower = reference_price + market_gap
    if lower >= ceiling:
        return None
    candidate = min(ceiling, reference_price + desired_pullback)
    return breakout._round_price_dynamic(max(candidate, lower))


def _rr_price(direction: str, entry_price: float, risk_distance: float, rr: float) -> float:
    if str(direction).upper() == "LONG":
        return breakout._round_price_dynamic(entry_price + (risk_distance * rr))
    return breakout._round_price_dynamic(entry_price - (risk_distance * rr))


def _cap_target_to_barrier(direction: str, entry_price: float, raw_target: float, barrier_price: Optional[float], barrier_fraction: float) -> float:
    if barrier_price is None:
        return breakout._round_price_dynamic(raw_target)

    entry_price = float(entry_price)
    barrier_price = float(barrier_price)
    barrier_fraction = breakout._clamp(float(barrier_fraction), 0.55, 0.98)

    if str(direction).upper() == "LONG":
        barrier_gap = barrier_price - entry_price
        if barrier_gap <= 0:
            return breakout._round_price_dynamic(raw_target)
        return breakout._round_price_dynamic(min(raw_target, entry_price + (barrier_gap * barrier_fraction)))

    barrier_gap = entry_price - barrier_price
    if barrier_gap <= 0:
        return breakout._round_price_dynamic(raw_target)
    return breakout._round_price_dynamic(max(raw_target, entry_price - (barrier_gap * barrier_fraction)))


def _build_liquidity_trade_profiles(
    *,
    entry_price: float,
    direction: str,
    atr_now: float,
    sweep_extreme_price: float,
    barrier_price: Optional[float],
) -> Dict[str, Dict]:
    profiles: Dict[str, Dict] = {}
    entry_price = float(entry_price)
    atr_now = max(float(atr_now or 0.0), max(abs(entry_price) * 0.0003, 1e-9))
    sweep_extreme_price = float(sweep_extreme_price)

    for name, cfg in LIQUIDITY_TRADE_PROFILES.items():
        buffer_distance = atr_now * max(float(cfg.get("stop_buffer_atr_mult", 0.0)), 0.0)
        if str(direction).upper() == "LONG":
            raw_stop = sweep_extreme_price - buffer_distance
            min_stop = entry_price * (1.0 - float(cfg["min_stop_pct"]))
            stop_loss = min(raw_stop, min_stop)
            risk_distance = entry_price - stop_loss
        else:
            raw_stop = sweep_extreme_price + buffer_distance
            min_stop = entry_price * (1.0 + float(cfg["min_stop_pct"]))
            stop_loss = max(raw_stop, min_stop)
            risk_distance = stop_loss - entry_price

        risk_pct = risk_distance / max(entry_price, 1e-9)
        if risk_distance <= 0 or risk_pct > float(cfg["max_stop_pct"]):
            continue

        raw_tp1 = _rr_price(direction, entry_price, risk_distance, float(cfg["tp1_rr"]))
        raw_tp2 = _rr_price(direction, entry_price, risk_distance, float(cfg["tp2_rr"]))
        tp1 = _cap_target_to_barrier(direction, entry_price, raw_tp1, barrier_price, 0.78)
        tp2 = _cap_target_to_barrier(direction, entry_price, raw_tp2, barrier_price, 0.96)

        if str(direction).upper() == "LONG":
            if not (tp1 > entry_price and tp2 > tp1):
                continue
        else:
            if not (tp1 < entry_price and tp2 < tp1):
                continue

        profiles[name] = {
            "stop_loss": breakout._round_price_dynamic(stop_loss),
            "take_profits": [tp1, tp2],
            "leverage": cfg["leverage"],
        }

    return profiles



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
    df_5m: Optional[pd.DataFrame],
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

    tuned_profile = _directional_profile(profile, direction)

    atr_pct = float(last["atr_pct"])
    if not (float(tuned_profile["atr_pct_min"]) <= atr_pct <= float(tuned_profile["atr_pct_max"])):
        _record_reject(debug_counts, "liquidity_atr_pct")
        return None

    if not _higher_timeframe_context_ok(df_1h, direction, tuned_profile):
        _record_reject(debug_counts, "liquidity_htf_context")
        return None

    zone = _select_liquidity_zone(df, direction, tuned_profile)
    if not zone:
        _record_reject(debug_counts, "liquidity_zone_missing")
        return None

    level = float(zone["price"])
    sweep = _find_recent_sweep(df, direction, level, tuned_profile)
    if not sweep:
        _record_reject(debug_counts, "liquidity_minimum_sweep")
        return None

    if not _recovery_candle_ok(last, direction, level, tuned_profile):
        _record_reject(debug_counts, "liquidity_recovery_close")
        return None

    if not _confirmation_candle_ok(last, direction, level, tuned_profile):
        _record_reject(debug_counts, "liquidity_confirmation")
        return None

    if not _directional_context_ok(df, direction, tuned_profile):
        _record_reject(debug_counts, "liquidity_directional_context")
        return None

    if not _ema_reclaim_ok(last, direction):
        _record_reject(debug_counts, "liquidity_ema_reclaim")
        return None

    current_reference_price = float(current_market_price or last["close"])
    if current_reference_price <= 0:
        current_reference_price = float(last["close"])

    atr_now = float(last["atr"])
    entry_price = _build_pullback_entry(
        direction=direction,
        level=level,
        confirmation_close=float(last["close"]),
        market_price=current_reference_price,
        atr_now=atr_now,
    )
    if entry_price is None:
        _record_reject(debug_counts, "liquidity_pullback_geometry")
        return None

    barrier_price = _nearest_barrier_price(df, direction, entry_price)
    trade_profiles = _build_liquidity_trade_profiles(
        entry_price=entry_price,
        direction=direction,
        atr_now=atr_now,
        sweep_extreme_price=float(sweep.get("extreme_price") or level),
        barrier_price=barrier_price,
    )
    conservative = trade_profiles.get("conservador") or {}
    stop_loss = float(conservative.get("stop_loss") or 0.0)
    take_profits = list(conservative.get("take_profits") or [])
    if stop_loss <= 0 or not take_profits:
        _record_reject(debug_counts, "liquidity_trade_profile")
        return None

    barrier_rr = _safe_rr(entry_price, stop_loss, barrier_price) if barrier_price is not None else 0.0
    if barrier_rr < float(tuned_profile["min_rr"]):
        _record_reject(debug_counts, "liquidity_barrier_room")
        return None

    components = _score_components(last, direction, zone, sweep, barrier_rr, tuned_profile)
    raw_score = _sum_components(components)
    if raw_score < _directional_min_raw_score(str(tuned_profile["name"]), direction):
        _record_reject(debug_counts, "liquidity_score_floor")
        return None

    entry_model_price = float(last["close"])
    payload = {
        "direction": direction,
        "entry_price": breakout._round_price_dynamic(entry_price),
        "stop_loss": stop_loss,
        "take_profits": take_profits,
        "profiles": trade_profiles,
        "score": raw_score,
        "raw_score": raw_score,
        "normalized_score": raw_score,
        "components": components,
        "raw_components": components,
        "normalized_components": components,
        "timeframes": ["15M", "5M"],
        "setup_group": str(tuned_profile["name"]),
        "atr_pct": round(atr_pct, 6),
        "score_profile": str(tuned_profile["name"]),
        "score_calibration": SCORE_CALIBRATION_VERSION,
        "send_mode": SEND_MODE_PENDING_ENTRY,
        "entry_model_price": breakout._round_price_dynamic(entry_model_price),
        "entry_sent_price": breakout._round_price_dynamic(current_reference_price),
        "setup_stage": SETUP_STAGE_WAITING_PULLBACK,
        "entry_model": ENTRY_MODEL_NAME,
        "higher_tf_context": {"liquidity_zone": level, "sweep_distance_atr": sweep.get("sweep_distance_atr")},
        "liquidity_zone": breakout._round_price_dynamic(level),
        "liquidity_sweep_distance_atr": sweep.get("sweep_distance_atr"),
        "liquidity_sweep_extreme_price": sweep.get("extreme_price"),
        "strategy_runtime": {
            "post_fill_invalidation": {
                "minutes": POST_FILL_INVALIDATION_MINUTES,
                "min_tp1_progress_pct": POST_FILL_MIN_TP1_PROGRESS_PCT,
                "reason": "liquidity_no_followthrough",
            }
        },
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
    if enriched_15m is None or enriched_15m.empty or not _indicators_ready(enriched_15m.iloc[-1]):
        _record_reject(debug_counts, "liquidity_indicator_warmup")
        return None

    closed_1h = _closed_15m_frame(df_1h) if df_1h is not None and not df_1h.empty else df_1h
    enriched_1h = add_indicators(closed_1h) if closed_1h is not None and not closed_1h.empty else closed_1h

    for profile in PROFILES:
        candidates: List[Tuple[Dict, Tuple[float, float, float]]] = []
        for direction in ("LONG", "SHORT"):
            result = _evaluate_direction(
                enriched_15m,
                enriched_1h,
                df_5m,
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
