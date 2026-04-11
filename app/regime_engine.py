from __future__ import annotations

import os
import threading
import time
from typing import Any, Callable, Dict, Optional

import pandas as pd

BTC_REGIME_SYMBOL = str(os.getenv("BTC_REGIME_SYMBOL", "BTCUSDT")).strip().upper() or "BTCUSDT"
BTC_REGIME_5M_LOOKBACK = max(40, int(os.getenv("BTC_REGIME_5M_LOOKBACK", "120")))
BTC_REGIME_15M_LOOKBACK = max(24, int(os.getenv("BTC_REGIME_15M_LOOKBACK", "96")))
BTC_REGIME_DIRECTIONAL_MOVE_PCT = max(0.15, float(os.getenv("BTC_REGIME_DIRECTIONAL_MOVE_PCT", "0.55")))
BTC_REGIME_FAST_MOVE_PCT = max(0.08, float(os.getenv("BTC_REGIME_FAST_MOVE_PCT", "0.28")))
BTC_REGIME_SHOCK_MOVE_PCT = max(0.20, float(os.getenv("BTC_REGIME_SHOCK_MOVE_PCT", "0.90")))
BTC_REGIME_SHOCK_RANGE_ATR = max(1.0, float(os.getenv("BTC_REGIME_SHOCK_RANGE_ATR", "2.20")))
BTC_REGIME_SHOCK_BODY_ATR = max(0.8, float(os.getenv("BTC_REGIME_SHOCK_BODY_ATR", "1.20")))
BTC_REGIME_COOLDOWN_BARS = max(1, int(os.getenv("BTC_REGIME_COOLDOWN_BARS", "3")))
MARKET_REGIME_CONFIRM_BARS = max(1, int(os.getenv("MARKET_REGIME_CONFIRM_BARS", "2")))
MARKET_REGIME_MIN_HOLD_SECONDS = max(60, int(os.getenv("MARKET_REGIME_MIN_HOLD_SECONDS", "900")))
MARKET_REGIME_SNAPSHOT_TTL_SECONDS = max(15.0, float(os.getenv("MARKET_REGIME_SNAPSHOT_TTL_SECONDS", "180")))
MARKET_REGIME_FAIL_OPEN = str(os.getenv("MARKET_REGIME_FAIL_OPEN", "true")).strip().lower() in {"1", "true", "yes", "on"}
MARKET_REGIME_ROUTER_VERSION = "v1_btc_regime_router_hysteresis"

_state_lock = threading.Lock()
_state: Dict[str, Any] = {
    "fetched_at_ts": 0.0,
    "state": "unknown",
    "raw_state": "unknown",
    "bias": "neutral",
    "allow": True,
    "reason": "uninitialized",
    "entered_at_ts": 0.0,
    "candidate_state": None,
    "candidate_count": 0,
    "cooldown_until_ts": 0.0,
}


def _now_ts() -> float:
    return time.time()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _closed_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "close_time" not in df.columns:
        return (df or pd.DataFrame()).copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    closed = df[df["close_time"] <= now_utc].copy()
    if not closed.empty:
        return closed
    if len(df) > 1:
        return df.iloc[:-1].copy()
    return df.copy()


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


def _body_ratio_series(df: pd.DataFrame) -> pd.Series:
    rng = (df["high"].astype(float) - df["low"].astype(float)).replace(0, 1e-9)
    body = (df["close"].astype(float) - df["open"].astype(float)).abs()
    return body / rng


def _wickiness_series(df: pd.DataFrame) -> pd.Series:
    return 1.0 - _body_ratio_series(df)


def _trend_consistency(closes: pd.Series) -> float:
    if closes is None or len(closes) < 4:
        return 0.0
    returns = closes.astype(float).diff().dropna()
    if returns.empty:
        return 0.0
    signs = returns.apply(lambda v: 1 if v > 0 else (-1 if v < 0 else 0))
    non_zero = signs[signs != 0]
    if non_zero.empty:
        return 0.0
    dominant = non_zero.value_counts().max()
    return float(dominant) / float(len(non_zero))


def _sign_flip_ratio(closes: pd.Series) -> float:
    if closes is None or len(closes) < 5:
        return 0.0
    returns = closes.astype(float).diff().dropna()
    signs = returns.apply(lambda v: 1 if v > 0 else (-1 if v < 0 else 0)).tolist()
    signs = [s for s in signs if s != 0]
    if len(signs) < 2:
        return 0.0
    flips = sum(1 for idx in range(1, len(signs)) if signs[idx] != signs[idx - 1])
    return float(flips) / float(max(1, len(signs) - 1))


def _classify_raw_market_regime(df_5m: pd.DataFrame, df_15m: pd.DataFrame) -> Dict[str, Any]:
    closed_5m = _closed_frame(df_5m)
    closed_15m = _closed_frame(df_15m)

    if len(closed_5m) < 20 or len(closed_15m) < 8:
        return {
            "raw_state": "unknown",
            "bias": "neutral",
            "allow": MARKET_REGIME_FAIL_OPEN,
            "reason": "market_regime_insufficient_btc_history",
            "metrics": {},
        }

    atr_5m = _simple_atr_series(closed_5m, period=14)
    if atr_5m.empty or pd.isna(atr_5m.iloc[-1]) or float(atr_5m.iloc[-1]) <= 1e-9:
        return {
            "raw_state": "unknown",
            "bias": "neutral",
            "allow": MARKET_REGIME_FAIL_OPEN,
            "reason": "market_regime_btc_atr_unavailable",
            "metrics": {},
        }

    last_5m = closed_5m.iloc[-1]
    prev_5m = closed_5m.iloc[-2]
    atr_now = float(atr_5m.iloc[-1])
    last_range_atr = abs(float(last_5m["high"]) - float(last_5m["low"])) / atr_now
    last_body_atr = abs(float(last_5m["close"]) - float(last_5m["open"])) / atr_now
    last_move_pct = abs((float(last_5m["close"]) / max(float(prev_5m["close"]), 1e-9) - 1.0) * 100.0)

    recent_window = closed_5m.tail(max(BTC_REGIME_COOLDOWN_BARS, 4)).copy().reset_index(drop=True)
    recent_atr = atr_5m.tail(len(recent_window)).reset_index(drop=True)
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

    body_ratio_5m = _body_ratio_series(closed_5m.tail(6)).fillna(0.0)
    body_ratio_15m = _body_ratio_series(closed_15m.tail(4)).fillna(0.0)
    wickiness_5m = _wickiness_series(closed_5m.tail(6)).fillna(0.0)
    wickiness_15m = _wickiness_series(closed_15m.tail(4)).fillna(0.0)
    consistency_5m = _trend_consistency(closed_5m.tail(7)["close"])
    consistency_15m = _trend_consistency(closed_15m.tail(5)["close"])
    sign_flip_ratio = _sign_flip_ratio(closed_5m.tail(8)["close"])

    continuation_score = 0
    if abs(move_15m_pct) >= BTC_REGIME_DIRECTIONAL_MOVE_PCT:
        continuation_score += 1
    if abs(move_fast_pct) >= BTC_REGIME_FAST_MOVE_PCT:
        continuation_score += 1
    if float(body_ratio_5m.mean()) >= 0.52 and float(body_ratio_15m.mean()) >= 0.48:
        continuation_score += 1
    if float(wickiness_5m.mean()) <= 0.42 and float(wickiness_15m.mean()) <= 0.46:
        continuation_score += 1
    if consistency_5m >= 0.60 and consistency_15m >= 0.60:
        continuation_score += 1

    sweep_score = 0
    if float(wickiness_5m.mean()) >= 0.46 or float(wickiness_15m.mean()) >= 0.50:
        sweep_score += 1
    if sign_flip_ratio >= 0.50:
        sweep_score += 1
    if consistency_5m <= 0.55:
        sweep_score += 1
    if abs(move_15m_pct) < BTC_REGIME_DIRECTIONAL_MOVE_PCT or abs(move_fast_pct) < BTC_REGIME_FAST_MOVE_PCT:
        sweep_score += 1

    shock_now = bool(
        last_range_atr >= BTC_REGIME_SHOCK_RANGE_ATR
        or last_body_atr >= BTC_REGIME_SHOCK_BODY_ATR
        or last_move_pct >= BTC_REGIME_SHOCK_MOVE_PCT
    )

    raw_state = "sweep_reversal"
    reason = "market_regime_sweep_reversal"
    allow = True

    if shock_now:
        raw_state = "risk_off"
        reason = "market_regime_vol_shock"
        allow = False
    elif recent_shock:
        raw_state = "risk_off"
        reason = "market_regime_cooldown"
        allow = False
    elif continuation_score >= 4 and continuation_score >= (sweep_score + 1) and directional_bias in {"up", "down"}:
        raw_state = "continuation_clean"
        reason = "market_regime_continuation_clean"
    elif sweep_score >= 2:
        raw_state = "sweep_reversal"
        reason = "market_regime_sweep_reversal"
    elif directional_bias in {"up", "down"} and continuation_score >= 3:
        raw_state = "continuation_clean"
        reason = "market_regime_continuation_soft"

    return {
        "raw_state": raw_state,
        "bias": directional_bias,
        "allow": allow,
        "reason": reason,
        "metrics": {
            "move_15m_pct": round(move_15m_pct, 4),
            "move_fast_pct": round(move_fast_pct, 4),
            "last_move_pct": round(last_move_pct, 4),
            "last_range_atr": round(last_range_atr, 4),
            "last_body_atr": round(last_body_atr, 4),
            "consistency_5m": round(consistency_5m, 4),
            "consistency_15m": round(consistency_15m, 4),
            "sign_flip_ratio": round(sign_flip_ratio, 4),
            "avg_body_ratio_5m": round(float(body_ratio_5m.mean()), 4),
            "avg_body_ratio_15m": round(float(body_ratio_15m.mean()), 4),
            "avg_wickiness_5m": round(float(wickiness_5m.mean()), 4),
            "avg_wickiness_15m": round(float(wickiness_15m.mean()), 4),
            "continuation_score": continuation_score,
            "sweep_score": sweep_score,
        },
    }


def classify_market_regime(df_5m: pd.DataFrame, df_15m: pd.DataFrame, *, now_ts: Optional[float] = None) -> Dict[str, Any]:
    current_ts = _now_ts() if now_ts is None else float(now_ts)
    raw = _classify_raw_market_regime(df_5m, df_15m)

    with _state_lock:
        stable_state = str(_state.get("state") or "unknown")
        entered_at_ts = float(_state.get("entered_at_ts") or 0.0)
        candidate_state = _state.get("candidate_state")
        candidate_count = int(_state.get("candidate_count") or 0)
        cooldown_until_ts = float(_state.get("cooldown_until_ts") or 0.0)

        if raw["raw_state"] == "risk_off":
            stable_state = "risk_off"
            entered_at_ts = current_ts
            cooldown_until_ts = max(cooldown_until_ts, current_ts + (BTC_REGIME_COOLDOWN_BARS * 5 * 60))
            candidate_state = None
            candidate_count = 0
            reason = raw["reason"]
        elif cooldown_until_ts > current_ts:
            stable_state = "risk_off"
            reason = "market_regime_cooldown_hold"
            raw["allow"] = False
            raw["raw_state"] = "risk_off"
        else:
            raw_state = str(raw.get("raw_state") or "unknown")
            if stable_state in {"unknown", "uninitialized"}:
                stable_state = raw_state
                entered_at_ts = current_ts
                candidate_state = None
                candidate_count = 0
                reason = str(raw.get("reason") or "market_regime_initialized")
            elif raw_state == stable_state:
                candidate_state = None
                candidate_count = 0
                reason = str(raw.get("reason") or "market_regime_stable")
            elif (current_ts - entered_at_ts) < MARKET_REGIME_MIN_HOLD_SECONDS:
                reason = "market_regime_hold_active"
            else:
                if candidate_state == raw_state:
                    candidate_count += 1
                else:
                    candidate_state = raw_state
                    candidate_count = 1
                if raw_state == "risk_off" or candidate_count >= MARKET_REGIME_CONFIRM_BARS:
                    stable_state = raw_state
                    entered_at_ts = current_ts
                    candidate_state = None
                    candidate_count = 0
                    reason = str(raw.get("reason") or "market_regime_switch_confirmed")
                else:
                    reason = "market_regime_switch_pending"

        allow = stable_state != "risk_off"
        strategy_name = {
            "continuation_clean": "breakout_reset",
            "sweep_reversal": "liquidity_sweep_reversal",
            "risk_off": "risk_off",
            "unknown": "breakout_reset" if MARKET_REGIME_FAIL_OPEN else "risk_off",
        }.get(stable_state, "breakout_reset")

        snapshot = {
            "fetched_at_ts": current_ts,
            "state": stable_state,
            "raw_state": raw.get("raw_state"),
            "bias": raw.get("bias", "neutral"),
            "allow": allow,
            "reason": reason,
            "raw_reason": raw.get("reason"),
            "metrics": dict(raw.get("metrics") or {}),
            "entered_at_ts": entered_at_ts,
            "stable_for_seconds": round(max(0.0, current_ts - entered_at_ts), 2) if entered_at_ts else 0.0,
            "candidate_state": candidate_state,
            "candidate_count": candidate_count,
            "cooldown_until_ts": cooldown_until_ts,
            "strategy_name": strategy_name,
            "router_version": MARKET_REGIME_ROUTER_VERSION,
            "symbol": BTC_REGIME_SYMBOL,
        }

        _state.clear()
        _state.update(snapshot)
        return dict(snapshot)


def snapshot_market_regime() -> Dict[str, Any]:
    with _state_lock:
        return dict(_state)


def fetch_market_regime_snapshot(
    fetch_klines: Callable[..., pd.DataFrame],
    *,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    cached = snapshot_market_regime()
    now_ts = _now_ts()
    age = now_ts - float(cached.get("fetched_at_ts") or 0.0)
    if cached.get("state") not in {None, "unknown", "uninitialized"} and age <= MARKET_REGIME_SNAPSHOT_TTL_SECONDS and not force_refresh:
        return cached

    try:
        df_5m = fetch_klines(BTC_REGIME_SYMBOL, "5m", limit=BTC_REGIME_5M_LOOKBACK)
        df_15m = fetch_klines(BTC_REGIME_SYMBOL, "15m", limit=BTC_REGIME_15M_LOOKBACK)
        return classify_market_regime(df_5m, df_15m, now_ts=now_ts)
    except Exception as exc:
        fallback = dict(cached) if cached else {}
        if fallback:
            fallback["reason"] = str(fallback.get("reason") or "market_regime_cached_stale")
            fallback["fetch_error"] = str(exc)
            fallback["fetched_at_ts"] = now_ts
            with _state_lock:
                _state.clear()
                _state.update(fallback)
            return fallback
        return {
            "fetched_at_ts": now_ts,
            "state": "unknown",
            "raw_state": "unknown",
            "bias": "neutral",
            "allow": MARKET_REGIME_FAIL_OPEN,
            "reason": "market_regime_fetch_failed_fail_open" if MARKET_REGIME_FAIL_OPEN else "market_regime_fetch_failed_fail_closed",
            "raw_reason": "market_regime_fetch_failed",
            "metrics": {},
            "entered_at_ts": 0.0,
            "stable_for_seconds": 0.0,
            "candidate_state": None,
            "candidate_count": 0,
            "cooldown_until_ts": 0.0,
            "strategy_name": "breakout_reset" if MARKET_REGIME_FAIL_OPEN else "risk_off",
            "router_version": MARKET_REGIME_ROUTER_VERSION,
            "symbol": BTC_REGIME_SYMBOL,
            "fetch_error": str(exc),
        }
