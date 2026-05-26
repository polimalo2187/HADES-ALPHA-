from __future__ import annotations

import os
from typing import Any, Dict, Optional

import pandas as pd

SYMBOL_REGIME_VERSION = "v1_symbol_context"
SYMBOL_REGIME_MIN_5M_BARS = max(35, int(os.getenv("SYMBOL_REGIME_MIN_5M_BARS", "55")))
SYMBOL_REGIME_MIN_15M_BARS = max(12, int(os.getenv("SYMBOL_REGIME_MIN_15M_BARS", "18")))
SYMBOL_REGIME_EMA_FAST = max(5, int(os.getenv("SYMBOL_REGIME_EMA_FAST", "20")))
SYMBOL_REGIME_EMA_MID = max(SYMBOL_REGIME_EMA_FAST + 1, int(os.getenv("SYMBOL_REGIME_EMA_MID", "50")))
SYMBOL_REGIME_ATR_PERIOD = max(7, int(os.getenv("SYMBOL_REGIME_ATR_PERIOD", "14")))
SYMBOL_REGIME_COMPRESSION_ATR_PCT = max(0.02, float(os.getenv("SYMBOL_REGIME_COMPRESSION_ATR_PCT", "0.22")))
SYMBOL_REGIME_EXPANSION_ATR_PCT = max(SYMBOL_REGIME_COMPRESSION_ATR_PCT, float(os.getenv("SYMBOL_REGIME_EXPANSION_ATR_PCT", "0.42")))
SYMBOL_REGIME_EXHAUSTION_DISTANCE_ATR = max(1.0, float(os.getenv("SYMBOL_REGIME_EXHAUSTION_DISTANCE_ATR", "3.20")))
SYMBOL_REGIME_WICKY_AVG = min(0.95, max(0.20, float(os.getenv("SYMBOL_REGIME_WICKY_AVG", "0.53"))))
SYMBOL_REGIME_FLIP_RATIO = min(0.95, max(0.20, float(os.getenv("SYMBOL_REGIME_FLIP_RATIO", "0.55"))))
SYMBOL_REGIME_TREND_MIN_SCORE = max(2, int(os.getenv("SYMBOL_REGIME_TREND_MIN_SCORE", "4")))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        if pd.isna(result):
            return float(default)
        return result
    except Exception:
        return float(default)


def _closed_frame(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if "close_time" not in df.columns:
        return df.copy()
    try:
        close_time = pd.to_datetime(df["close_time"], utc=True, errors="coerce")
        now_utc = pd.Timestamp.now(tz="UTC")
        closed = df[close_time <= now_utc].copy()
        if not closed.empty:
            return closed
    except Exception:
        pass
    if len(df) > 1:
        return df.iloc[:-1].copy()
    return df.copy()


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.astype(float).ewm(span=int(span), adjust=False, min_periods=min(int(span), max(1, len(series)))).mean()


def _atr(df: pd.DataFrame, period: int = SYMBOL_REGIME_ATR_PERIOD) -> pd.Series:
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
    return tr.rolling(window=int(period), min_periods=int(period)).mean()


def _body_ratio(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)
    rng = (df["high"].astype(float) - df["low"].astype(float)).abs().replace(0, 1e-9)
    return (df["close"].astype(float) - df["open"].astype(float)).abs() / rng


def _wickiness(df: pd.DataFrame) -> pd.Series:
    return 1.0 - _body_ratio(df)


def _trend_consistency(closes: pd.Series) -> float:
    if closes is None or len(closes) < 5:
        return 0.0
    returns = closes.astype(float).diff().dropna()
    signs = returns.apply(lambda v: 1 if v > 0 else (-1 if v < 0 else 0))
    non_zero = signs[signs != 0]
    if non_zero.empty:
        return 0.0
    return float(non_zero.value_counts().max()) / float(len(non_zero))


def _sign_flip_ratio(closes: pd.Series) -> float:
    if closes is None or len(closes) < 6:
        return 0.0
    signs = closes.astype(float).diff().dropna().apply(lambda v: 1 if v > 0 else (-1 if v < 0 else 0)).tolist()
    signs = [s for s in signs if s != 0]
    if len(signs) < 2:
        return 0.0
    flips = sum(1 for idx in range(1, len(signs)) if signs[idx] != signs[idx - 1])
    return float(flips) / float(max(1, len(signs) - 1))


def _bias_from_stack(close: float, ema_fast: float, ema_mid: float, fallback_move: float) -> str:
    if close > ema_fast > ema_mid:
        return "up"
    if close < ema_fast < ema_mid:
        return "down"
    if fallback_move > 0:
        return "up"
    if fallback_move < 0:
        return "down"
    return "neutral"


def classify_symbol_regime(
    df_5m: Optional[pd.DataFrame],
    df_15m: Optional[pd.DataFrame] = None,
    *,
    symbol: str = "",
) -> Dict[str, Any]:
    """Classify the local regime of one symbol.

    This is intentionally lightweight and fail-open. The global BTC regime is a
    context signal; this symbol regime is the local veto/fallback input that lets
    Breakout + Reset compete when the symbol itself is trending cleanly.
    """

    closed_5m = _closed_frame(df_5m)
    closed_15m = _closed_frame(df_15m)
    if len(closed_5m) < SYMBOL_REGIME_MIN_5M_BARS:
        return {
            "state": "symbol_unknown",
            "bias": "neutral",
            "reason": "symbol_regime_insufficient_5m_history",
            "allow_breakout": True,
            "score": 0,
            "version": SYMBOL_REGIME_VERSION,
            "symbol": str(symbol or ""),
            "metrics": {"bars_5m": int(len(closed_5m)), "bars_15m": int(len(closed_15m))},
        }

    closes = closed_5m["close"].astype(float)
    ema_fast = _ema(closes, SYMBOL_REGIME_EMA_FAST)
    ema_mid = _ema(closes, SYMBOL_REGIME_EMA_MID)
    atr = _atr(closed_5m)
    if atr.empty or pd.isna(atr.iloc[-1]) or float(atr.iloc[-1]) <= 1e-9:
        return {
            "state": "symbol_unknown",
            "bias": "neutral",
            "reason": "symbol_regime_atr_unavailable",
            "allow_breakout": True,
            "score": 0,
            "version": SYMBOL_REGIME_VERSION,
            "symbol": str(symbol or ""),
            "metrics": {"bars_5m": int(len(closed_5m)), "bars_15m": int(len(closed_15m))},
        }

    last_close = float(closes.iloc[-1])
    fast_now = float(ema_fast.iloc[-1])
    mid_now = float(ema_mid.iloc[-1])
    atr_now = float(atr.iloc[-1])
    atr_pct = (atr_now / max(last_close, 1e-9)) * 100.0
    recent = closed_5m.tail(12).copy()
    recent_closes = recent["close"].astype(float)
    move_12_pct = ((last_close / max(float(recent_closes.iloc[0]), 1e-9)) - 1.0) * 100.0
    consistency_5m = _trend_consistency(recent_closes)
    flip_ratio = _sign_flip_ratio(closed_5m.tail(14)["close"])
    wickiness = float(_wickiness(closed_5m.tail(10)).fillna(0.0).mean())
    body_ratio = float(_body_ratio(closed_5m.tail(10)).fillna(0.0).mean())
    distance_from_fast_atr = abs(last_close - fast_now) / max(atr_now, 1e-9)

    htf_bias = "neutral"
    htf_consistency = 0.0
    if len(closed_15m) >= SYMBOL_REGIME_MIN_15M_BARS:
        htf_closes = closed_15m["close"].astype(float)
        htf_ema_fast = _ema(htf_closes, min(SYMBOL_REGIME_EMA_FAST, max(5, len(htf_closes) // 2)))
        htf_move = float(htf_closes.iloc[-1] - htf_closes.iloc[max(0, len(htf_closes) - 6)])
        htf_bias = "up" if htf_closes.iloc[-1] > htf_ema_fast.iloc[-1] and htf_move > 0 else (
            "down" if htf_closes.iloc[-1] < htf_ema_fast.iloc[-1] and htf_move < 0 else "neutral"
        )
        htf_consistency = _trend_consistency(htf_closes.tail(8))

    bias = _bias_from_stack(last_close, fast_now, mid_now, move_12_pct)
    score = 0
    reasons = []
    if bias in {"up", "down"} and ((bias == "up" and last_close > fast_now > mid_now) or (bias == "down" and last_close < fast_now < mid_now)):
        score += 2
        reasons.append("ema_stack")
    if consistency_5m >= 0.62:
        score += 1
        reasons.append("consistent_5m")
    if body_ratio >= 0.44 and wickiness <= 0.50:
        score += 1
        reasons.append("clean_candles")
    if atr_pct >= SYMBOL_REGIME_EXPANSION_ATR_PCT:
        score += 1
        reasons.append("expansion")
    if htf_bias == bias and htf_consistency >= 0.55:
        score += 1
        reasons.append("htf_aligned")

    state = "symbol_unknown"
    reason = "symbol_regime_unknown"
    allow_breakout = True

    if atr_pct <= SYMBOL_REGIME_COMPRESSION_ATR_PCT and abs(move_12_pct) < (SYMBOL_REGIME_EXPANSION_ATR_PCT * 0.8):
        state = "symbol_compression"
        reason = "symbol_regime_compression"
        allow_breakout = True
    elif flip_ratio >= SYMBOL_REGIME_FLIP_RATIO or wickiness >= SYMBOL_REGIME_WICKY_AVG:
        state = "symbol_sweep_chop"
        reason = "symbol_regime_sweep_chop"
        allow_breakout = False
    elif distance_from_fast_atr >= SYMBOL_REGIME_EXHAUSTION_DISTANCE_ATR and consistency_5m < 0.68:
        state = "symbol_exhaustion"
        reason = "symbol_regime_exhaustion"
        allow_breakout = False
    elif score >= SYMBOL_REGIME_TREND_MIN_SCORE and bias in {"up", "down"}:
        state = "symbol_continuation_clean"
        reason = "symbol_regime_continuation_clean"
        allow_breakout = True
    elif bias in {"up", "down"} and consistency_5m >= 0.58 and flip_ratio < SYMBOL_REGIME_FLIP_RATIO:
        state = "symbol_continuation_clean"
        reason = "symbol_regime_continuation_soft"
        allow_breakout = True

    return {
        "state": state,
        "bias": bias,
        "reason": reason,
        "allow_breakout": bool(allow_breakout),
        "score": int(score),
        "version": SYMBOL_REGIME_VERSION,
        "symbol": str(symbol or ""),
        "metrics": {
            "bars_5m": int(len(closed_5m)),
            "bars_15m": int(len(closed_15m)),
            "atr_pct": round(float(atr_pct), 4),
            "move_12_pct": round(float(move_12_pct), 4),
            "consistency_5m": round(float(consistency_5m), 4),
            "flip_ratio": round(float(flip_ratio), 4),
            "avg_wickiness": round(float(wickiness), 4),
            "avg_body_ratio": round(float(body_ratio), 4),
            "distance_from_fast_atr": round(float(distance_from_fast_atr), 4),
            "htf_bias": htf_bias,
            "htf_consistency": round(float(htf_consistency), 4),
            "score_reasons": reasons,
        },
    }
