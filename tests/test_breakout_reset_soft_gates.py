import tests._bootstrap

import pandas as pd

from app import strategy_breakout_reset as strategy


def _soft_gate_frame(*, overshoot_close: float = 100.37) -> pd.DataFrame:
    rows = []
    for _ in range(80):
        rows.append({
            "open": 100.0,
            "high": 100.3,
            "low": 99.7,
            "close": 100.0,
            "volume": 1000.0,
        })
    df = pd.DataFrame(rows)
    df["ema20"] = 101.0
    df["ema50"] = 100.7
    df["ema200"] = 100.2
    df["adx"] = 25.0
    df["atr"] = 1.0
    df["atr_pct"] = 0.01
    df["body_ratio"] = 0.40
    df["vol_ma"] = 1000.0

    breakout_pos = len(df) - 4
    df.loc[breakout_pos, ["open", "high", "low", "close", "body_ratio"]] = [100.0, 100.60, 100.12, overshoot_close, 0.70]
    df.loc[len(df) - 3, ["open", "high", "low", "close", "body_ratio"]] = [100.40, 100.90, 100.42, 100.74, 0.45]
    df.loc[len(df) - 2, ["open", "high", "low", "close", "body_ratio"]] = [100.72, 100.88, 100.45, 100.70, 0.28]
    df.loc[len(df) - 1, ["open", "high", "low", "close", "body_ratio"]] = [100.66, 100.84, 100.41, 100.74, 0.26]
    return df


def test_breakout_quality_below_profile_threshold_becomes_soft_gate_not_hard_reject():
    ok, quality, reason = strategy._confirm_breakout_prereset(
        _soft_gate_frame(overshoot_close=100.37),
        "LONG",
        dict(strategy.FREE_PROFILE),
        reference_market_price=None,
    )

    assert ok is True
    assert reason is None
    assert quality["soft_gate_breakout_overshoot"] == 1.0
    assert quality["soft_gate_count"] >= 1.0


def test_breakout_quality_below_hard_safety_floor_still_rejects():
    ok, quality, reason = strategy._confirm_breakout_prereset(
        _soft_gate_frame(overshoot_close=100.32),
        "LONG",
        dict(strategy.FREE_PROFILE),
        reference_market_price=None,
    )

    assert ok is False
    assert quality == {}
    assert reason == "breakout_overshoot_hard"
