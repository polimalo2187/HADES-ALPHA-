import tests._bootstrap

import pandas as pd

from app import strategy_breakout_reset as strategy


def _recent_breakout_frame(*, invalidated: bool = False) -> pd.DataFrame:
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

    # Breakout happened four closed candles ago. Previous implementation only
    # accepted the immediately previous candle and would reject this shape.
    breakout_pos = len(df) - 5
    df.loc[breakout_pos, ["open", "high", "low", "close", "body_ratio"]] = [100.2, 100.95, 100.12, 100.75, 0.55]

    # Follow-through stays above the broken level without touching the reset.
    df.loc[len(df) - 4, ["open", "high", "low", "close", "body_ratio"]] = [100.72, 101.05, 100.58, 100.92, 0.42]
    df.loc[len(df) - 3, ["open", "high", "low", "close", "body_ratio"]] = [100.91, 101.10, 100.50, 100.82, 0.33]
    df.loc[len(df) - 2, ["open", "high", "low", "close", "body_ratio"]] = [100.80, 100.98, 100.47, 100.70, 0.28]
    df.loc[len(df) - 1, ["open", "high", "low", "close", "body_ratio"]] = [100.66, 100.88, 100.43, 100.74, 0.26]

    if invalidated:
        # A decisive close back inside the old range invalidates the setup even
        # if later candles recover above the level.
        df.loc[len(df) - 3, ["open", "high", "low", "close", "body_ratio"]] = [100.72, 100.78, 99.92, 100.02, 0.70]

    return df


def test_recent_breakout_window_accepts_alive_setup_after_multiple_closed_candles():
    ok, quality, reason = strategy._confirm_breakout_prereset(
        _recent_breakout_frame(),
        "LONG",
        dict(strategy.FREE_PROFILE),
        reference_market_price=100.28,
    )

    assert ok is True
    assert reason is None
    assert quality["level"] == 100.3
    assert quality["breakout_age_bars"] == 4.0
    assert quality["post_breakout_bars"] == 4.0
    assert quality["recent_breakout_window_bars"] >= 4.0
    assert quality["pre_reset_space_atr"] > strategy.FREE_PROFILE["min_pre_reset_space_atr"]


def test_recent_breakout_window_rejects_setup_closed_back_inside_range():
    ok, quality, reason = strategy._confirm_breakout_prereset(
        _recent_breakout_frame(invalidated=True),
        "LONG",
        dict(strategy.FREE_PROFILE),
        reference_market_price=100.28,
    )

    assert ok is False
    assert quality == {}
    assert reason in {"breakout_invalidated", "breakout_drifted_back_inside"}
