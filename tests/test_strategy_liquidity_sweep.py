from datetime import timedelta

import pandas as pd

from tests import _bootstrap  # noqa: F401
from app import strategy_liquidity_sweep as strategy


def test_liquidity_strategy_emits_market_on_close_candidate(monkeypatch):
    rows = []
    base_time = pd.Timestamp.now(tz="UTC") - timedelta(minutes=(strategy.LIQUIDITY_LOOKBACK + 8) * 15)
    for idx in range(strategy.LIQUIDITY_LOOKBACK + 6):
        open_time = base_time + timedelta(minutes=idx * 15)
        close_time = open_time + timedelta(minutes=15)
        rows.append(
            {
                "open": 100.0,
                "close": 100.8,
                "high": 101.4,
                "low": 99.7,
                "volume": 1000.0 + idx,
                "open_time": open_time,
                "close_time": close_time,
            }
        )

    df_15m = pd.DataFrame(rows)
    df_1h = pd.DataFrame(rows + rows[:40])

    monkeypatch.setattr(strategy, "_higher_timeframe_context_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_select_liquidity_zone", lambda *_args, **_kwargs: {"price": 100.1, "count": 3, "latest_index": 10})
    monkeypatch.setattr(strategy, "_find_recent_sweep", lambda *_args, **_kwargs: {"index": 10, "sweep_distance_atr": 0.6, "wick_ratio": 0.5})
    monkeypatch.setattr(strategy, "_recovery_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_confirmation_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_ema_reclaim_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_nearest_barrier_price", lambda *_args, **_kwargs: 103.8)

    result = strategy.mtf_strategy(df_1h=df_1h, df_15m=df_15m, df_5m=None, reference_market_price=100.4, debug_counts={})

    assert result is not None
    assert result["send_mode"] == "market_on_close"
    assert result["entry_price"] == result["entry_sent_price"]
    assert result["entry_price"] == round(100.4, 4)
    assert result["entry_model"] == strategy.ENTRY_MODEL_NAME
    assert result["setup_stage"] == strategy.SETUP_STAGE_CLOSED_CONFIRMED
