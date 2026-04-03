from datetime import datetime, timedelta, timezone

import pandas as pd

import app.strategy as strategy


def test_live_confirmation_ready_uses_time_progress_body_and_projected_volume():
    now = datetime.now(timezone.utc)
    confirm = pd.Series({
        "open": 100.0,
        "close": 100.8,
        "high": 101.0,
        "low": 99.9,
        "body_ratio": 0.75,
        "rel_volume": 0.52,
        "open_time": now - timedelta(minutes=9),
        "close_time": now + timedelta(minutes=6),
    })
    sweep = pd.Series({"close": 100.2, "high": 100.4, "low": 99.7})
    assert strategy._live_confirmation_ready(confirm, sweep, "LONG", strategy.PREMIUM_PROFILE) is True


def test_live_entry_candidate_rejects_price_that_breaks_rr_or_stop_side():
    assert strategy._live_entry_candidate(99.7, 99.8, "LONG", 103.2, 102.8, strategy.PLUS_PROFILE) is None
    assert strategy._live_entry_candidate(102.95, 99.8, "LONG", 103.2, 102.8, strategy.PLUS_PROFILE) is None



def test_evaluate_direction_falls_back_to_structural_when_live_entry_is_not_viable(monkeypatch):
    now = datetime.now(timezone.utc)
    rows = []
    for idx in range(strategy.LIQUIDITY_LOOKBACK + 2):
        rows.append(
            {
                "open": 100.0,
                "close": 100.8,
                "high": 101.2,
                "low": 99.6,
                "volume": 1000.0,
                "atr": 1.0,
                "atr_pct": 0.01,
                "rel_volume": 1.5,
                "body": 0.8,
                "range": 1.6,
                "body_ratio": 0.5,
                "upper_wick": 0.2,
                "lower_wick": 0.2,
                "ema20": 100.5,
                "ema50": 100.4,
                "open_time": now - timedelta(minutes=(strategy.LIQUIDITY_LOOKBACK + 2 - idx) * 15),
                "close_time": now - timedelta(minutes=(strategy.LIQUIDITY_LOOKBACK + 1 - idx) * 15),
            }
        )

    df = pd.DataFrame(rows)
    df_1h = pd.DataFrame(rows + rows[:30])

    monkeypatch.setattr(strategy, "_higher_timeframe_context_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_select_liquidity_zone", lambda *_args, **_kwargs: {"price": 100.1, "count": 3, "latest_index": 10})
    monkeypatch.setattr(strategy, "_recovery_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_live_confirmation_ready", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_confirmation_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_ema_reclaim_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_nearest_barrier_price", lambda *_args, **_kwargs: 103.0)
    monkeypatch.setattr(strategy, "_live_entry_candidate", lambda *_args, **_kwargs: None)

    result = strategy._evaluate_direction(df, df_1h, "LONG", strategy.FREE_PROFILE, current_market_price=101.9)

    assert result is not None
    payload, ranking = result
    assert payload["send_mode"] == "structural"
    assert payload["entry_price"] == payload["entry_model_price"]
    assert ranking[1] > 0
