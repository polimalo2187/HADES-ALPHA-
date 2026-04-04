from datetime import timedelta

import pandas as pd

import app.strategy as strategy


def test_closed_15m_frame_drops_open_candle_and_keeps_closed_history():
    now = pd.Timestamp.now(tz="UTC")
    df = pd.DataFrame(
        [
            {"close_time": now - timedelta(minutes=15), "close": 1.0},
            {"close_time": now + timedelta(minutes=10), "close": 2.0},
        ]
    )

    closed = strategy._closed_15m_frame(df)

    assert len(closed) == 1
    assert float(closed.iloc[-1]["close"]) == 1.0


def test_market_entry_candidate_rejects_price_that_breaks_rr_or_stop_side():
    assert strategy._market_entry_candidate(99.7, 99.8, "LONG", 103.2, 102.8, strategy.PLUS_PROFILE) is None
    assert strategy._market_entry_candidate(102.95, 99.8, "LONG", 103.2, 102.8, strategy.PLUS_PROFILE) is None


def test_evaluate_direction_uses_pending_entry_from_liquidity_model(monkeypatch):
    rows = []
    base_time = pd.Timestamp.now(tz="UTC") - timedelta(minutes=(strategy.LIQUIDITY_LOOKBACK + 3) * 15)
    for idx in range(strategy.LIQUIDITY_LOOKBACK + 2):
        open_time = base_time + timedelta(minutes=idx * 15)
        close_time = open_time + timedelta(minutes=15)
        rows.append(
            {
                "open": 100.0,
                "close": 100.8,
                "high": 104.0,
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
                "open_time": open_time,
                "close_time": close_time,
            }
        )

    df = pd.DataFrame(rows)
    df_1h = pd.DataFrame(rows + rows[:30])

    monkeypatch.setattr(strategy, "_higher_timeframe_context_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_select_liquidity_zone", lambda *_args, **_kwargs: {"price": 100.1, "count": 3, "latest_index": 10})
    monkeypatch.setattr(strategy, "_recovery_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_confirmation_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_ema_reclaim_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_nearest_barrier_price", lambda *_args, **_kwargs: 103.8)

    result = strategy._evaluate_direction(df, df_1h, "LONG", strategy.FREE_PROFILE, current_market_price=100.2)

    assert result is not None
    payload, ranking = result
    assert payload["send_mode"] == "entry_zone_pending"
    assert payload["entry_sent_price"] is None
    assert payload["entry_price"] == payload["entry_model_price"]
    assert payload["entry_price"] != round(100.4, 8)
    assert payload["setup_stage"] == "closed_confirmed"
    assert ranking[1] > 0


def test_evaluate_direction_emits_numeric_component_breakdown(monkeypatch):
    rows = []
    base_time = pd.Timestamp.now(tz="UTC") - timedelta(minutes=(strategy.LIQUIDITY_LOOKBACK + 3) * 15)
    for idx in range(strategy.LIQUIDITY_LOOKBACK + 2):
        open_time = base_time + timedelta(minutes=idx * 15)
        close_time = open_time + timedelta(minutes=15)
        rows.append(
            {
                "open": 100.0,
                "close": 100.8,
                "high": 104.0,
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
                "open_time": open_time,
                "close_time": close_time,
            }
        )

    df = pd.DataFrame(rows)
    df_1h = pd.DataFrame(rows + rows[:30])

    monkeypatch.setattr(strategy, "_select_liquidity_zone", lambda *_args, **_kwargs: {"price": 100.1, "count": 3, "latest_index": 10})
    monkeypatch.setattr(strategy, "_recovery_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_confirmation_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_ema_reclaim_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_nearest_barrier_price", lambda *_args, **_kwargs: 103.8)

    result = strategy._evaluate_direction(df, df_1h, "LONG", strategy.FREE_PROFILE, current_market_price=100.2)

    assert result is not None
    payload, _ranking = result
    assert payload["components"]
    assert isinstance(payload["components"][0], dict)
    assert payload["components"][0]["points"] is not None
    assert payload["raw_components"][0]["points"] is not None
    assert payload["normalized_components"][0]["points"] is not None


def test_strategy_profiles_keep_strictness_order_after_rebalance():
    assert strategy.SCORE_CALIBRATION_VERSION == "v14_reference_close_guarded"

    assert strategy.PREMIUM_PROFILE["min_rel_volume"] > strategy.PLUS_PROFILE["min_rel_volume"] > strategy.FREE_PROFILE["min_rel_volume"]
    assert strategy.PREMIUM_PROFILE["min_confirm_rel_volume"] > strategy.PLUS_PROFILE["min_confirm_rel_volume"] > strategy.FREE_PROFILE["min_confirm_rel_volume"]
    assert strategy.PREMIUM_PROFILE["min_confirm_body_ratio"] > strategy.PLUS_PROFILE["min_confirm_body_ratio"] > strategy.FREE_PROFILE["min_confirm_body_ratio"]
    assert strategy.PREMIUM_PROFILE["min_rr"] > strategy.PLUS_PROFILE["min_rr"] > strategy.FREE_PROFILE["min_rr"]
    assert strategy.PREMIUM_PROFILE["min_barrier_rr"] > strategy.PLUS_PROFILE["min_barrier_rr"] > strategy.FREE_PROFILE["min_barrier_rr"]
    assert strategy.PREMIUM_PROFILE["max_countertrend_atr"] < strategy.PLUS_PROFILE["max_countertrend_atr"] < strategy.FREE_PROFILE["max_countertrend_atr"]
    assert strategy.PREMIUM_PROFILE["min_sweep_range_atr"] > strategy.PLUS_PROFILE["min_sweep_range_atr"] > strategy.FREE_PROFILE["min_sweep_range_atr"]


def test_pending_entry_actionable_rejects_already_away_signals():
    assert strategy._pending_entry_actionable("SHORT", 100.0, 99.6) is False
    assert strategy._pending_entry_actionable("LONG", 100.0, 100.5) is False
    assert strategy._pending_entry_actionable("SHORT", 100.0, 100.4) is True
    assert strategy._pending_entry_actionable("LONG", 100.0, 99.6) is True
    assert strategy._pending_entry_actionable("SHORT", 100.0, 99.9) is True


def test_evaluate_direction_rejects_stale_pending_short_on_send(monkeypatch):
    rows = []
    base_time = pd.Timestamp.now(tz="UTC") - timedelta(minutes=(strategy.LIQUIDITY_LOOKBACK + 3) * 15)
    for idx in range(strategy.LIQUIDITY_LOOKBACK + 2):
        open_time = base_time + timedelta(minutes=idx * 15)
        close_time = open_time + timedelta(minutes=15)
        rows.append(
            {
                "open": 100.0,
                "close": 100.8,
                "high": 104.0,
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
                "open_time": open_time,
                "close_time": close_time,
            }
        )

    df = pd.DataFrame(rows)
    df_1h = pd.DataFrame(rows + rows[:30])

    monkeypatch.setattr(strategy, "_higher_timeframe_context_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_select_liquidity_zone", lambda *_args, **_kwargs: {"price": 100.1, "count": 3, "latest_index": 10})
    monkeypatch.setattr(strategy, "_recovery_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_confirmation_candle_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_ema_reclaim_ok", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(strategy, "_nearest_barrier_price", lambda *_args, **_kwargs: 96.0)
    monkeypatch.setattr(strategy, "_htf_context_snapshot", lambda *_args, **_kwargs: {"ok": True})

    result = strategy._evaluate_direction(df, df_1h, "SHORT", strategy.FREE_PROFILE, current_market_price=99.7)

    assert result is None


def test_pending_entry_actionable_accepts_small_favorable_move():
    assert strategy._pending_entry_actionable("SHORT", 100.0, 99.72) is True
    assert strategy._pending_entry_actionable("LONG", 100.0, 100.28) is True


def test_mtf_strategy_accepts_reference_market_price_and_debug_counts(monkeypatch):
    captured = {}

    def fake_liquidity(df_1h, df_15m, df_5m=None, reference_market_price=None, debug_counts=None):
        captured["reference_market_price"] = reference_market_price
        captured["debug_counts"] = debug_counts
        return {"ok": True}

    monkeypatch.setattr(strategy, "liquidity_sweep_reversal_strategy", fake_liquidity)
    debug = {}
    result = strategy.mtf_strategy(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), reference_market_price=100.2, debug_counts=debug)
    assert result == {"ok": True}
    assert captured["reference_market_price"] == 100.2
    assert captured["debug_counts"] is debug
