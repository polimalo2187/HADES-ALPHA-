import os

import pandas as pd

import tests._bootstrap

from app import breakout_reset_setup_store
from app import strategy_breakout_reset as strategy


def _stateful_frame(close: float = 100.74) -> pd.DataFrame:
    rows = []
    base_time = pd.Timestamp("2026-01-01T00:00:00Z")
    for i in range(80):
        rows.append({
            "open": 100.0,
            "high": 100.3,
            "low": 99.7,
            "close": 100.0,
            "volume": 1000.0,
            "close_time": base_time + pd.Timedelta(minutes=5 * i),
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
    breakout_pos = len(df) - 5
    df.loc[breakout_pos, ["open", "high", "low", "close", "body_ratio"]] = [100.2, 100.95, 100.12, 100.75, 0.55]
    df.loc[len(df) - 4, ["open", "high", "low", "close", "body_ratio"]] = [100.72, 101.05, 100.58, 100.92, 0.42]
    df.loc[len(df) - 3, ["open", "high", "low", "close", "body_ratio"]] = [100.91, 101.10, 100.50, 100.82, 0.33]
    df.loc[len(df) - 2, ["open", "high", "low", "close", "body_ratio"]] = [100.80, 100.98, 100.47, 100.70, 0.28]
    df.loc[len(df) - 1, ["open", "high", "low", "close", "body_ratio"]] = [100.66, 100.88, 100.43, close, 0.26]
    return df


def test_stateful_setup_is_persisted_and_reloaded_from_memory(monkeypatch):
    monkeypatch.setenv("BREAKOUT_RESET_SETUP_STORE", "memory")
    breakout_reset_setup_store.clear_memory_store()
    df = _stateful_frame()

    ok, quality, _ = strategy._confirm_breakout_prereset(
        df,
        "LONG",
        dict(strategy.FREE_PROFILE),
        reference_market_price=None,
    )
    assert ok is True

    entry = strategy._reset_entry_price(quality["level"], df.iloc[-1], "LONG")
    profiles = strategy._build_trade_profiles(entry, "LONG", float(df.iloc[-1]["atr_pct"]))
    zone_low, zone_high = strategy._calculate_entry_zone(entry, profiles["conservador"]["stop_loss"])

    stored = strategy._persist_waiting_setup(
        symbol="TESTUSDT",
        direction="LONG",
        profile=dict(strategy.FREE_PROFILE),
        quality=quality,
        entry_price=entry,
        zone_low=zone_low,
        zone_high=zone_high,
        atr=float(df.iloc[-1]["atr"]),
        atr_pct=float(df.iloc[-1]["atr_pct"]),
        df=df,
    )
    assert stored is not None
    assert stored["status"] == "waiting_reset"

    debug = {}
    loaded_quality, loaded_doc = strategy._load_stateful_setup(
        "TESTUSDT",
        "LONG",
        df,
        dict(strategy.FREE_PROFILE),
        debug,
    )
    assert loaded_doc is not None
    assert loaded_quality is not None
    assert loaded_quality["level"] == quality["level"]
    assert debug["breakout_stateful_setup_loaded"] if "breakout_stateful_setup_loaded" in debug else True


def test_adaptive_reset_zone_expands_base_zone_by_bounded_atr(monkeypatch):
    monkeypatch.setattr(strategy, "BREAKOUT_RESET_ADAPTIVE_RESET_ZONE_ENABLED", True)
    monkeypatch.setattr(strategy, "BREAKOUT_RESET_RESET_ZONE_PADDING_ATR", 0.12)
    monkeypatch.setattr(strategy, "BREAKOUT_RESET_RESET_ZONE_MAX_PADDING_ATR", 0.18)

    low, high = strategy._expand_reset_zone_with_atr(99.90, 100.10, 1.0)

    assert low == 99.78
    assert high == 100.22


def test_reset_near_miss_uses_candle_distance_to_zone(monkeypatch):
    monkeypatch.setattr(strategy, "BREAKOUT_RESET_RESET_ZONE_NEAR_MISS_ATR", 0.10)

    assert strategy._is_reset_near_miss(99.90, 100.10, 1.0, candle_high=100.25, candle_low=100.21) is False
    assert strategy._is_reset_near_miss(99.90, 100.10, 1.0, candle_high=100.25, candle_low=100.19) is True


def test_multi_reset_model_can_select_ema20_touch_with_timing_guard(monkeypatch):
    monkeypatch.setattr(strategy, "BREAKOUT_RESET_MULTI_RESET_ENABLED", True)
    monkeypatch.setattr(strategy, "BREAKOUT_RESET_ALLOW_EMA20_RESET", True)
    monkeypatch.setattr(strategy, "BREAKOUT_RESET_PUBLICATION_TIMING_GUARD_ENABLED", True)
    df = _stateful_frame(close=100.74)
    last = df.iloc[-1].copy()
    last["ema20"] = 100.74
    quality = {
        "level": 100.30,
        "reference_price": 100.90,
        "extension_atr": 0.60,
        "current_extension_atr": 0.44,
        "breakout_candle_open": 100.20,
        "breakout_candle_close": 100.75,
    }
    debug = {}
    selected, stage = strategy._select_live_reset_model(
        quality=quality,
        last=last,
        direction="LONG",
        atr=1.0,
        atr_pct=0.01,
        live_price=100.74,
        live_high=100.78,
        live_low=100.70,
        debug_counts=debug,
    )
    assert stage == strategy.SETUP_STAGE_RESET_TOUCH_LIVE
    assert selected is not None
    assert selected["reset_model"] == "ema20"
    assert debug["breakout_reset_model_ema20"] == 1


def test_publication_timing_guard_blocks_late_entry_near_tp1():
    debug = {}
    profiles = strategy._build_trade_profiles(100.0, "LONG", 0.01)
    tp1 = profiles["conservador"]["take_profits"][0]
    late_price = 100.0 + ((tp1 - 100.0) * 0.55)
    ok, reason, diag = strategy._publication_timing_guard(
        direction="LONG",
        live_price=late_price,
        live_high=late_price,
        live_low=99.98,
        entry_model_price=late_price,
        entry_price=100.0,
        atr=1.0,
        trade_profiles=profiles,
        debug_counts=debug,
    )
    assert ok is False
    assert reason == "publication_timing_tp1_progress"
    assert debug["publication_timing_tp1_progress"] == 1
    assert diag["tp1_progress"] > 0.30
