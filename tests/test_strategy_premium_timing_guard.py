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


def test_reprice_candidate_keeps_structural_stop_and_revalidates_rr():
    repriced = strategy._reprice_candidate(101.0, 99.8, "LONG", 103.2, 102.8, strategy.PLUS_PROFILE)
    assert repriced is not None
    entry_price, trade_profiles, room_rr, barrier_rr = repriced
    assert round(entry_price, 4) == 101.0
    assert trade_profiles["conservador"]["stop_loss"] == round(99.8, 6)
    assert room_rr >= strategy.PLUS_PROFILE["min_rr"]
    assert barrier_rr >= strategy.PLUS_PROFILE["min_barrier_rr"]
