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
