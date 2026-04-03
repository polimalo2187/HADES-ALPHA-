import pandas as pd

import app.strategy as strategy


def test_live_entry_candidate_revalidates_rr_from_current_price():
    candidate = strategy._live_entry_candidate(
        current_price=101.0,
        stop_loss=99.8,
        direction="LONG",
        structure_target=103.2,
        nearest_barrier=102.8,
        profile=strategy.PLUS_PROFILE,
    )
    assert candidate is not None
    entry_price, trade_profiles, room_rr, barrier_rr = candidate
    assert round(entry_price, 4) == 101.0
    assert trade_profiles["conservador"]["stop_loss"] == round(99.8, 8)
    assert room_rr >= strategy.PLUS_PROFILE["min_rr"]
    assert barrier_rr >= strategy.PLUS_PROFILE["min_barrier_rr"]


def test_current_candle_progress_without_timestamps_falls_back_to_closed():
    candle = pd.Series({"open": 1.0, "close": 1.1})
    assert strategy._current_candle_progress(candle) == 1.0
