import pandas as pd

import app.strategy as strategy


def test_market_entry_candidate_revalidates_rr_from_current_price():
    candidate = strategy._market_entry_candidate(
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


def test_progress_metrics_from_model_entry_are_consistent():
    pct = strategy._progress_from_model_to_tp1_pct(100.0, 102.0, 100.5, "LONG")
    r = strategy._r_progress_from_model_entry(100.0, 99.0, 100.5, "LONG")
    assert pct == 25.0
    assert r == 0.5
