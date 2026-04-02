import pandas as pd

import app.strategy as strategy


def test_decide_send_mode_structural_repriced_and_late():
    assert strategy._decide_send_mode(100.0, 100.15, 101.5, 99.0, "LONG") == "structural"
    assert strategy._decide_send_mode(100.0, 100.30, 101.5, 99.0, "LONG") == "repriced"
    assert strategy._decide_send_mode(100.0, 100.90, 101.5, 99.0, "LONG") == "discarded_late"


def test_current_candle_progress_without_timestamps_falls_back_to_closed():
    candle = pd.Series({"open": 1.0, "close": 1.1})
    assert strategy._current_candle_progress(candle) == 1.0
