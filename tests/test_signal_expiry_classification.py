import tests._bootstrap

from datetime import datetime, timedelta
from unittest.mock import patch

from app.signals import _evaluate_signal_result


def _base_signal(direction: str = "LONG"):
    now = datetime.utcnow()
    return {
        "symbol": "BTCUSDT",
        "direction": direction,
        "entry_price": 100.0,
        "stop_loss": 98.0 if direction == "LONG" else 102.0,
        "take_profits": [102.0 if direction == "LONG" else 98.0, 104.0 if direction == "LONG" else 96.0],
        "created_at": now - timedelta(minutes=20),
        "evaluation_valid_until": now,
    }


def test_expired_no_fill_when_entry_never_touches():
    signal = _base_signal("LONG")
    klines = [
        [1, 101.2, 101.5, 100.4, 101.0, 0],
        [2, 101.0, 101.3, 100.2, 100.9, 0],
    ]

    with patch('app.signals._fetch_klines_between', return_value=klines):
        evaluation = _evaluate_signal_result(signal)

    assert evaluation["result"] == "expired"
    assert evaluation["resolution"] == "expired_no_fill"
    assert evaluation["entry_touched"] is False
    assert evaluation["expiry_type"] == "no_fill"
    assert evaluation["tp1_progress_max_pct"] == 0.0


def test_expired_after_entry_when_signal_fills_but_never_develops():
    signal = _base_signal("LONG")
    klines = [
        [1, 100.6, 101.0, 99.9, 100.4, 0],
        [2, 100.4, 101.1, 99.7, 100.2, 0],
    ]

    with patch('app.signals._fetch_klines_between', return_value=klines):
        evaluation = _evaluate_signal_result(signal)

    assert evaluation["result"] == "expired"
    assert evaluation["resolution"] == "expired_after_entry"
    assert evaluation["entry_touched"] is True
    assert evaluation["expiry_type"] == "after_entry_no_followthrough"
    assert evaluation["tp1_progress_max_pct"] > 0
    assert evaluation["max_favorable_excursion_r"] > 0


def test_short_expiry_after_entry_is_classified_correctly():
    signal = _base_signal("SHORT")
    klines = [
        [1, 100.5, 100.7, 99.6, 100.1, 0],
        [2, 100.1, 100.4, 99.5, 99.9, 0],
    ]

    with patch('app.signals._fetch_klines_between', return_value=klines):
        evaluation = _evaluate_signal_result(signal)

    assert evaluation["resolution"] == "expired_after_entry"
    assert evaluation["entry_touched"] is True
    assert evaluation["max_adverse_excursion_r"] >= 0
