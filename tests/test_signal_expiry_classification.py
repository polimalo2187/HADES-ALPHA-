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


def test_signal_cannot_fill_for_first_time_after_telegram_window_closes():
    now = datetime.utcnow()
    signal = {
        **_base_signal("SHORT"),
        "created_at": now - timedelta(minutes=20),
        "telegram_valid_until": now - timedelta(minutes=5),
        "evaluation_valid_until": now,
    }
    klines = [
        [1, 99.8, 99.9, 99.1, 99.4, 0, int((signal["created_at"] + timedelta(minutes=1)).timestamp() * 1000)],
        [2, 99.4, 101.4, 99.2, 101.1, 0, int((signal["telegram_valid_until"] + timedelta(minutes=1)).timestamp() * 1000)],
        [3, 101.1, 102.4, 100.8, 102.1, 0, int((signal["evaluation_valid_until"]).timestamp() * 1000)],
    ]

    with patch('app.signals._fetch_klines_between', return_value=klines):
        evaluation = _evaluate_signal_result(signal)

    assert evaluation["result"] == "expired"
    assert evaluation["resolution"] == "expired_no_fill"
    assert evaluation["entry_touched"] is False
    assert evaluation["expiry_type"] == "no_fill"


def test_signal_can_still_resolve_after_fill_that_happened_before_telegram_window_closes():
    now = datetime.utcnow()
    signal = {
        **_base_signal("SHORT"),
        "created_at": now - timedelta(minutes=20),
        "telegram_valid_until": now - timedelta(minutes=10),
        "evaluation_valid_until": now,
    }
    entry_row_open = signal["created_at"] + timedelta(minutes=2)
    entry_row_close = entry_row_open + timedelta(minutes=1)
    sl_row_open = signal["telegram_valid_until"] + timedelta(minutes=2)
    sl_row_close = sl_row_open + timedelta(minutes=1)
    klines = [
        [1, 100.1, 100.5, 99.4, 100.0, 0, int(entry_row_close.timestamp() * 1000)],
        [2, 100.0, 102.3, 99.7, 102.0, 0, int(sl_row_close.timestamp() * 1000)],
    ]

    with patch('app.signals._fetch_klines_between', return_value=klines):
        evaluation = _evaluate_signal_result(signal)

    assert evaluation["result"] == "lost"
    assert evaluation["resolution"] == "sl"
    assert evaluation["entry_touched"] is True


def test_pending_short_zone_that_never_retraces_cannot_finish_as_sl():
    now = datetime.utcnow()
    signal = {
        **_base_signal("SHORT"),
        "entry_price": 100.0,
        "entry_zone": {"low": 99.85, "high": 100.15},
        "signal_market_price": 101.0,
        "send_mode": "entry_zone_pending",
        "stop_loss": 102.0,
        "take_profits": [98.0, 96.0],
        "created_at": now - timedelta(minutes=20),
        "telegram_valid_until": now - timedelta(minutes=5),
        "evaluation_valid_until": now,
    }
    klines = [
        [1, 101.0, 101.6, 100.4, 101.3, 0, int((signal["created_at"] + timedelta(minutes=2)).timestamp() * 1000)],
        [2, 101.3, 102.4, 100.3, 102.1, 0, int((signal["created_at"] + timedelta(minutes=8)).timestamp() * 1000)],
        [3, 102.1, 102.5, 100.2, 102.2, 0, int(signal["evaluation_valid_until"].timestamp() * 1000)],
    ]

    with patch('app.signals._fetch_klines_between', return_value=klines):
        evaluation = _evaluate_signal_result(signal)

    assert evaluation["result"] == "expired"
    assert evaluation["resolution"] == "expired_no_fill"
    assert evaluation["entry_touched"] is False
