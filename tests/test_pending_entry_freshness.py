import tests._bootstrap

from datetime import datetime, timedelta

import app.signals as signals


def test_calculate_entry_zone_adapts_to_risk_and_is_capped():
    low, high = signals.calculate_entry_zone(100.0, stop_loss=99.0)
    # 1% risk * 0.22 => 0.22% zone, wider than the old fixed 0.15%
    assert low == 99.78
    assert high == 100.22

    low2, high2 = signals.calculate_entry_zone(100.0, stop_loss=92.0)
    # capped by ENTRY_ZONE_MAX_PCT=0.35%
    assert low2 == 99.65
    assert high2 == 100.35


def test_pending_entry_guard_requires_long_to_be_above_zone_before_reset():
    ok, details = signals._pending_entry_is_still_actionable(
        direction="LONG",
        entry_price=100.0,
        stop_loss=99.2,
        take_profits=[100.8, 101.4],
        current_price=100.1,
        zone_low=99.78,
        zone_high=100.22,
    )
    assert ok is False
    assert details["actionability_reason"] == "already_in_reset_zone"


def test_pending_entry_guard_allows_armed_long_signal_above_zone():
    ok, details = signals._pending_entry_is_still_actionable(
        direction="LONG",
        entry_price=100.0,
        stop_loss=99.2,
        take_profits=[100.8, 101.4],
        current_price=100.6,
        zone_low=99.78,
        zone_high=100.22,
    )
    assert ok is True
    assert details["actionability_reason"] == "armed_waiting_reset"
    assert details["zone_distance_pct"] is not None


def test_pending_entry_guard_rejects_short_not_armed_above_entry_side():
    ok, details = signals._pending_entry_is_still_actionable(
        direction="SHORT",
        entry_price=100.0,
        stop_loss=100.8,
        take_profits=[99.2, 98.6],
        current_price=100.1,
        zone_low=99.78,
        zone_high=100.22,
    )
    assert ok is False
    assert details["actionability_reason"] in {"already_in_reset_zone", "pre_reset_not_armed"}
