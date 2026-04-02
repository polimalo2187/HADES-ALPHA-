import tests._bootstrap

from app.statistics import _calculate_stats_from_results


def test_statistics_use_r_model_and_exclude_clean_expired():
    rows = [
        {"result": "won", "resolution": "tp1", "r_multiple": 1.0},
        {"result": "won", "resolution": "tp2", "r_multiple": 2.0},
        {"result": "lost", "resolution": "sl", "r_multiple": -1.0},
        {"result": "expired", "resolution": "expired_no_fill", "entry_touched": False, "r_multiple": None},
        {"result": "expired", "resolution": "expired_after_entry", "entry_touched": True, "r_multiple": None},
    ]

    stats = _calculate_stats_from_results(rows)

    assert stats["tp1"] == 1
    assert stats["tp2"] == 1
    assert stats["sl"] == 1
    assert stats["expired"] == 2
    assert stats["expired_no_fill"] == 1
    assert stats["expired_after_entry"] == 1
    assert stats["filled_total"] == 4
    assert stats["resolved"] == 3
    assert stats["fill_rate"] == 80.0
    assert stats["no_fill_rate"] == 20.0
    assert stats["post_fill_expiry_rate"] == 20.0
    assert stats["after_entry_failure_rate"] == 25.0
    assert stats["profit_factor"] == 3.0
    assert stats["expectancy_r"] == 0.6667
    assert stats["gross_profit_r"] == 3.0
    assert stats["gross_loss_r"] == 1.0


def test_legacy_wins_are_normalized_to_tp1_or_tp2_r():
    rows = [
        {"result": "won", "r_multiple": 1.23},
        {"result": "won", "r_multiple": 1.76},
        {"result": "lost", "r_multiple": -0.75},
    ]
    stats = _calculate_stats_from_results(rows)
    assert stats["profit_factor"] == 3.0
    assert stats["tp1"] == 1
    assert stats["tp2"] == 1
    assert stats["sl"] == 1
