import tests._bootstrap

from app.statistics import _build_strategy_direction_stats, _build_strategy_stats


def test_strategy_breakdown_uses_signal_metadata_when_results_do_not_persist_strategy_yet():
    results = [
        {"base_signal_id": "sig-breakout", "direction": "LONG", "result": "won", "resolution": "tp1", "r_multiple": 1.0},
        {"base_signal_id": "sig-liquidity", "direction": "SHORT", "result": "expired", "resolution": "expired_no_fill", "entry_touched": False},
        {"base_signal_id": "sig-liquidity", "direction": "SHORT", "result": "won", "resolution": "tp2", "r_multiple": 2.0},
    ]
    signals = [
        {"_id": "sig-breakout", "strategy_name": "breakout_reset", "send_mode": "market_on_close", "normalized_score": 91.0},
        {"_id": "sig-liquidity", "strategy_name": "liquidity_sweep_reversal", "send_mode": "entry_zone_pending", "normalized_score": 87.0},
    ]

    strategy_rows = _build_strategy_stats(results, signals)
    direction_rows = _build_strategy_direction_stats(results, signals)

    assert [row["strategy_key"] for row in strategy_rows] == ["breakout_reset", "liquidity_sweep_reversal"]
    assert strategy_rows[0]["strategy_label"] == "Breakout + Reset"
    assert strategy_rows[0]["primary_send_mode_label"] == "Entrada al envío"
    assert strategy_rows[0]["resolved"] == 1
    assert strategy_rows[1]["expired_no_fill"] == 1
    assert strategy_rows[1]["signals_total"] == 1
    assert strategy_rows[1]["primary_send_mode_label"] == "Entrada por pullback"

    assert [row["direction"] for row in direction_rows] == ["LONG", "SHORT"]
    assert direction_rows[1]["strategy_key"] == "liquidity_sweep_reversal"
    assert direction_rows[1]["resolved"] == 1
    assert direction_rows[1]["expired_no_fill"] == 1
