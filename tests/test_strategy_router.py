import pandas as pd

from tests import _bootstrap  # noqa: F401
from app import strategy_router


def _dummy_frames():
    ts = pd.Timestamp("2026-04-10T00:00:00Z")
    df = pd.DataFrame([
        {"open_time": ts, "close_time": ts, "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 1000.0}
    ])
    return df, df, df


def test_route_candidate_selects_breakout_when_regime_is_continuation(monkeypatch):
    df_1h, df_15m, df_5m = _dummy_frames()
    monkeypatch.setattr(strategy_router.breakout_strategy, "mtf_strategy", lambda **kwargs: {"direction": "LONG", "entry_price": 1.0, "stop_loss": 0.9, "take_profits": [1.1, 1.2], "profiles": {"conservador": {"stop_loss": 0.9, "take_profits": [1.1, 1.2]}}, "score": 90.0, "raw_score": 90.0, "normalized_score": 90.0, "components": [], "raw_components": [], "normalized_components": [], "setup_group": "premium", "score_profile": "premium", "score_calibration": "v", "send_mode": "entry_zone_pending", "setup_stage": "pre_reset", "entry_model": "breakout", "entry_model_price": 1.0})

    result = strategy_router.route_candidate(
        symbol="BTCUSDT",
        df_1h=df_1h,
        df_15m=df_15m,
        df_5m=df_5m,
        market_regime={"state": "continuation_clean", "strategy_name": "breakout_reset", "bias": "up", "reason": "ok"},
        reference_market_price=1.0,
        debug_counts={},
    )

    assert result is not None
    assert result["strategy_name"] == "breakout_reset"
    assert result["regime_state"] == "continuation_clean"


def test_route_candidate_selects_liquidity_when_regime_is_sweep(monkeypatch):
    df_1h, df_15m, df_5m = _dummy_frames()
    monkeypatch.setattr(strategy_router.liquidity_strategy, "mtf_strategy", lambda **kwargs: {"direction": "SHORT", "entry_price": 1.0, "stop_loss": 1.1, "take_profits": [0.9, 0.8], "profiles": {"conservador": {"stop_loss": 1.1, "take_profits": [0.9, 0.8]}}, "score": 84.0, "raw_score": 84.0, "normalized_score": 84.0, "components": [], "raw_components": [], "normalized_components": [], "setup_group": "plus", "score_profile": "plus", "score_calibration": "v", "send_mode": "market_on_close", "setup_stage": "closed_confirmed", "entry_model": "liquidity", "entry_model_price": 1.0})

    result = strategy_router.route_candidate(
        symbol="ETHUSDT",
        df_1h=df_1h,
        df_15m=df_15m,
        df_5m=df_5m,
        market_regime={"state": "sweep_reversal", "strategy_name": "liquidity_sweep_reversal", "bias": "neutral", "reason": "ok"},
        reference_market_price=1.0,
        debug_counts={},
    )

    assert result is not None
    assert result["strategy_name"] == "liquidity_sweep_reversal"
    assert result["regime_state"] == "sweep_reversal"
