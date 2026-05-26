import pandas as pd

from tests import _bootstrap  # noqa: F401
from app import regime_engine


def _make_ohlc_frame(closes, *, step_minutes=5, base_ts="2026-04-10T00:00:00Z"):
    ts0 = pd.Timestamp(base_ts)
    rows = []
    prev_close = float(closes[0])
    for idx, close in enumerate(closes):
        close = float(close)
        open_price = prev_close
        high = max(open_price, close) + 0.12
        low = min(open_price, close) - 0.12
        rows.append(
            {
                "open_time": ts0 + pd.Timedelta(minutes=step_minutes * idx),
                "close_time": ts0 + pd.Timedelta(minutes=(step_minutes * idx) + step_minutes - 1),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1000.0 + idx,
            }
        )
        prev_close = close
    return pd.DataFrame(rows)


def test_classify_market_regime_detects_continuation_clean():
    closes_5m = [100.0 + (idx * 0.24) for idx in range(50)]
    closes_15m = [100.0 + (idx * 0.42) for idx in range(18)]

    snapshot = regime_engine.classify_market_regime(
        _make_ohlc_frame(closes_5m, step_minutes=5),
        _make_ohlc_frame(closes_15m, step_minutes=15),
        now_ts=1_000_000.0,
    )

    assert snapshot["state"] == "continuation_clean"
    assert snapshot["strategy_name"] == "breakout_reset"
    assert snapshot["allow"] is True


def test_classify_market_regime_detects_risk_off_on_shock():
    closes_5m = [100.0 + (idx * 0.05) for idx in range(49)] + [103.8]
    closes_15m = [100.0 + (idx * 0.10) for idx in range(18)]

    snapshot = regime_engine.classify_market_regime(
        _make_ohlc_frame(closes_5m, step_minutes=5),
        _make_ohlc_frame(closes_15m, step_minutes=15),
        now_ts=2_000_000.0,
    )

    assert snapshot["state"] == "risk_off"
    assert snapshot["strategy_name"] == "risk_off"
    assert snapshot["allow"] is False



def test_raw_market_regime_defaults_to_continuation_when_sweep_is_not_strong(monkeypatch):
    df_5m = _make_ohlc_frame([100.0 + (idx * 0.06) for idx in range(50)], step_minutes=5)
    df_15m = _make_ohlc_frame([100.0 + (idx * 0.10) for idx in range(18)], step_minutes=15)

    monkeypatch.setattr(regime_engine, "_body_ratio_series", lambda df: pd.Series([0.49] * len(df)))
    monkeypatch.setattr(regime_engine, "_wickiness_series", lambda df: pd.Series([0.47] * len(df)))
    monkeypatch.setattr(regime_engine, "_trend_consistency", lambda closes: 0.58)
    monkeypatch.setattr(regime_engine, "_sign_flip_ratio", lambda closes: 0.40)

    raw = regime_engine._classify_raw_market_regime(df_5m, df_15m)

    assert raw["raw_state"] == "continuation_clean"
    assert raw["reason"].startswith("market_regime_continuation")



def test_raw_market_regime_switches_to_sweep_only_on_strong_chop(monkeypatch):
    df_5m = _make_ohlc_frame([100.0 + ((-1) ** idx) * 0.03 for idx in range(50)], step_minutes=5)
    df_15m = _make_ohlc_frame([100.0 + ((-1) ** idx) * 0.02 for idx in range(18)], step_minutes=15)

    monkeypatch.setattr(regime_engine, "_body_ratio_series", lambda df: pd.Series([0.30] * len(df)))
    monkeypatch.setattr(regime_engine, "_wickiness_series", lambda df: pd.Series([0.62] * len(df)))
    monkeypatch.setattr(regime_engine, "_trend_consistency", lambda closes: 0.42)
    monkeypatch.setattr(regime_engine, "_sign_flip_ratio", lambda closes: 0.72)

    raw = regime_engine._classify_raw_market_regime(df_5m, df_15m)

    assert raw["raw_state"] == "sweep_reversal"
    assert raw["reason"] == "market_regime_sweep_reversal"


def _reset_regime_state():
    with regime_engine._state_lock:
        regime_engine._state.clear()
        regime_engine._state.update({
            "fetched_at_ts": 0.0,
            "state": "unknown",
            "raw_state": "unknown",
            "bias": "neutral",
            "allow": True,
            "reason": "test_reset",
            "risk_severity": "none",
            "entered_at_ts": 0.0,
            "candidate_state": None,
            "candidate_count": 0,
            "cooldown_until_ts": 0.0,
        })


def test_classify_market_regime_marks_moderate_risk_off_as_allowed():
    _reset_regime_state()
    closes_5m = [100.0 + (idx * 0.05) for idx in range(49)] + [102.8]
    closes_15m = [100.0 + (idx * 0.10) for idx in range(18)]

    snapshot = regime_engine.classify_market_regime(
        _make_ohlc_frame(closes_5m, step_minutes=5),
        _make_ohlc_frame(closes_15m, step_minutes=15),
        now_ts=3_000_000.0,
    )

    assert snapshot["state"] == "risk_off"
    assert snapshot["risk_severity"] == "moderate"
    assert snapshot["strategy_name"] == "breakout_reset"
    assert snapshot["allow"] is True
    assert snapshot["metrics"]["risk_severity"] == "moderate"


def test_classify_market_regime_keeps_severe_risk_off_as_hard_block():
    _reset_regime_state()
    closes_5m = [100.0 + (idx * 0.05) for idx in range(49)] + [103.8]
    closes_15m = [100.0 + (idx * 0.10) for idx in range(18)]

    snapshot = regime_engine.classify_market_regime(
        _make_ohlc_frame(closes_5m, step_minutes=5),
        _make_ohlc_frame(closes_15m, step_minutes=15),
        now_ts=4_000_000.0,
    )

    assert snapshot["state"] == "risk_off"
    assert snapshot["risk_severity"] == "severe"
    assert snapshot["strategy_name"] == "risk_off"
    assert snapshot["allow"] is False
    assert snapshot["metrics"]["severe_shock_trigger_count"] >= 1


def test_classify_market_regime_downgrades_cooldown_hold_to_moderate():
    _reset_regime_state()
    shock_5m = [100.0 + (idx * 0.05) for idx in range(49)] + [103.8]
    normal_5m = [100.0 + (idx * 0.20) for idx in range(50)]
    closes_15m = [100.0 + (idx * 0.10) for idx in range(18)]

    first = regime_engine.classify_market_regime(
        _make_ohlc_frame(shock_5m, step_minutes=5),
        _make_ohlc_frame(closes_15m, step_minutes=15),
        now_ts=5_000_000.0,
    )
    second = regime_engine.classify_market_regime(
        _make_ohlc_frame(normal_5m, step_minutes=5),
        _make_ohlc_frame(closes_15m, step_minutes=15),
        now_ts=5_000_060.0,
    )

    assert first["risk_severity"] == "severe"
    assert second["state"] == "risk_off"
    assert second["reason"] == "market_regime_cooldown_hold"
    assert second["risk_severity"] == "moderate"
    assert second["strategy_name"] == "breakout_reset"
    assert second["allow"] is True
