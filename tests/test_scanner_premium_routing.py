import pandas as pd
import importlib
import sys
import types


def _fake_ta_module():
    trend = types.SimpleNamespace(
        ema_indicator=lambda series, *_args, **_kwargs: series,
        adx=lambda *_args, **_kwargs: 20.0,
    )
    volatility = types.SimpleNamespace(
        average_true_range=lambda high, low, close, *_args, **_kwargs: close * 0 + 1.0,
    )
    return types.SimpleNamespace(trend=trend, volatility=volatility)


def _load_scanner():
    sys.modules.setdefault("telegram", types.SimpleNamespace(Bot=object))
    sys.modules.setdefault("ta", _fake_ta_module())
    sys.modules.setdefault("app.realtime_pipeline", types.SimpleNamespace(enqueue_signal_dispatch=lambda *_args, **_kwargs: None))
    sys.modules.setdefault("app.database", types.SimpleNamespace(signals_collection=lambda: None))
    sys.modules.setdefault("app.signals", types.SimpleNamespace(create_base_signal=lambda **kwargs: kwargs))
    sys.modules.setdefault("app.observability", types.SimpleNamespace(heartbeat=lambda *_args, **_kwargs: None, log_event=lambda *_args, **_kwargs: None, record_audit_event=lambda *_args, **_kwargs: None))
    sys.modules.setdefault("app.plans", types.SimpleNamespace(PLAN_FREE="free", PLAN_PLUS="plus", PLAN_PREMIUM="premium", SUBSCRIPTION_STATUS_EXPIRED="expired", normalize_plan=lambda value: str(value).lower(), activate_plan_purchase=lambda *args, **kwargs: None, get_plan_price=lambda *args, **kwargs: 0, validate_plan_duration=lambda *args, **kwargs: True))
    sys.modules.pop("app.scanner", None)
    return importlib.import_module("app.scanner")


def test_scanner_routes_only_native_profile_candidates():
    scanner = _load_scanner()

    assert scanner._qualifies_for_premium({"setup_group": "premium", "raw_score": 90.0}) is True
    assert scanner._qualifies_for_premium({"setup_group": "plus", "raw_score": 90.0}) is False
    assert scanner._qualifies_for_premium({"setup_group": "free", "raw_score": 90.0}) is False

    assert scanner._qualifies_for_plus({"setup_group": "plus", "raw_score": 82.0}) is True
    assert scanner._qualifies_for_plus({"setup_group": "premium", "raw_score": 82.0}) is False

    assert scanner._qualifies_for_free({"setup_group": "free", "raw_score": 74.0}) is True
    assert scanner._qualifies_for_free({"setup_group": "plus", "raw_score": 74.0}) is False



def test_select_dispatchable_signal_skips_duplicate_and_uses_next_candidate(monkeypatch):
    scanner = _load_scanner()

    duplicate = {"symbol": "AAAUSDT", "direction": "LONG", "entry_price": 1.0, "stop_loss": 0.9, "take_profits": [1.1], "timeframes": ["15M"], "raw_score": 90.0, "normalized_score": 90.0, "setup_group": "premium", "score_profile": "premium", "score_calibration": "v1"}
    fallback = {"symbol": "BBBUSDT", "direction": "LONG", "entry_price": 2.0, "stop_loss": 1.8, "take_profits": [2.2], "timeframes": ["15M"], "raw_score": 90.0, "normalized_score": 90.0, "setup_group": "premium", "score_profile": "premium", "score_calibration": "v1"}

    monkeypatch.setattr(scanner, "recent_duplicate_exists", lambda symbol, *_args: symbol == "AAAUSDT")
    monkeypatch.setattr(scanner, "create_base_signal", lambda **kwargs: {"symbol": kwargs["symbol"], "visibility": kwargs["visibility"]})

    chosen = scanner._select_dispatchable_signal([duplicate, fallback], "premium", set())

    assert chosen is not None
    signal, base_signal = chosen
    assert signal["symbol"] == "BBBUSDT"
    assert base_signal["symbol"] == "BBBUSDT"


def test_scanner_interval_default_is_20_seconds():
    scanner = _load_scanner()
    assert scanner.SCAN_INTERVAL_SECONDS == 20


def test_build_symbol_candidate_omits_reference_market_price_when_strategy_does_not_support_it(monkeypatch):
    import app.scanner as scanner

    captured = {}

    def fake_strategy(*, df_1h, df_15m, df_5m):
        captured["df_1h"] = df_1h
        captured["df_15m"] = df_15m
        captured["df_5m"] = df_5m
        return None

    monkeypatch.setattr(scanner.strategy_engine, "mtf_strategy", fake_strategy)

    df = pd.DataFrame(columns=["close_time", "close"])
    result = scanner.build_symbol_candidate("BTCUSDT", df, df, df)

    assert result is None
    assert "df_1h" in captured


def test_build_symbol_candidate_passes_reference_market_price_when_strategy_supports_it(monkeypatch):
    import app.scanner as scanner

    captured = {}

    def fake_strategy(*, df_1h, df_15m, df_5m, reference_market_price=None, debug_counts=None):
        captured["reference_market_price"] = reference_market_price
        captured["debug_counts"] = debug_counts
        return None

    monkeypatch.setattr(scanner.strategy_engine, "mtf_strategy", fake_strategy)

    df_5m = pd.DataFrame([{"close_time": pd.Timestamp.now(tz="UTC"), "close": 123.45}])
    df_15m = pd.DataFrame([{"close_time": pd.Timestamp.now(tz="UTC"), "close": 120.0}])
    df_1h = pd.DataFrame([{"close_time": pd.Timestamp.now(tz="UTC"), "close": 121.0}])

    result = scanner.build_symbol_candidate("BTCUSDT", df_1h, df_15m, df_5m)

    assert result is None
    assert captured["reference_market_price"] == 123.45
    assert captured["debug_counts"] == {}


def test_scanner_disables_request_delay_when_concurrent():
    scanner = _load_scanner()
    assert scanner.SCANNER_SYMBOL_CONCURRENCY >= 1
    if scanner.SCANNER_SYMBOL_CONCURRENCY > 1 and not scanner.SCANNER_FORCE_REQUEST_DELAY:
        assert scanner.EFFECTIVE_REQUEST_DELAY == 0.0


def test_build_symbol_candidate_uses_reference_close_when_5m_disabled(monkeypatch):
    import app.scanner as scanner

    captured = {}

    def fake_strategy(*, df_1h, df_15m, df_5m=None, reference_market_price=None, debug_counts=None):
        captured["reference_market_price"] = reference_market_price
        return None

    monkeypatch.setattr(scanner.strategy_engine, "mtf_strategy", fake_strategy)

    ts = pd.Timestamp.now(tz="UTC")
    df_15m = pd.DataFrame([{"close_time": ts, "close": 120.0}])
    df_1h = pd.DataFrame([{"close_time": ts, "close": 121.0}])

    result = scanner.build_symbol_candidate("BTCUSDT", df_1h, df_15m, None, debug_counts={})

    assert result is None
    assert captured["reference_market_price"] == 120.0


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



def test_btc_regime_classifies_directional_uptrend_without_shock():
    scanner = _load_scanner()

    closes_5m = [100.0 + (idx * 0.22) for idx in range(40)]
    closes_15m = [100.0 + (idx * 0.35) for idx in range(16)]

    snapshot = scanner._classify_btc_regime(
        _make_ohlc_frame(closes_5m, step_minutes=5),
        _make_ohlc_frame(closes_15m, step_minutes=15),
    )

    assert snapshot["state"] == "trend_up"
    assert snapshot["bias"] == "up"
    assert snapshot["allow"] is True



def test_btc_regime_classifies_vol_shock_on_large_last_candle():
    scanner = _load_scanner()

    closes_5m = [100.0 + (idx * 0.05) for idx in range(39)] + [102.8]
    closes_15m = [100.0 + (idx * 0.08) for idx in range(16)]

    snapshot = scanner._classify_btc_regime(
        _make_ohlc_frame(closes_5m, step_minutes=5),
        _make_ohlc_frame(closes_15m, step_minutes=15),
    )

    assert snapshot["state"] == "vol_shock"
    assert snapshot["reason"] == "btc_regime_vol_shock"
    assert snapshot["allow"] is False



def test_btc_regime_guard_blocks_countertrend_signal():
    scanner = _load_scanner()

    candidate = {
        "symbol": "ENAUSDT",
        "direction": "SHORT",
        "raw_score": 88.0,
        "normalized_score": 84.0,
        "setup_group": "premium",
    }
    btc_regime = {"state": "trend_up", "bias": "up", "reason": "btc_regime_trend_up"}

    assert scanner._apply_btc_regime_guard(candidate, btc_regime) is None



def test_btc_regime_guard_allows_aligned_premium_during_cooldown():
    scanner = _load_scanner()

    candidate = {
        "symbol": "SOLUSDT",
        "direction": "SHORT",
        "raw_score": scanner.PREMIUM_RAW_SCORE_MIN + scanner.BTC_REGIME_PREMIUM_SHOCK_SCORE_BUFFER + 1.0,
        "normalized_score": 86.0,
        "setup_group": "premium",
    }
    btc_regime = {"state": "cooldown", "bias": "down", "reason": "btc_regime_cooldown"}

    guarded = scanner._apply_btc_regime_guard(candidate, btc_regime)

    assert guarded is not None
    assert guarded["btc_regime_guard_action"] == "allow_premium_aligned_cooldown"
