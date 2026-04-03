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
    sys.modules.setdefault("app.observability", types.SimpleNamespace(heartbeat=lambda *_args, **_kwargs: None))
    sys.modules.setdefault("app.plans", types.SimpleNamespace(PLAN_FREE="free", PLAN_PLUS="plus", PLAN_PREMIUM="premium"))
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
