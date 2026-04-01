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
    sys.modules.setdefault("ta", _fake_ta_module())
    telegram = types.SimpleNamespace(Bot=object)
    sys.modules.setdefault("telegram", telegram)
    sys.modules.setdefault("app.realtime_pipeline", types.SimpleNamespace(enqueue_signal_dispatch=lambda *_args, **_kwargs: None))
    sys.modules.setdefault("app.database", types.SimpleNamespace(signals_collection=lambda: None))
    sys.modules.setdefault("app.signals", types.SimpleNamespace(create_base_signal=lambda **kwargs: kwargs))
    sys.modules.setdefault("app.observability", types.SimpleNamespace(heartbeat=lambda *_args, **_kwargs: None))
    sys.modules.setdefault("app.plans", types.SimpleNamespace(PLAN_FREE="free", PLAN_PLUS="plus", PLAN_PREMIUM="premium"))
    sys.modules.pop("app.scanner", None)
    return importlib.import_module("app.scanner")


def test_scanner_treats_premium_setup_group_as_premium_candidate():
    scanner = _load_scanner()

    assert scanner._qualifies_for_premium({"setup_group": "premium", "raw_score": 82.0}) is True
    assert scanner._qualifies_for_premium({"setup_group": "shared", "raw_score": 80.0}) is True
    assert scanner._qualifies_for_premium({"setup_group": "shared", "raw_score": 75.0}) is False
