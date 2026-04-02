import importlib
import math
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


def _load_strategy():
    sys.modules.setdefault("ta", _fake_ta_module())
    sys.modules.pop("app.strategy", None)
    return importlib.import_module("app.strategy")


def test_adaptive_entry_blend_moves_closer_to_close_when_pullback_is_weak():
    strategy = _load_strategy()
    quality = {
        "retest_distance_atr": 0.40,
        "continuation_body_ratio": 0.54,
        "close_extension_atr": 0.88,
    }

    blend = strategy._adaptive_entry_blend(quality, strategy.SHARED_PROFILE)

    assert blend > 0.55
    assert blend <= strategy.SHARED_PROFILE["entry_blend_max"]


def test_adaptive_entry_blend_stays_patient_when_retest_is_clean():
    strategy = _load_strategy()
    quality = {
        "retest_distance_atr": 0.05,
        "continuation_body_ratio": 0.24,
        "close_extension_atr": 0.16,
    }

    blend = strategy._adaptive_entry_blend(quality, strategy.FREE_PROFILE)

    assert math.isclose(blend, strategy.FREE_PROFILE["entry_blend_min"], rel_tol=0.0, abs_tol=0.06)
    assert blend < 0.35


def test_short_adaptive_entry_blend_is_more_aggressive_than_long_for_same_quality():
    strategy = _load_strategy()
    quality = {
        "retest_distance_atr": 0.32,
        "continuation_body_ratio": 0.42,
        "close_extension_atr": 0.74,
    }

    long_blend = strategy._adaptive_entry_blend(quality, strategy.SHARED_PROFILE, direction="LONG")
    short_blend = strategy._adaptive_entry_blend(quality, strategy.SHARED_PROFILE, direction="SHORT")

    assert short_blend > long_blend
    assert short_blend >= strategy.SHARED_PROFILE["short_entry_blend_min"]
