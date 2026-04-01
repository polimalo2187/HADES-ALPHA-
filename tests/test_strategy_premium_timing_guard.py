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


def _load_strategy():
    sys.modules.setdefault("ta", _fake_ta_module())
    sys.modules.pop("app.strategy", None)
    return importlib.import_module("app.strategy")


def test_premium_emit_guard_blocks_signal_when_tp1_progress_is_too_high():
    strategy = _load_strategy()

    allowed = strategy._premium_emit_is_fresh(
        entry_price=100.0,
        current_price=100.5,
        tp1_price=101.5,
        direction="LONG",
        profile=strategy.PREMIUM_LSR_PROFILE,
    )

    blocked = strategy._premium_emit_is_fresh(
        entry_price=100.0,
        current_price=101.0,
        tp1_price=101.5,
        direction="LONG",
        profile=strategy.PREMIUM_LSR_PROFILE,
    )

    assert allowed is True
    assert blocked is False


def test_premium_confirmation_allows_early_valid_follow_through():
    strategy = _load_strategy()
    profile = strategy.PREMIUM_LSR_PROFILE
    sweep = {
        "close": 100.0,
        "low": 99.7,
        "high": 100.6,
    }
    confirm = {
        "body_ratio": 0.13,
        "rel_volume": 0.62,
        "atr": 1.0,
        "close": 100.28,
        "open": 100.08,
        "high": 100.35,
        "low": 100.02,
    }

    assert strategy._premium_confirmation_candle_ok(confirm, sweep, "LONG", profile, 100.0) is True
