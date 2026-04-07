import tests._bootstrap

import importlib
import math
import os
import sys
import types

import pandas as pd

if 'telegram' not in sys.modules:
    telegram = types.ModuleType('telegram')
    class Bot: ...
    telegram.Bot = Bot
    sys.modules['telegram'] = telegram

if 'app.realtime_pipeline' not in sys.modules:
    sys.modules['app.realtime_pipeline'] = types.SimpleNamespace(enqueue_signal_dispatch=lambda *_args, **_kwargs: None)
if 'app.observability' not in sys.modules:
    sys.modules['app.observability'] = types.SimpleNamespace(heartbeat=lambda *_args, **_kwargs: None)
if 'app.plans' not in sys.modules:
    sys.modules['app.plans'] = types.SimpleNamespace(PLAN_FREE='free', PLAN_PLUS='plus', PLAN_PREMIUM='premium')



def _fake_ta_module():
    def ema_indicator(series, window, *args, **kwargs):
        values = []
        for idx, value in enumerate(series.tolist()):
            if idx + 1 < int(window):
                values.append(float('nan'))
            else:
                values.append(float(value))
        return pd.Series(values, index=series.index)

    def adx(high, low, close, *args, **kwargs):
        if len(close) == 0:
            return pd.Series(dtype=float)
        return pd.Series([25.0] * len(close), index=close.index)

    def average_true_range(high, low, close, *args, **kwargs):
        if len(close) == 0:
            return pd.Series(dtype=float)
        return pd.Series([1.0] * len(close), index=close.index)

    trend = types.SimpleNamespace(ema_indicator=ema_indicator, adx=adx)
    volatility = types.SimpleNamespace(average_true_range=average_true_range)
    return types.SimpleNamespace(trend=trend, volatility=volatility)



def _reload_modules():
    sys.modules['ta'] = _fake_ta_module()
    sys.modules.pop('app.strategy', None)
    sys.modules.pop('app.scanner', None)
    strategy = importlib.import_module('app.strategy')
    scanner = importlib.import_module('app.scanner')
    return strategy, scanner



def test_strategy_required_history_bars_covers_ema200_warmup():
    strategy, _scanner = _reload_modules()
    assert strategy._required_history_bars() >= strategy.EMA_SLOW + 30
    assert strategy._required_history_bars() >= 260



def test_strategy_marks_indicator_warmup_instead_of_trend_structure():
    strategy, _scanner = _reload_modules()

    rows = []
    for idx in range(100):
        rows.append(
            {
                'open': 100.0,
                'high': 101.0,
                'low': 99.0,
                'close': 100.0 + (idx * 0.01),
                'volume': 1000.0,
            }
        )
    df_5m = pd.DataFrame(rows)
    df_15m = pd.DataFrame(rows * 3)
    df_1h = pd.DataFrame(rows * 3)
    debug = {}

    result = strategy.mtf_strategy(df_1h=df_1h, df_15m=df_15m, df_5m=df_5m, debug_counts=debug)

    assert result is None
    assert debug.get('insufficient_history', 0) == 1 or debug.get('indicator_warmup', 0) >= 1



def test_scanner_enforces_safe_kline_minimums_when_env_is_too_low(monkeypatch):
    monkeypatch.setenv('SCANNER_KLINE_LIMIT_5M', '64')
    monkeypatch.setenv('SCANNER_KLINE_LIMIT_15M', '96')
    monkeypatch.setenv('SCANNER_KLINE_LIMIT_1H', '96')

    strategy, scanner = _reload_modules()

    assert scanner.KLINE_LIMIT_5M >= strategy._required_history_bars()
    assert scanner.KLINE_LIMIT_15M >= 220
    assert scanner.KLINE_LIMIT_1H >= 220



def test_scanner_coerces_dataframe_reference_price_to_last_close():
    _strategy, scanner = _reload_modules()
    frame = pd.DataFrame([
        {'close': 99.5},
        {'close': 100.25},
    ])
    assert math.isclose(scanner._coerce_reference_price(frame), 100.25, rel_tol=0, abs_tol=1e-9)
