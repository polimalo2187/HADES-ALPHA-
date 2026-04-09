import tests._bootstrap

import sys
import types
import pymongo

if 'telegram' not in sys.modules:
    telegram = types.ModuleType('telegram')
    class Bot: ...
    telegram.Bot = Bot
    sys.modules['telegram'] = telegram

if not hasattr(pymongo, 'UpdateOne'):
    class UpdateOne:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
    pymongo.UpdateOne = UpdateOne

errors_mod = sys.modules.get('pymongo.errors')
if errors_mod is not None and not hasattr(errors_mod, 'BulkWriteError'):
    class BulkWriteError(Exception):
        pass
    errors_mod.BulkWriteError = BulkWriteError

import pandas as pd

import app.scanner as scanner


def test_apply_close_market_execution_rejects_late_entry():
    result = {
        'direction': 'LONG',
        'entry_price': 100.0,
        'stop_loss': 99.0,
        'take_profits': [101.5, 101.95],
        'profiles': {
            'conservador': {'stop_loss': 99.0, 'take_profits': [101.5, 101.95], 'leverage': '20x-30x'},
            'moderado': {'stop_loss': 99.0, 'take_profits': [101.75, 102.35], 'leverage': '30x-40x'},
            'agresivo': {'stop_loss': 99.0, 'take_profits': [102.05, 102.75], 'leverage': '40x-50x'},
        },
        'score': 82.0,
        'components': ['liquidity_zone', 'confirmation_candle'],
    }

    assert scanner._apply_close_market_execution(result, current_price=101.0) is None


def test_build_candidate_preserves_pending_entry_metadata():
    reference_price = 100.1
    result = {
        'direction': 'SHORT',
        'entry_price': 100.0,
        'stop_loss': 100.8,
        'take_profits': [98.8, 98.44],
        'profiles': {
            'conservador': {'stop_loss': 100.8, 'take_profits': [98.8, 98.44], 'leverage': '20x-30x'},
            'moderado': {'stop_loss': 100.8, 'take_profits': [98.6, 98.12], 'leverage': '30x-40x'},
            'agresivo': {'stop_loss': 100.8, 'take_profits': [98.36, 97.8], 'leverage': '40x-50x'},
        },
        'score': 90.0,
        'raw_score': 90.0,
        'normalized_score': 90.0,
        'setup_group': 'premium',
        'score_profile': 'premium',
        'send_mode': 'entry_zone_pending',
        'components': ['liquidity_zone'],
    }

    candidate = scanner._build_candidate('BTCUSDT', result, reference_price)

    assert candidate is not None
    assert candidate['symbol'] == 'BTCUSDT'
    assert candidate['setup_group'] == 'premium'
    assert candidate['send_mode'] == 'entry_zone_pending'
    assert candidate['entry_price'] == 100.0
    assert candidate['signal_market_price'] == reference_price


def test_mtf_strategy_routes_premium_then_plus_then_free(monkeypatch):
    import app.strategy as strategy

    bars = strategy._required_history_bars()
    df = pd.DataFrame([
        {
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        }
        for _ in range(bars)
    ])

    monkeypatch.setattr(strategy, "add_indicators", lambda frame: frame.assign(ema20=1.0, ema50=1.0, ema200=1.0, adx=25.0, atr=0.01, atr_pct=0.01, body_ratio=0.3))

    responses = [
        {"direction": "LONG", "entry_price": 1.0, "trade_profiles": {"conservador": {"stop_loss": 0.9, "take_profits": [1.1, 1.2]}}, "score": 90.0, "raw_score": 90.0, "normalized_score": 90.0, "components": [], "raw_components": [], "normalized_components": [], "atr_pct": 0.01, "score_calibration": "v", "higher_tf_context": {}, "send_mode": "entry_zone_pending", "setup_stage": "pre_reset_waiting_retest", "entry_model": "m", "entry_model_price": 1.0, "reset_level": 1.0, "reset_close_price": 1.0},
        None,
        {"direction": "SHORT", "entry_price": 1.0, "trade_profiles": {"conservador": {"stop_loss": 1.1, "take_profits": [0.9, 0.8]}}, "score": 84.0, "raw_score": 84.0, "normalized_score": 84.0, "components": [], "raw_components": [], "normalized_components": [], "atr_pct": 0.01, "score_calibration": "v", "higher_tf_context": {}, "send_mode": "entry_zone_pending", "setup_stage": "pre_reset_waiting_retest", "entry_model": "m", "entry_model_price": 1.0, "reset_level": 1.0, "reset_close_price": 1.0},
        None,
        None,
        {"direction": "LONG", "entry_price": 1.0, "trade_profiles": {"conservador": {"stop_loss": 0.9, "take_profits": [1.1, 1.2]}}, "score": 76.0, "raw_score": 76.0, "normalized_score": 70.0, "components": [], "raw_components": [], "normalized_components": [], "atr_pct": 0.01, "score_calibration": "v", "higher_tf_context": {}, "send_mode": "entry_zone_pending", "setup_stage": "pre_reset_waiting_retest", "entry_model": "m", "entry_model_price": 1.0, "reset_level": 1.0, "reset_close_price": 1.0},
    ]

    def fake_evaluate(*_args, **_kwargs):
        return responses.pop(0)

    monkeypatch.setattr(strategy, "_evaluate_profile", fake_evaluate)

    premium = strategy.mtf_strategy(df, df, df.copy())
    plus = strategy.mtf_strategy(df, df, df.copy())
    free = strategy.mtf_strategy(df, df, df.copy())

    assert premium["setup_group"] == "premium"
    assert premium["score_profile"] == "premium"
    assert plus["setup_group"] == "plus"
    assert plus["score_profile"] == "plus"
    assert free["setup_group"] == "free"
    assert free["score_profile"] == "free"



def test_strategy_requires_reference_price_beyond_entry_side_for_prereset_signal(monkeypatch):
    import app.strategy as strategy

    bars = strategy._required_history_bars()
    rows = []
    for idx in range(bars):
        rows.append({
            "open": 100.0,
            "high": 100.3,
            "low": 99.7,
            "close": 100.0,
            "volume": 1000.0,
        })
    df = pd.DataFrame(rows)

    def fake_add_indicators(frame):
        enriched = frame.copy()
        enriched["ema20"] = 101.0
        enriched["ema50"] = 100.7
        enriched["ema200"] = 100.2
        enriched["adx"] = 25.0
        enriched["atr"] = 1.0
        enriched["atr_pct"] = 0.01
        enriched["body_ratio"] = 0.4
        enriched["vol_ma"] = 1000.0
        # controlamos las dos últimas velas: breakout + continuación sin reset
        enriched.iloc[-2, enriched.columns.get_loc("close")] = 101.6
        enriched.iloc[-2, enriched.columns.get_loc("high")] = 101.9
        enriched.iloc[-2, enriched.columns.get_loc("low")] = 100.8
        enriched.iloc[-2, enriched.columns.get_loc("open")] = 100.9
        enriched.iloc[-2, enriched.columns.get_loc("body_ratio")] = 0.52
        enriched.iloc[-1, enriched.columns.get_loc("close")] = 101.9
        enriched.iloc[-1, enriched.columns.get_loc("high")] = 102.2
        enriched.iloc[-1, enriched.columns.get_loc("low")] = 101.2
        enriched.iloc[-1, enriched.columns.get_loc("open")] = 101.4
        enriched.iloc[-1, enriched.columns.get_loc("body_ratio")] = 0.38
        return enriched

    monkeypatch.setattr(strategy, "add_indicators", fake_add_indicators)
    monkeypatch.setattr(strategy, "_passes_profile_score_floor", lambda *_args, **_kwargs: True)

    # Reference price por debajo de la entrada esperada: no debe publicar pre-reset.
    blocked = strategy.mtf_strategy(df, df, df.copy(), reference_market_price=100.05)
    assert blocked is None

    # Precio ya extendido por encima del nivel: ahora sí debe anticipar el reset.
    candidate = strategy.mtf_strategy(df, df, df.copy(), reference_market_price=101.1)
    assert candidate is not None
    assert candidate["send_mode"] == "entry_zone_pending"
    assert candidate["setup_stage"] == "pre_reset_waiting_retest"
    assert float(candidate["entry_price"]) < 101.1


def test_mtf_strategy_downgrades_premium_candidate_to_plus_when_premium_floor_not_met(monkeypatch):
    import app.strategy as strategy

    bars = strategy._required_history_bars()
    df = pd.DataFrame([
        {
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        }
        for _ in range(bars)
    ])

    monkeypatch.setattr(
        strategy,
        "add_indicators",
        lambda frame: frame.assign(ema20=1.0, ema50=1.0, ema200=1.0, adx=25.0, atr=0.01, atr_pct=0.01, body_ratio=0.3),
    )

    responses = [
        {
            "direction": "LONG",
            "entry_price": 1.0,
            "trade_profiles": {"conservador": {"stop_loss": 0.9, "take_profits": [1.1, 1.2]}},
            "score": 78.0,
            "raw_score": 78.0,
            "normalized_score": 78.0,
            "components": [],
            "raw_components": [],
            "normalized_components": [],
            "atr_pct": 0.01,
            "score_calibration": "v",
            "higher_tf_context": {},
            "send_mode": "entry_zone_pending",
            "setup_stage": "pre_reset_waiting_retest",
            "entry_model": "m",
            "entry_model_price": 1.0,
            "reset_level": 1.0,
            "reset_close_price": 1.0,
        },
        {
            "direction": "LONG",
            "entry_price": 1.0,
            "trade_profiles": {"conservador": {"stop_loss": 0.9, "take_profits": [1.1, 1.2]}},
            "score": 78.0,
            "raw_score": 78.0,
            "normalized_score": 78.0,
            "components": [],
            "raw_components": [],
            "normalized_components": [],
            "atr_pct": 0.01,
            "score_calibration": "v",
            "higher_tf_context": {},
            "send_mode": "entry_zone_pending",
            "setup_stage": "pre_reset_waiting_retest",
            "entry_model": "m",
            "entry_model_price": 1.0,
            "reset_level": 1.0,
            "reset_close_price": 1.0,
        },
    ]

    monkeypatch.setattr(strategy, "_evaluate_profile", lambda *_args, **_kwargs: responses.pop(0))

    result = strategy.mtf_strategy(df, df, df.copy())

    assert result is not None
    assert result["setup_group"] == "plus"
    assert result["score_profile"] == "plus"
    assert result["raw_score"] == 78.0


def test_continuation_filter_keeps_only_direction_and_body_as_hard_gate():
    import app.strategy as strategy

    profile = dict(strategy.PLUS_PROFILE)
    last = pd.Series({
        "open": 100.0,
        "high": 101.0,
        "low": 99.8,
        "close": 100.35,
        "body_ratio": 0.23,
        "volume": 900.0,
        "vol_ma": 1000.0,
        "atr": 1.0,
    })
    quality = {"level": 100.0}

    assert strategy._continuation_ok(last, "LONG", profile, quality) is True

    last["body_ratio"] = 0.10
    assert strategy._continuation_ok(last, "LONG", profile, quality) is False


def test_continuation_score_rewards_close_position_volume_and_progress():
    import app.strategy as strategy

    profile = dict(strategy.PLUS_PROFILE)
    weak = pd.Series({
        "open": 100.0,
        "high": 101.0,
        "low": 99.8,
        "close": 100.32,
        "body_ratio": 0.24,
        "volume": 950.0,
        "vol_ma": 1000.0,
        "atr": 1.0,
    })
    strong = pd.Series({
        "open": 100.0,
        "high": 101.2,
        "low": 99.9,
        "close": 101.02,
        "body_ratio": 0.42,
        "volume": 1500.0,
        "vol_ma": 1000.0,
        "atr": 1.0,
    })
    quality = {"level": 100.0}

    weak_score = strategy._continuation_score(weak, profile, "LONG", quality)
    strong_score = strategy._continuation_score(strong, profile, "LONG", quality)

    assert strong_score > weak_score


def test_profile_defaults_keep_free_plus_premium_hierarchy_after_rebalance():
    import app.strategy as strategy

    assert strategy.FREE_PROFILE["min_rel_volume_continuation"] < strategy.PLUS_PROFILE["min_rel_volume_continuation"] < strategy.PREMIUM_PROFILE["min_rel_volume_continuation"]
    assert strategy.FREE_PROFILE["min_close_position_continuation"] < strategy.PLUS_PROFILE["min_close_position_continuation"] < strategy.PREMIUM_PROFILE["min_close_position_continuation"]
    assert strategy.FREE_PROFILE["min_post_breakout_progress_atr"] < strategy.PLUS_PROFILE["min_post_breakout_progress_atr"] < strategy.PREMIUM_PROFILE["min_post_breakout_progress_atr"]
    assert strategy.FREE_RAW_SCORE_MIN < strategy.PLUS_RAW_SCORE_MIN < strategy.PREMIUM_RAW_SCORE_MIN
