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
