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

from datetime import timedelta

import pandas as pd

import app.scanner as scanner


def test_closed_15m_frame_drops_open_candle_and_keeps_closed_history():
    now = pd.Timestamp.now(tz='UTC')
    df = pd.DataFrame(
        [
            {'close_time': now - timedelta(minutes=15), 'close': 1.0},
            {'close_time': now + timedelta(minutes=10), 'close': 2.0},
        ]
    )

    closed = scanner._closed_15m_frame(df)

    assert len(closed) == 1
    assert float(closed.iloc[-1]['close']) == 1.0


def test_apply_close_market_execution_uses_market_price_and_keeps_original_filters():
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

    payload = scanner._apply_close_market_execution(result, current_price=100.1)

    assert payload is not None
    assert payload['send_mode'] == 'market_on_close'
    assert payload['entry_price'] == round(100.1, 8)
    assert payload['entry_sent_price'] == round(100.1, 8)
    assert payload['entry_model_price'] == round(100.0, 8)
    assert payload['setup_group'] == 'plus'
    assert payload['setup_stage'] == 'closed_confirmed'
    assert payload['tp1_progress_at_send_pct'] >= 0.0
