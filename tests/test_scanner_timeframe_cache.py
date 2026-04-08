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

import app.scanner as scanner


def _fake_kline_payload(limit: int = 3):
    base_open = 1_700_000_000_000
    rows = []
    for idx in range(limit):
        open_time = base_open + (idx * 60_000)
        close_time = open_time + 59_999
        rows.append([
            open_time,
            '100.0',
            '101.0',
            '99.0',
            str(100.5 + idx),
            '123.45',
            close_time,
            '0',
            '0',
            '0',
            '0',
            '0',
        ])
    return rows


def test_15m_uses_bucket_cache(monkeypatch):
    scanner._kline_cache.clear()
    call_count = {'n': 0}
    now_holder = {'ts': 1000.0}

    monkeypatch.setattr(scanner, '_now_ts', lambda: now_holder['ts'])

    def fake_request_json(url, *, params=None, timeout=None):
        call_count['n'] += 1
        return _fake_kline_payload(limit=int(params['limit']))

    monkeypatch.setattr(scanner, '_request_json', fake_request_json)

    df1 = scanner.get_klines('BTCUSDT', '15m', limit=3)
    df2 = scanner.get_klines('BTCUSDT', '15m', limit=3)

    assert call_count['n'] == 1
    assert list(df1['close']) == list(df2['close'])
    assert df1 is not df2


def test_15m_cache_expires_on_new_bucket(monkeypatch):
    scanner._kline_cache.clear()
    call_count = {'n': 0}
    now_holder = {'ts': 1000.0}

    monkeypatch.setattr(scanner, '_now_ts', lambda: now_holder['ts'])

    def fake_request_json(url, *, params=None, timeout=None):
        call_count['n'] += 1
        return _fake_kline_payload(limit=int(params['limit']))

    monkeypatch.setattr(scanner, '_request_json', fake_request_json)

    scanner.get_klines('ETHUSDT', '15m', limit=3)
    now_holder['ts'] = 1900.0  # next 15m bucket
    scanner.get_klines('ETHUSDT', '15m', limit=3)

    assert call_count['n'] == 2


def test_5m_remains_uncached_by_default(monkeypatch):
    scanner._kline_cache.clear()
    call_count = {'n': 0}
    now_holder = {'ts': 1000.0}

    monkeypatch.setattr(scanner, '_now_ts', lambda: now_holder['ts'])

    def fake_request_json(url, *, params=None, timeout=None):
        call_count['n'] += 1
        return _fake_kline_payload(limit=int(params['limit']))

    monkeypatch.setattr(scanner, '_request_json', fake_request_json)

    scanner.get_klines('SOLUSDT', '5m', limit=3)
    scanner.get_klines('SOLUSDT', '5m', limit=3)

    assert call_count['n'] == 2
