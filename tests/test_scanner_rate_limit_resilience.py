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


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text='', headers=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'http_{self.status_code}')

    def json(self):
        return self._payload


def test_parse_binance_ban_until_supports_epoch_ms():
    ts = scanner._parse_binance_ban_until('Way too many requests; IP banned until 1775686499417. Please use websocket')
    assert ts is not None
    assert abs(ts - 1775686499.417) < 1e-6


def test_request_json_activates_cooldown_on_418(monkeypatch):
    scanner._request_gate.clear()
    now_holder = {'ts': 1000.0}
    monkeypatch.setattr(scanner, '_now_ts', lambda: now_holder['ts'])

    def fake_get(url, params=None, timeout=None):
        return _FakeResponse(
            status_code=418,
            text='{"code":-1003,"msg":"Way too many requests; IP banned until 1005000. Please use websocket"}',
        )

    monkeypatch.setattr(scanner.requests, 'get', fake_get)

    try:
        scanner._request_json('https://example.invalid/test')
        raise AssertionError('expected BinanceRateLimitedError')
    except scanner.BinanceRateLimitedError as exc:
        assert exc.status_code == 418

    remaining = scanner._request_gate.remaining_seconds()
    assert remaining > 0
    scanner._request_gate.clear()


def test_active_symbol_cache_uses_ttl(monkeypatch):
    scanner._active_symbols_cache.clear()
    scanner._request_gate.clear()
    now_holder = {'ts': 2000.0}
    monkeypatch.setattr(scanner, '_now_ts', lambda: now_holder['ts'])
    call_count = {'n': 0}

    payload = [
        {'symbol': 'BTCUSDT', 'quoteVolume': '30000000'},
        {'symbol': 'ETHUSDT', 'quoteVolume': '25000000'},
    ]

    def fake_request_json(url, *, params=None, timeout=None):
        call_count['n'] += 1
        return payload

    monkeypatch.setattr(scanner, '_request_json', fake_request_json)

    first = scanner.get_active_futures_symbols()
    second = scanner.get_active_futures_symbols()

    assert first == ['BTCUSDT', 'ETHUSDT']
    assert second == first
    assert call_count['n'] == 1

    now_holder['ts'] += scanner.ACTIVE_SYMBOLS_CACHE_SECONDS + 1.0
    third = scanner.get_active_futures_symbols()
    assert third == first
    assert call_count['n'] == 2


def test_bootstrap_cycle_uses_batches_before_full_universe():
    symbols = [f'SYM{i:03d}USDT' for i in range(100)]
    first, first_bootstrap = scanner._select_symbols_for_cycle(symbols, 0)
    second, second_bootstrap = scanner._select_symbols_for_cycle(symbols, 1)
    after_warmup, after_bootstrap = scanner._select_symbols_for_cycle(symbols, scanner._bootstrap_total_cycles(len(symbols)))

    assert first_bootstrap is True
    assert second_bootstrap is True
    assert len(first) == min(scanner.SCANNER_BOOTSTRAP_BATCH_SIZE, len(symbols))
    assert len(second) == min(scanner.SCANNER_BOOTSTRAP_BATCH_SIZE, max(0, len(symbols) - scanner.SCANNER_BOOTSTRAP_BATCH_SIZE))
    assert after_bootstrap is False
    assert after_warmup == symbols
