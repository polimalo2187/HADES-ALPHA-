import tests._bootstrap
import unittest
from unittest.mock import patch

from app import watchlist


class WatchlistStorageTests(unittest.TestCase):
    def test_ensure_doc_does_not_repeat_fields_between_set_and_set_on_insert(self):
        with patch.object(watchlist, 'collection') as collection:
            watchlist._ensure_doc(123)

        args, kwargs = collection.update_one.call_args
        update = args[1]
        self.assertNotIn('updated_at', update['$setOnInsert'])
        self.assertNotIn('schema_version', update['$setOnInsert'])
        self.assertEqual(update['$setOnInsert']['symbols'], [])
        self.assertTrue(kwargs.get('upsert'))

    def test_set_symbols_upsert_seed_does_not_repeat_mutated_fields(self):
        with patch.object(watchlist, 'collection') as collection, \
             patch.object(watchlist, 'get_valid_symbols', return_value=set()):
            ok, _ = watchlist.set_symbols(123, ['BTC', 'ETH'], plan='PLUS')

        self.assertTrue(ok)
        args, kwargs = collection.update_one.call_args
        update = args[1]
        self.assertNotIn('symbols', update['$setOnInsert'])
        self.assertNotIn('updated_at', update['$setOnInsert'])
        self.assertNotIn('schema_version', update['$setOnInsert'])
        self.assertEqual(update['$set']['symbols'], ['BTCUSDT', 'ETHUSDT'])
        self.assertTrue(kwargs.get('upsert'))

    def test_add_symbol_rescues_symbol_missing_from_bulk_universe(self):
        with patch.object(watchlist, 'collection') as collection, \
             patch.object(watchlist, 'get_symbols', return_value=[]), \
             patch.object(watchlist, 'get_valid_symbols', return_value={*(f'COIN{i}USDT' for i in range(60)), 'BTCUSDT', 'ETHUSDT'}), \
             patch.object(watchlist, 'get_public_24h_ticker_for_symbol', return_value={'symbol': 'PLAYUSDT', 'lastPrice': '1', 'provider': 'binance'}):
            ok, message = watchlist.add_symbol(123, 'PLAY', plan='PLUS')

        self.assertTrue(ok)
        self.assertIn('PLAYUSDT', message)
        args, kwargs = collection.update_one.call_args
        self.assertEqual(args[1]['$addToSet']['symbols'], 'PLAYUSDT')

    def test_add_symbol_rejects_only_after_bulk_and_direct_lookup_fail(self):
        with patch.object(watchlist, 'get_valid_symbols', return_value={*(f'COIN{i}USDT' for i in range(60)), 'BTCUSDT', 'ETHUSDT'}), \
             patch.object(watchlist, 'get_public_24h_ticker_for_symbol', return_value={}):
            ok, message = watchlist.add_symbol(123, 'NOTREALUSDT', plan='PLUS')

        self.assertFalse(ok)
        self.assertIn('proveedores públicos activos', message)

    def test_set_symbols_reports_rejected_symbols_instead_of_silent_skip(self):
        def fake_lookup(symbol, allow_direct_fetch=True):
            return {'symbol': symbol, 'lastPrice': '1', 'provider': 'binance'} if symbol == 'PLAYUSDT' else {}

        with patch.object(watchlist, 'collection') as collection, \
             patch.object(watchlist, 'get_valid_symbols', return_value={*(f'COIN{i}USDT' for i in range(60)), 'BTCUSDT'}), \
             patch.object(watchlist, 'get_public_24h_ticker_for_symbol', side_effect=fake_lookup):
            ok, message = watchlist.set_symbols(123, ['BTC', 'PLAY', 'NOTREAL'], plan='PREMIUM')

        self.assertTrue(ok)
        self.assertIn('No añadí NOTREALUSDT', message)
        args, kwargs = collection.update_one.call_args
        self.assertEqual(args[1]['$set']['symbols'], ['BTCUSDT', 'PLAYUSDT'])


if __name__ == '__main__':
    unittest.main()
