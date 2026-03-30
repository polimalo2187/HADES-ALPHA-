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


if __name__ == '__main__':
    unittest.main()
