import tests._bootstrap
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.database import _reconcile_payment_orders_tx_hash_index


class PaymentOrderIndexMigrationTests(unittest.TestCase):
    def test_reconcile_replaces_legacy_sparse_tx_hash_index(self):
        collection = MagicMock()
        collection.list_indexes.return_value = [
            {"name": "_id_", "key": {"_id": 1}},
            {"name": "matched_tx_hash_unique", "key": {"matched_tx_hash": 1}, "unique": True, "sparse": True},
        ]
        collection.update_many.return_value = SimpleNamespace(modified_count=2)

        with patch('app.database.payment_orders_collection', return_value=collection):
            _reconcile_payment_orders_tx_hash_index()

        collection.update_many.assert_called_once_with(
            {"matched_tx_hash": None},
            {"$unset": {"matched_tx_hash": ""}},
        )
        collection.drop_index.assert_called_once_with('matched_tx_hash_unique')

    def test_reconcile_keeps_modern_partial_index_untouched(self):
        collection = MagicMock()
        collection.list_indexes.return_value = [
            {
                "name": "matched_tx_hash_unique",
                "key": {"matched_tx_hash": 1},
                "unique": True,
                "partialFilterExpression": {"matched_tx_hash": {"$type": "string"}},
            },
        ]

        with patch('app.database.payment_orders_collection', return_value=collection):
            _reconcile_payment_orders_tx_hash_index()

        collection.update_many.assert_not_called()
        collection.drop_index.assert_not_called()


if __name__ == '__main__':
    unittest.main()
