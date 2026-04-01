import tests._bootstrap
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from app.miniapp.service import build_live_signals_payload


class MiniAppLiveSignalsServiceTests(unittest.TestCase):
    def test_live_signals_payload_exposes_feed_version_and_recent_items(self):
        now = datetime.utcnow()
        active_docs = [
            {'signal_id': 'sig-2', 'symbol': 'ETHUSDT', 'direction': 'SHORT', 'visibility': 'plus', 'created_at': now, 'updated_at': now, 'telegram_valid_until': now + timedelta(minutes=10)},
            {'signal_id': 'sig-1', 'symbol': 'BTCUSDT', 'direction': 'LONG', 'visibility': 'free', 'created_at': now - timedelta(minutes=1), 'updated_at': now - timedelta(minutes=1), 'telegram_valid_until': now + timedelta(minutes=5)},
        ]
        recent_docs = active_docs + [
            {'signal_id': 'sig-0', 'symbol': 'SOLUSDT', 'direction': 'LONG', 'visibility': 'premium', 'created_at': now - timedelta(minutes=5), 'updated_at': now - timedelta(minutes=3), 'telegram_valid_until': now - timedelta(minutes=1), 'result': 'won'},
        ]

        active_cursor = MagicMock()
        active_cursor.sort.return_value.limit.return_value = active_docs
        recent_cursor = MagicMock()
        recent_cursor.sort.return_value.limit.return_value = recent_docs
        collection = MagicMock()
        collection.find.side_effect = [active_cursor, recent_cursor]
        collection.count_documents.return_value = 2

        with patch('app.miniapp.service.user_signals_collection', return_value=collection):
            payload = build_live_signals_payload({'user_id': 55})

        self.assertEqual(payload['active_signals_count'], 2)
        self.assertEqual(len(payload['recent_signals']), 2)
        self.assertEqual(len(payload['signals']), 3)
        self.assertEqual(payload['signals'][0]['signal_id'], 'sig-2')
        self.assertIn('feed_version', payload)
        self.assertIn('generated_at', payload)
        self.assertTrue(payload['latest_signal_activity_at'])


if __name__ == '__main__':
    unittest.main()
