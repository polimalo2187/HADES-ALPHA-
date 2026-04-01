import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppLiveSignalsEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_live_signals_endpoint_returns_lightweight_payload(self):
        payload = {
            'active_signals_count': 2,
            'recent_signals': [{'signal_id': 'sig-1', 'symbol': 'BTCUSDT'}],
            'signals': [{'signal_id': 'sig-1', 'symbol': 'BTCUSDT'}, {'signal_id': 'sig-2', 'symbol': 'ETHUSDT'}],
            'feed_version': '2|2026-01-01T00:00:00',
            'generated_at': '2026-01-01T00:00:01',
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_live_signals_payload', return_value=payload) as mocked_build:
            with self._build_client() as client:
                response = client.get('/api/miniapp/live-signals', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['active_signals_count'], 2)
        self.assertEqual(body['signals'][1]['symbol'], 'ETHUSDT')
        mocked_build.assert_called_once()
        args, _ = mocked_build.call_args
        self.assertEqual(args[0]['user_id'], 10)


if __name__ == '__main__':
    unittest.main()
