import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppLiveSignalsEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_live_signals_endpoint_returns_payload_when_version_is_missing(self):
        meta = {
            'active_signals_count': 2,
            'feed_version': '2|2026-01-01T00:00:00',
            'generated_at': '2026-01-01T00:00:01',
        }
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
             patch('app.miniapp.app.build_live_signals_feed_meta', return_value=meta) as mocked_meta, \
             patch('app.miniapp.app.build_live_signals_payload', return_value=payload) as mocked_build:
            with self._build_client() as client:
                response = client.get('/api/miniapp/live-signals', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['active_signals_count'], 2)
        self.assertEqual(body['signals'][1]['symbol'], 'ETHUSDT')
        mocked_meta.assert_called_once()
        mocked_build.assert_called_once()
        args, kwargs = mocked_build.call_args
        self.assertEqual(args[0]['user_id'], 10)
        self.assertEqual(kwargs['meta'], meta)

    def test_live_signals_endpoint_returns_204_when_version_matches(self):
        meta = {
            'active_signals_count': 2,
            'feed_version': '2|2026-01-01T00:00:00',
            'generated_at': '2026-01-01T00:00:01',
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_live_signals_feed_meta', return_value=meta), \
             patch('app.miniapp.app.build_live_signals_payload') as mocked_build:
            with self._build_client() as client:
                response = client.get('/api/miniapp/live-signals?since_version=2|2026-01-01T00:00:00', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.headers.get('X-Live-Signals-Version'), meta['feed_version'])
        self.assertEqual(response.headers.get('X-Live-Signals-Generated-At'), meta['generated_at'])
        mocked_build.assert_not_called()


if __name__ == '__main__':
    unittest.main()
