import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppWatchlistEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_watchlist_get_returns_items_and_meta(self):
        payload = {
            "items": [{"symbol": "BTCUSDT"}],
            "meta": {"symbols": ["BTCUSDT"], "symbols_count": 1, "max_symbols": 2, "slots_left": 1, "plan": "free", "plan_name": "FREE", "can_add_more": True},
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'free'}), \
             patch('app.miniapp.app.build_watchlist_context', return_value=payload):
            with self._build_client() as client:
                response = client.get('/api/miniapp/watchlist', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['items'][0]['symbol'], 'BTCUSDT')
        self.assertEqual(body['meta']['symbols_count'], 1)

    def test_watchlist_add_returns_updated_context(self):
        payload = {
            "items": [{"symbol": "BTCUSDT"}],
            "meta": {"symbols": ["BTCUSDT"], "symbols_count": 1, "max_symbols": 2, "slots_left": 1, "plan": "free", "plan_name": "FREE", "can_add_more": True},
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', side_effect=[{'user_id': 10, 'banned': False, 'plan': 'free'}, {'user_id': 10, 'banned': False, 'plan': 'free'}]), \
             patch('app.miniapp.app.add_symbol', return_value=(True, 'ok')), \
             patch('app.miniapp.app.record_audit_event'), \
             patch('app.miniapp.app.build_watchlist_context', return_value=payload):
            with self._build_client() as client:
                response = client.post('/api/miniapp/watchlist/add', json={'symbol': 'BTC'}, headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['meta']['symbols'][0], 'BTCUSDT')

    def test_watchlist_replace_limit_violation_returns_400(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'free'}), \
             patch('app.miniapp.app.set_symbols', return_value=(False, '🔒 Tu plan permite hasta 2 símbolos en Watchlist.')), \
             patch('app.miniapp.app.record_audit_event'):
            with self._build_client() as client:
                response = client.post('/api/miniapp/watchlist/replace', json={'symbols': ['BTC', 'ETH', 'SOL']}, headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 400)
        self.assertIn('permite hasta 2 símbolos', response.json()['detail'])


if __name__ == '__main__':
    unittest.main()
