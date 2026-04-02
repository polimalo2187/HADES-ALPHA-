import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppBootstrapEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_bootstrap_endpoint_returns_light_payload(self):
        payload = {
            'bootstrap_mode': 'light',
            'me': {'user_id': 7, 'plan': 'premium', 'plan_name': 'PREMIUM', 'days_left': 23},
            'dashboard': {'active_signals_count': 0, 'summary_7d': {}, 'summary_30d': {}},
            'signals': [],
            'history': [],
            'market': {'radar': [], 'top_gainers': [], 'top_losers': [], 'top_volume': [], 'radar_summary': {'total': 0}, 'radar_context': {'bias': 'neutral', 'regime': 'neutral', 'environment': '—', 'recommendation': 'Cargando lectura de mercado...'}},
            'watchlist': [],
            'watchlist_meta': {'symbols': [], 'symbols_count': 0, 'max_symbols': 20, 'slots_left': 20, 'can_add_more': True},
            'plans': {'plus': [], 'premium': []},
            'account': {},
            'bot_username': 'NeoTrade_bot',
            'support_url': '#',
            'payment_config_status': {'ready': True, 'checks': [], 'missing_keys': []},
            'payment_config_ready': True,
            'generated_at': '2026-01-01T00:00:00',
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 7}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 7, 'banned': False, 'plan': 'premium'}), \
             patch('app.miniapp.app.build_bootstrap_payload', return_value=payload):
            with self._build_client() as client:
                response = client.get('/api/miniapp/bootstrap', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['bootstrap_mode'], 'light')
        self.assertEqual(body['me']['user_id'], 7)
        self.assertEqual(body['signals'], [])
        self.assertEqual(body['watchlist_meta']['max_symbols'], 20)


if __name__ == '__main__':
    unittest.main()
