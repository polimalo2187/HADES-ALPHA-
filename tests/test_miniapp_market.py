import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppMarketEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_market_endpoint_returns_enriched_payload(self):
        payload = {
            'bias': 'Alcista',
            'preferred_side': 'LONGS',
            'radar_summary': {'total': 1, 'longs': 1, 'shorts': 0, 'hot': 1, 'immediate': 1, 'active_signals': 0},
            'radar': [{'symbol': 'BTCUSDT', 'direction': 'LONG', 'final_score': 90.0}],
            'top_gainers': [],
            'top_losers': [],
            'top_volume': [],
            'top_open_interest': [],
            'market_rotation': {'gainers': [{'symbol': 'BTCUSDT'}], 'losers': [], 'volume': []},
            'btc': {},
            'eth': {},
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_market_payload', return_value=payload) as mocked_build:
            with self._build_client() as client:
                response = client.get('/api/miniapp/market', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['radar_summary']['hot'], 1)
        self.assertEqual(body['radar'][0]['symbol'], 'BTCUSDT')
        self.assertEqual(body['market_rotation']['gainers'][0]['symbol'], 'BTCUSDT')
        mocked_build.assert_called_once()
        args, _ = mocked_build.call_args
        self.assertEqual(args[0]['user_id'], 10)


    def test_radar_detail_endpoint_returns_tactical_payload(self):
        payload = {
            'symbol': 'BTCUSDT',
            'radar': {'symbol': 'BTCUSDT', 'direction': 'LONG', 'final_score': 91.0},
            'scanner': {'status': 'confirmed'},
            'signal_context': {'has_active_signal': True, 'signal_detail_available': True},
            'tactical_checks': ['ok'],
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_radar_symbol_payload', return_value=payload) as mocked_build:
            with self._build_client() as client:
                response = client.get('/api/miniapp/radar/BTCUSDT', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['symbol'], 'BTCUSDT')
        self.assertEqual(body['scanner']['status'], 'confirmed')
        mocked_build.assert_called_once()
        args, _ = mocked_build.call_args
        self.assertEqual(args[0]['user_id'], 10)
        self.assertEqual(args[1], 'BTCUSDT')

    def test_radar_detail_endpoint_returns_404_when_symbol_missing(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_radar_symbol_payload', return_value=None):
            with self._build_client() as client:
                response = client.get('/api/miniapp/radar/FAKEUSDT', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['detail'], 'radar_symbol_not_found')


if __name__ == '__main__':
    unittest.main()
