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
        mocked_build.assert_called_once()
        args, _ = mocked_build.call_args
        self.assertEqual(args[0]['user_id'], 10)


if __name__ == '__main__':
    unittest.main()
