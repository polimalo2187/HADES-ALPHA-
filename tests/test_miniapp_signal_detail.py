import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppSignalDetailEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_signal_detail_returns_payload(self):
        payload = {
            'signal': {'signal_id': 'sig-1', 'symbol': 'BTCUSDT', 'direction': 'LONG'},
            'viewer_plan': 'plus',
            'tracking_tier': 'full',
            'selected_profile': 'moderado',
            'profile_options': ['conservador', 'moderado', 'agresivo'],
            'tracking': {'state_label': 'ACTIVA'},
            'analysis': {'normalized_score': 82.5},
            'upgrade_hint': 'Premium añade desglose interno del scoring y componentes avanzados.',
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_signal_detail_payload', return_value=payload):
            with self._build_client() as client:
                response = client.get('/api/miniapp/signals/sig-1?profile=moderado', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['signal']['symbol'], 'BTCUSDT')
        self.assertEqual(body['tracking_tier'], 'full')
        self.assertEqual(body['analysis']['normalized_score'], 82.5)

    def test_signal_detail_returns_404_when_missing(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'free'}), \
             patch('app.miniapp.app.build_signal_detail_payload', return_value=None):
            with self._build_client() as client:
                response = client.get('/api/miniapp/signals/missing', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['detail'], 'signal_not_found')


if __name__ == '__main__':
    unittest.main()
