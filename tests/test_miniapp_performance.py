import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppPerformanceEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_performance_endpoint_returns_payload(self):
        payload = {
            'overview': {'focus_days': 30, 'focus_label': '30D', 'windows': [{'days': 7, 'label': '7D'}, {'days': 30, 'label': '30D'}]},
            'focus': {'days': 30, 'label': '30D', 'summary': {'resolved': 12}, 'activity': {'signals_total': 20}},
            'windows': [],
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_performance_center_payload', return_value=payload) as mocked_builder:
            with self._build_client() as client:
                response = client.get('/api/miniapp/performance?days=30', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['overview']['focus_days'], 30)
        mocked_builder.assert_called_once()
        _, kwargs = mocked_builder.call_args
        self.assertEqual(kwargs['focus_days'], 30)

    def test_performance_endpoint_rejects_unsupported_window(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}):
            with self._build_client() as client:
                response = client.get('/api/miniapp/performance?days=15', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'unsupported_performance_window')


if __name__ == '__main__':
    unittest.main()
