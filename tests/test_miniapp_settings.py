import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppSettingsEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_settings_endpoint_returns_payload(self):
        payload = {
            'overview': {'user_id': 10, 'plan_name': 'PLUS'},
            'language': {'current': 'es', 'options': [{'value': 'es', 'label': 'Español'}]},
            'push_alerts': {'enabled': True, 'selected_tiers': ['free', 'plus']},
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_settings_center_payload', return_value=payload) as mocked_builder:
            with self._build_client() as client:
                response = client.get('/api/miniapp/settings', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['language']['current'], 'es')
        self.assertEqual(body['push_alerts']['selected_tiers'], ['free', 'plus'])
        mocked_builder.assert_called_once()

    def test_settings_update_endpoint_saves_payload(self):
        saved = {
            'overview': {'user_id': 10, 'plan_name': 'PREMIUM'},
            'language': {'current': 'en', 'options': [{'value': 'en', 'label': 'English'}]},
            'push_alerts': {'enabled': False, 'selected_tiers': []},
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'premium'}), \
             patch('app.miniapp.app.save_settings_center_payload', return_value=saved) as mocked_save:
            with self._build_client() as client:
                response = client.post(
                    '/api/miniapp/settings',
                    headers={'Authorization': 'Bearer token'},
                    json={
                        'language': 'en',
                        'push_alerts_enabled': False,
                        'push_tiers': {'free': False, 'plus': False, 'premium': False},
                    },
                )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['language']['current'], 'en')
        mocked_save.assert_called_once()
        args, _ = mocked_save.call_args
        self.assertEqual(args[0], 10)
        self.assertEqual(args[1]['language'], 'en')
        self.assertFalse(args[1]['push_alerts_enabled'])


if __name__ == '__main__':
    unittest.main()
