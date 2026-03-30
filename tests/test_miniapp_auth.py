import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppAuthEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_dev_auth_is_rejected_when_disabled(self):
        with patch('app.miniapp.app.initialize_database'),              patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']),              patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False),              patch('app.miniapp.app.get_runtime_role', return_value='web'):
            with self._build_client() as client:
                response = client.post('/api/miniapp/auth', json={'dev_user_id': 999})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()['detail'], 'autenticación dev deshabilitada')

    def test_dev_auth_rejects_unconfigured_user_id_mismatch(self):
        fake_user = {'user_id': 123, 'username': 'dev_123', 'language': 'es', 'plan': 'free'}
        with patch('app.miniapp.app.initialize_database'),              patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']),              patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=True),              patch('app.miniapp.app.get_mini_app_dev_user_id', return_value=123),              patch('app.miniapp.app.get_runtime_role', return_value='web'),              patch('app.miniapp.app.ensure_mini_app_user', return_value=fake_user),              patch('app.miniapp.app.build_me_payload', return_value={'user_id': 123}):
            with self._build_client() as client:
                bad = client.post('/api/miniapp/auth', json={'dev_user_id': 999})
                ok = client.post('/api/miniapp/auth', json={'dev_user_id': 123})

        self.assertEqual(bad.status_code, 401)
        self.assertEqual(bad.json()['detail'], 'dev_user_id no autorizado')
        self.assertEqual(ok.status_code, 200)
        payload = ok.json()
        self.assertTrue(payload['ok'])
        self.assertIn('session_token', payload)
        self.assertEqual(payload['me']['user_id'], 123)


if __name__ == '__main__':
    unittest.main()
