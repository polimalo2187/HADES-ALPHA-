import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppResilienceTests(unittest.TestCase):
    def _build_client(self, *, raise_server_exceptions=True):
        app = create_mini_app()
        return TestClient(app, raise_server_exceptions=raise_server_exceptions)

    def test_request_id_header_is_echoed(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.record_audit_event'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False):
            with self._build_client() as client:
                response = client.get('/miniapp/health/live', headers={'X-Request-ID': 'req-123'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get('X-Request-ID'), 'req-123')

    def test_unhandled_exception_returns_json_with_request_id(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.record_audit_event') as audit_mock, \
             patch('app.miniapp.app.heartbeat') as heartbeat_mock, \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False):
            app = create_mini_app()

            @app.get('/boom-test')
            async def boom_test():
                raise RuntimeError('boom')

            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.get('/boom-test', headers={'X-Request-ID': 'req-boom'})

        self.assertEqual(response.status_code, 500)
        body = response.json()
        self.assertFalse(body['ok'])
        self.assertEqual(body['detail'], 'internal_server_error')
        self.assertEqual(body['request_id'], 'req-boom')
        heartbeat_mock.assert_called()
        audit_mock.assert_called()


if __name__ == '__main__':
    unittest.main()
