import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppHealthEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_health_ready_returns_200_when_runtime_report_is_ok(self):
        report = {
            'ok': True,
            'overall_status': 'ok',
            'runtime_role': 'web',
            'components': {},
            'required_components': ['miniapp', 'database'],
            'missing_required_components': [],
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.get_runtime_role', return_value='web'), \
             patch('app.miniapp.app.build_runtime_health_report', return_value=report):
            with self._build_client() as client:
                response = client.get('/miniapp/health/ready')
                summary = client.get('/miniapp/health')

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['ok'])
        self.assertEqual(summary.status_code, 200)
        self.assertEqual(summary.json()['service'], 'miniapp')

    def test_health_ready_returns_503_when_runtime_report_is_not_ready(self):
        report = {
            'ok': False,
            'overall_status': 'degraded',
            'runtime_role': 'web',
            'components': {'database': {'effective_status': 'missing'}},
            'required_components': ['miniapp', 'database'],
            'missing_required_components': ['database'],
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.get_runtime_role', return_value='web'), \
             patch('app.miniapp.app.build_runtime_health_report', return_value=report):
            with self._build_client() as client:
                response = client.get('/miniapp/health/ready')

        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()['ok'])


if __name__ == '__main__':
    unittest.main()
