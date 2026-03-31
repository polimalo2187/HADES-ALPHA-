import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app


class MiniAppAdminEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_admin_overview_requires_admin(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 123}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 123, 'banned': False}), \
             patch('app.miniapp.app.is_admin', return_value=False):
            with self._build_client() as client:
                response = client.get('/api/miniapp/admin/overview', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()['detail'], 'admin_required')

    def test_admin_overview_returns_payload_for_admin(self):
        payload = {
            'generated_at': '2026-03-30T12:00:00',
            'runtime': {'ok': True, 'overall_status': 'ok', 'runtimes': {}},
            'users': {'total': 10, 'banned': 1, 'active_paid': 4},
            'signals': {'created_last_24h': 8, 'pending_evaluation': 2},
            'payments': {'configuration_ready': True, 'pending_orders': 1, 'awaiting_confirmation': 0, 'paid_last_24h': 2},
            'audit': {'errors_last_24h': 0, 'warnings_last_24h': 1},
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}), \
             patch('app.miniapp.app.is_admin', return_value=True), \
             patch('app.miniapp.app.get_admin_operational_overview', return_value=payload):
            with self._build_client() as client:
                response = client.get('/api/miniapp/admin/overview', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['requested_by'], 999)
        self.assertEqual(body['users']['total'], 10)
        self.assertTrue(body['runtime']['ok'])


    def test_admin_incidents_returns_payload_for_admin(self):
        payload = {
            'items': [{'source': 'runtime_health', 'status': 'warning', 'message': 'runtime degraded'}],
            'limit': 25,
            'counts': {'error': 0, 'warning': 1},
            'runtime_overall_status': 'degraded',
            'generated_at': '2026-03-30T12:00:00',
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}), \
             patch('app.miniapp.app.is_admin', return_value=True), \
             patch('app.miniapp.app.list_recent_incidents', return_value=payload):
            with self._build_client() as client:
                response = client.get('/api/miniapp/admin/incidents', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['requested_by'], 999)
        self.assertEqual(body['counts']['warning'], 1)
        self.assertEqual(body['items'][0]['source'], 'runtime_health')

    def test_admin_audit_returns_400_for_invalid_status(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}), \
             patch('app.miniapp.app.is_admin', return_value=True), \
             patch('app.miniapp.app.list_recent_audit_events', side_effect=ValueError('status inválido')):
            with self._build_client() as client:
                response = client.get('/api/miniapp/admin/audit?status=bad', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'status inválido')




    def test_admin_reset_requires_confirmation(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}), \
             patch('app.miniapp.app.is_admin', return_value=True):
            with self._build_client() as client:
                response = client.post('/api/miniapp/admin/reset-results', headers={'Authorization': 'Bearer token'}, json={'confirm': False})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'confirm_required')

    def test_admin_reset_executes_and_returns_summary(self):
        summary = {
            'mode': 'full_reset',
            'deleted_base_signals': 12,
            'deleted_user_signals': 14,
            'deleted_results': 10,
            'deleted_history': 8,
            'deleted_snapshots': 2,
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}), \
             patch('app.miniapp.app.is_admin', return_value=True), \
             patch('app.miniapp.app.reset_statistics', return_value=summary), \
             patch('app.miniapp.app.record_audit_event') as mocked_audit:
            with self._build_client() as client:
                response = client.post('/api/miniapp/admin/reset-results', headers={'Authorization': 'Bearer token'}, json={'confirm': True})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['requested_by'], 999)
        self.assertEqual(body['summary']['deleted_results'], 10)
        self.assertTrue(any(call.kwargs.get('event_type') == 'miniapp_admin_results_reset' for call in mocked_audit.call_args_list))


if __name__ == '__main__':
    unittest.main()
