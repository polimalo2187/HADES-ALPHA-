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
            'runtime': {'ok': True, 'overall_status': 'ok', 'runtimes': {'web': {'overall_status': 'ok', 'components': {}}}},
            'users': {'total': 10, 'banned': 1, 'active_paid': 4, 'free': 5, 'plus_active': 2, 'premium_active': 2, 'trialing': 1, 'expired_free': 3, 'current_mix': {'free': 5, 'plus': 2, 'premium': 2}},
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
        self.assertEqual(body['users']['plus_active'], 2)
        self.assertEqual(body['users']['premium_active'], 2)
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

    def test_admin_user_lookup_returns_payload_for_admin(self):
        payload = {
            'target': {'user_id': 777, 'plan': 'free', 'plan_name': 'FREE', 'subscription_status': 'free'},
            'plan_options': [{'key': 'free', 'available': True}],
            'rules': {'free_manual_summary': 'Free manual solo aplica a usuarios Free con el trial vencido.'},
        }
        with patch('app.miniapp.app.initialize_database'),              patch('app.miniapp.app.start_background_heartbeat'),              patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']),              patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False),              patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}),              patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}),              patch('app.miniapp.app.is_admin', return_value=True),              patch('app.miniapp.app.build_admin_user_lookup_payload', return_value=payload):
            with self._build_client() as client:
                response = client.get('/api/miniapp/admin/user-lookup?user_id=777', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['requested_by'], 999)
        self.assertEqual(body['target']['user_id'], 777)
        self.assertTrue(body['plan_options'][0]['available'])

    def test_admin_manual_plan_activation_returns_payload_for_admin(self):
        result = {
            'ok': True,
            'requested_by': 999,
            'activation': {'plan': 'plus', 'plan_name': 'PLUS', 'days': 21},
            'before': {'plan': 'free'},
            'target': {'user_id': 777, 'plan': 'plus', 'plan_name': 'PLUS', 'subscription_status': 'active'},
            'plan_options': [{'key': 'free', 'available': False}, {'key': 'plus', 'available': True}],
        }
        with patch('app.miniapp.app.initialize_database'),              patch('app.miniapp.app.start_background_heartbeat'),              patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']),              patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False),              patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}),              patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}),              patch('app.miniapp.app.is_admin', return_value=True),              patch('app.miniapp.app.apply_admin_manual_plan_activation', return_value=result),              patch('app.miniapp.app.record_audit_event') as mocked_audit:
            with self._build_client() as client:
                response = client.post('/api/miniapp/admin/manual-plan-activation', headers={'Authorization': 'Bearer token'}, json={'user_id': 777, 'plan': 'plus', 'days': 21})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['activation']['plan'], 'plus')
        self.assertEqual(body['target']['plan'], 'plus')
        self.assertTrue(any(call.kwargs.get('event_type') == 'miniapp_admin_manual_plan_activation' for call in mocked_audit.call_args_list))

    def test_admin_manual_plan_activation_returns_400_on_invalid_free_activation(self):
        with patch('app.miniapp.app.initialize_database'),              patch('app.miniapp.app.start_background_heartbeat'),              patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']),              patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False),              patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}),              patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}),              patch('app.miniapp.app.is_admin', return_value=True),              patch('app.miniapp.app.apply_admin_manual_plan_activation', side_effect=ValueError('free_manual_requires_expired_free')):
            with self._build_client() as client:
                response = client.post('/api/miniapp/admin/manual-plan-activation', headers={'Authorization': 'Bearer token'}, json={'user_id': 777, 'plan': 'free', 'days': 3})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'free_manual_requires_expired_free')
    def test_admin_user_moderation_requires_confirmation(self):
        with patch('app.miniapp.app.initialize_database'),              patch('app.miniapp.app.start_background_heartbeat'),              patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']),              patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False),              patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}),              patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}),              patch('app.miniapp.app.is_admin', return_value=True):
            with self._build_client() as client:
                response = client.post('/api/miniapp/admin/user-moderation', headers={'Authorization': 'Bearer token'}, json={'user_id': 123, 'action': 'ban_permanent', 'confirm': False})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'confirm_required')

    def test_admin_user_moderation_returns_payload_for_admin(self):
        result = {
            'ok': True,
            'requested_by': 999,
            'action': 'ban_temporary',
            'before': {'user_id': 123, 'banned': False},
            'target': {'user_id': 123, 'banned': True, 'ban_mode': 'temporary'},
            'action_summary': {'mode': 'temporary', 'duration_value': 7, 'duration_unit': 'days'},
            'moderation': {'can_unban': True},
        }
        with patch('app.miniapp.app.initialize_database'),              patch('app.miniapp.app.start_background_heartbeat'),              patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']),              patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False),              patch('app.miniapp.app.parse_session_token', return_value={'uid': 999}),              patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 999, 'banned': False}),              patch('app.miniapp.app.is_admin', return_value=True),              patch('app.miniapp.app.apply_admin_user_moderation_action', return_value=result),              patch('app.miniapp.app.record_audit_event') as mocked_audit:
            with self._build_client() as client:
                response = client.post('/api/miniapp/admin/user-moderation', headers={'Authorization': 'Bearer token'}, json={'user_id': 123, 'action': 'ban_temporary', 'duration_value': 7, 'duration_unit': 'days', 'confirm': True})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['action'], 'ban_temporary')
        self.assertTrue(body['target']['banned'])
        self.assertTrue(any(call.kwargs.get('event_type') == 'miniapp_admin_user_ban_temporary' for call in mocked_audit.call_args_list))


if __name__ == '__main__':
    unittest.main()
