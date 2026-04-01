import tests._bootstrap
import unittest
from datetime import datetime
from unittest.mock import patch

from app.services.admin_runtime_service import get_admin_runtime_health_matrix, list_recent_audit_events, list_recent_incidents


class _AuditCollection:
    def __init__(self, rows):
        self.rows = rows
        self.last_query = None
        self.last_sort = None
        self.last_limit = None

    def find(self, query, sort=None, limit=0):
        self.last_query = query
        self.last_sort = sort
        self.last_limit = limit
        return self.rows[:limit]




class _UsersCollection:
    def __init__(self, rows):
        self.rows = rows

    def find(self, *args, **kwargs):
        return list(self.rows)


class AdminRuntimeServiceTests(unittest.TestCase):

    def test_admin_overview_includes_user_mix_and_paid_tiers(self):
        users_rows = [
            {'user_id': 1, 'plan': 'free', 'trial_end': None, 'plan_end': None, 'subscription_status': 'free', 'banned': False},
            {'user_id': 2, 'plan': 'free', 'trial_end': datetime.max, 'plan_end': None, 'subscription_status': 'trial', 'banned': False},
            {'user_id': 3, 'plan': 'plus', 'trial_end': None, 'plan_end': datetime.max, 'subscription_status': 'active', 'banned': False},
            {'user_id': 4, 'plan': 'premium', 'trial_end': None, 'plan_end': datetime.max, 'subscription_status': 'active', 'banned': False},
            {'user_id': 5, 'plan': 'free', 'trial_end': None, 'plan_end': None, 'subscription_status': 'expired', 'banned': True},
        ]

        with patch('app.services.admin_runtime_service.users_collection', return_value=_UsersCollection(users_rows)), \
             patch('app.services.admin_runtime_service.signals_collection', return_value=type('C', (), {'count_documents': lambda self, q: 0})()), \
             patch('app.services.admin_runtime_service.payment_orders_collection', return_value=type('C', (), {'count_documents': lambda self, q: 0})()), \
             patch('app.services.admin_runtime_service.audit_logs_collection', return_value=type('C', (), {'count_documents': lambda self, q: 0})()), \
             patch('app.services.admin_runtime_service.is_payment_configuration_ready', return_value=True), \
             patch('app.services.admin_runtime_service.get_admin_runtime_health_matrix', return_value={'ok': True, 'overall_status': 'ok', 'generated_at': '2026-03-30T12:00:00', 'runtimes': {}}):
            from app.services.admin_runtime_service import get_admin_operational_overview
            payload = get_admin_operational_overview()

        self.assertEqual(payload['users']['total'], 5)
        self.assertEqual(payload['users']['banned'], 1)
        self.assertEqual(payload['users']['free'], 2)
        self.assertEqual(payload['users']['trialing'], 1)
        self.assertEqual(payload['users']['plus_active'], 1)
        self.assertEqual(payload['users']['premium_active'], 1)
        self.assertEqual(payload['users']['active_paid'], 2)
        self.assertEqual(payload['users']['current_mix']['free'], 2)

    def test_runtime_health_matrix_becomes_error_if_any_runtime_errors(self):
        def _build(role):
            status = 'error' if role == 'scheduler' else 'ok'
            return {'overall_status': status, 'ok': status == 'ok', 'runtime_role': role}

        with patch('app.services.admin_runtime_service.build_runtime_health_report', side_effect=_build):
            payload = get_admin_runtime_health_matrix()

        self.assertFalse(payload['ok'])
        self.assertEqual(payload['overall_status'], 'error')
        self.assertEqual(set(payload['runtimes'].keys()), {'web', 'bot', 'signal_worker', 'scheduler'})


    def test_list_recent_incidents_combines_runtime_and_audit(self):
        runtime_payload = {
            'overall_status': 'degraded',
            'generated_at': '2026-03-30T12:00:10',
            'runtimes': {
                'web': {
                    'components': {
                        'miniapp': {
                            'status': 'ok',
                            'effective_status': 'stale',
                            'updated_at': '2026-03-30T12:00:00',
                            'age_seconds': 181,
                            'stale_after_seconds': 180,
                            'details': {'stage': 'running'},
                        }
                    }
                }
            },
        }
        audit_payload = {
            'items': [
                {
                    'created_at': '2026-03-30T12:00:20',
                    'event_type': 'signal_dispatch_failed',
                    'status': 'error',
                    'module': 'signal_pipeline',
                    'message': 'boom',
                    'metadata': {'job_id': '123'},
                },
                {
                    'created_at': '2026-03-30T12:00:05',
                    'event_type': 'miniapp_auth_succeeded',
                    'status': 'ok',
                    'module': 'miniapp',
                    'message': 'ok',
                    'metadata': {},
                },
            ]
        }

        with patch('app.services.admin_runtime_service.get_admin_runtime_health_matrix', return_value=runtime_payload), \
             patch('app.services.admin_runtime_service.list_recent_audit_events', return_value=audit_payload):
            payload = list_recent_incidents(limit=10)

        self.assertEqual(payload['runtime_overall_status'], 'degraded')
        self.assertEqual(payload['counts']['error'], 1)
        self.assertEqual(payload['counts']['warning'], 1)
        self.assertEqual(payload['items'][0]['source'], 'audit')
        self.assertEqual(payload['items'][1]['source'], 'runtime_health')

    def test_list_recent_audit_events_serializes_and_clamps_limit(self):
        rows = [{
            'created_at': '2026-03-30T12:00:00',
            'event_type': 'miniapp_auth_failed',
            'status': 'error',
            'module': 'miniapp',
            'user_id': 77,
            'message': 'boom',
            'metadata': {'k': 'v'},
        }]
        collection = _AuditCollection(rows)
        with patch('app.services.admin_runtime_service.audit_logs_collection', return_value=collection):
            payload = list_recent_audit_events(limit=250, status='error', module='miniapp')

        self.assertEqual(payload['limit'], 100)
        self.assertEqual(payload['filters']['status'], 'error')
        self.assertEqual(payload['filters']['module'], 'miniapp')
        self.assertEqual(collection.last_query, {'status': 'error', 'module': 'miniapp'})
        self.assertEqual(collection.last_limit, 100)
        self.assertEqual(payload['items'][0]['event_type'], 'miniapp_auth_failed')


if __name__ == '__main__':
    unittest.main()
