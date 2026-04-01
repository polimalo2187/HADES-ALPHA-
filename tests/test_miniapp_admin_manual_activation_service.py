import tests._bootstrap
import unittest
from datetime import timedelta
from unittest.mock import patch

from app.miniapp.service import build_admin_manual_plan_lookup_payload, apply_admin_manual_plan_activation
from app.models import utcnow


class MiniAppAdminManualActivationServiceTests(unittest.TestCase):
    def test_lookup_payload_enables_free_only_for_expired_free_user(self):
        expired_free_user = {
            'user_id': 123,
            'username': 'alpha',
            'language': 'es',
            'plan': 'free',
            'trial_end': utcnow() - timedelta(days=1),
            'plan_end': None,
            'banned': False,
        }
        with patch('app.miniapp.service.get_user_by_id', return_value=expired_free_user), \
             patch('app.miniapp.service.is_admin', return_value=False):
            payload = build_admin_manual_plan_lookup_payload(123)

        self.assertEqual(payload['target']['user_id'], 123)
        options = {item['key']: item for item in payload['plan_options']}
        self.assertTrue(options['free']['available'])
        self.assertTrue(options['plus']['available'])
        self.assertTrue(options['premium']['available'])

    def test_apply_manual_activation_rejects_free_for_active_trial(self):
        active_trial_user = {
            'user_id': 123,
            'username': 'alpha',
            'language': 'es',
            'plan': 'free',
            'trial_end': utcnow() + timedelta(days=3),
            'plan_end': None,
            'banned': False,
        }
        with patch('app.miniapp.service.get_user_by_id', return_value=active_trial_user):
            with self.assertRaisesRegex(ValueError, 'free_manual_requires_expired_free'):
                apply_admin_manual_plan_activation(admin_user_id=999, target_user_id=123, plan='free', days=2)

    def test_apply_manual_activation_plus_returns_updated_payload(self):
        before = {
            'user_id': 123,
            'username': 'alpha',
            'language': 'es',
            'plan': 'free',
            'trial_end': utcnow() - timedelta(days=1),
            'plan_end': None,
            'banned': False,
        }
        after = {
            'user_id': 123,
            'username': 'alpha',
            'language': 'es',
            'plan': 'plus',
            'trial_end': None,
            'plan_end': utcnow() + timedelta(days=21),
            'banned': False,
        }
        with patch('app.miniapp.service.get_user_by_id', side_effect=[before, after]), \
             patch('app.miniapp.service.grant_plan_entitlement', return_value=True), \
             patch('app.miniapp.service.is_admin', return_value=False):
            result = apply_admin_manual_plan_activation(admin_user_id=999, target_user_id=123, plan='plus', days=21)

        self.assertTrue(result['ok'])
        self.assertEqual(result['activation']['plan'], 'plus')
        self.assertEqual(result['activation']['days'], 21)
        self.assertEqual(result['target']['plan'], 'plus')


if __name__ == '__main__':
    unittest.main()
