import tests._bootstrap
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

telegram_module = types.ModuleType('telegram')
telegram_module.Bot = object
sys.modules.setdefault('telegram', telegram_module)

from app.notifier import _eligible_users_for_alert


class NotifierPreferencesTests(unittest.TestCase):
    def test_eligible_users_for_alert_respects_selected_push_tiers(self):
        users = MagicMock()
        users.find.return_value = [
            {
                'user_id': 1,
                'plan': 'free',
                'banned': False,
                'miniapp_settings': {'push_alerts': {'enabled': True, 'tiers': {'free': True}}},
            },
            {
                'user_id': 2,
                'plan': 'plus',
                'banned': False,
                'miniapp_settings': {'push_alerts': {'enabled': True, 'tiers': {'free': False, 'plus': True}}},
            },
            {
                'user_id': 3,
                'plan': 'premium',
                'banned': False,
                'miniapp_settings': {'push_alerts': {'enabled': False, 'tiers': {'free': True, 'plus': True, 'premium': True}}},
            },
        ]
        with patch('app.notifier.users_collection', return_value=users), \
             patch('app.notifier.is_plan_active', return_value=True), \
             patch('app.notifier.is_trial_active', return_value=False), \
             patch('app.notifier.is_admin', return_value=False), \
             patch('app.notifier.plan_status', side_effect=lambda user: {'plan': user.get('plan')}):
            free_recipients = _eligible_users_for_alert('free')
            plus_recipients = _eligible_users_for_alert('plus')

        self.assertEqual(free_recipients, [1])
        self.assertEqual(plus_recipients, [2])


    def test_admin_receives_all_visible_tiers(self):
        users = MagicMock()
        users.find.return_value = [
            {
                'user_id': 99,
                'plan': 'premium',
                'banned': False,
                'miniapp_settings': {'push_alerts': {'enabled': True, 'tiers': {'free': True, 'plus': True, 'premium': True}}},
            },
        ]
        with patch('app.notifier.users_collection', return_value=users), \
             patch('app.notifier.is_plan_active', return_value=True), \
             patch('app.notifier.is_trial_active', return_value=False), \
             patch('app.notifier.is_admin', return_value=True), \
             patch('app.notifier.plan_status', side_effect=lambda user: {'plan': user.get('plan')}):
            free_recipients = _eligible_users_for_alert('free')
            plus_recipients = _eligible_users_for_alert('plus')
            premium_recipients = _eligible_users_for_alert('premium')

        self.assertEqual(free_recipients, [99])
        self.assertEqual(plus_recipients, [99])
        self.assertEqual(premium_recipients, [99])


if __name__ == '__main__':
    unittest.main()
