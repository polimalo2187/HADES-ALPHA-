import tests._bootstrap
import unittest
from unittest.mock import MagicMock, patch

from app.miniapp.service import build_settings_center_payload, save_settings_center_payload


class MiniAppSettingsServiceTests(unittest.TestCase):
    def test_plus_plan_exposes_free_and_plus_alert_toggles(self):
        user = {
            'user_id': 10,
            'plan': 'plus',
            'language': 'es',
            'miniapp_settings': {
                'push_alerts': {
                    'enabled': True,
                    'tiers': {'free': True, 'plus': True, 'premium': True},
                },
            },
        }
        with patch('app.miniapp.service.plan_status', return_value={'plan': 'plus'}):
            payload = build_settings_center_payload(user)

        tiers = {item['key']: item for item in payload['push_alerts']['tiers']}
        self.assertTrue(tiers['free']['available'])
        self.assertTrue(tiers['plus']['available'])
        self.assertFalse(tiers['premium']['available'])
        self.assertFalse(tiers['premium']['selected'])

    def test_save_settings_normalizes_language_and_disables_unavailable_tiers(self):
        user_doc = {
            'user_id': 11,
            'plan': 'plus',
            'language': 'es',
            'miniapp_settings': {},
        }
        users = MagicMock()
        users.find_one.side_effect = [user_doc, {**user_doc, 'language': 'en', 'miniapp_settings': {'push_alerts': {'enabled': True, 'tiers': {'free': True, 'plus': False, 'premium': False}}}}]
        with patch('app.miniapp.service.users_collection', return_value=users), \
             patch('app.miniapp.service.plan_status', return_value={'plan': 'plus'}):
            payload = save_settings_center_payload(11, {
                'language': 'EN',
                'push_alerts_enabled': True,
                'push_tiers': {'free': True, 'plus': False, 'premium': True},
            })

        users.update_one.assert_called_once()
        _, update_doc = users.update_one.call_args[0]
        updates = update_doc['$set']
        self.assertEqual(updates['language'], 'en')
        self.assertTrue(updates['miniapp_settings.push_alerts.tiers.free'])
        self.assertFalse(updates['miniapp_settings.push_alerts.tiers.plus'])
        self.assertFalse(updates['miniapp_settings.push_alerts.tiers.premium'])
        self.assertEqual(payload['language']['current'], 'en')


if __name__ == '__main__':
    unittest.main()
