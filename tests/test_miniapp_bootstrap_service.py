import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_bootstrap_payload


class MiniAppBootstrapServiceTests(unittest.TestCase):
    def test_bootstrap_payload_is_lightweight_and_skips_heavy_sections(self):
        user = {
            'user_id': 99,
            'username': 'neo',
            'language': 'es',
            'plan': 'plus',
            'subscription_status': 'active',
        }
        me_payload = {
            'user_id': 99,
            'username': 'neo',
            'language': 'es',
            'plan': 'plus',
            'plan_name': 'PLUS',
            'subscription_status': 'active',
            'subscription_status_label': 'Activo',
            'days_left': 11,
            'expires_at': None,
            'is_admin': False,
        }

        with patch('app.miniapp.service.build_me_payload', return_value=me_payload), \
             patch('app.miniapp.service.get_watchlist_limit_for_plan', return_value=15), \
             patch('app.miniapp.service.get_payment_configuration_status', return_value={'ready': True, 'checks': [], 'missing_keys': []}), \
             patch('app.miniapp.service.get_bot_username', return_value='NeoTrade_bot'), \
             patch('app.miniapp.service.build_dashboard_payload', side_effect=AssertionError('heavy dashboard must not run')), \
             patch('app.miniapp.service.build_signals_payload', side_effect=AssertionError('signals must not run in bootstrap')), \
             patch('app.miniapp.service.build_history_payload', side_effect=AssertionError('history must not run in bootstrap')), \
             patch('app.miniapp.service.build_market_payload', side_effect=AssertionError('market must not run in bootstrap')), \
             patch('app.miniapp.service.build_watchlist_payload', side_effect=AssertionError('watchlist must not run in bootstrap')), \
             patch('app.miniapp.service.build_watchlist_context', side_effect=AssertionError('watchlist context must not run in bootstrap')), \
             patch('app.miniapp.service.build_plans_payload', side_effect=AssertionError('plans must not run in bootstrap')), \
             patch('app.miniapp.service.build_account_center_payload', side_effect=AssertionError('account must not run in bootstrap')):
            payload = build_bootstrap_payload(user)

        self.assertEqual(payload['bootstrap_mode'], 'light')
        self.assertEqual(payload['me']['user_id'], 99)
        self.assertEqual(payload['dashboard']['active_signals_count'], 0)
        self.assertEqual(payload['signals'], [])
        self.assertEqual(payload['history'], [])
        self.assertEqual(payload['market']['radar'], [])
        self.assertEqual(payload['watchlist_meta']['max_symbols'], 15)
        self.assertEqual(payload['bot_username'], 'NeoTrade_bot')
        self.assertTrue(payload['payment_config_ready'])


if __name__ == '__main__':
    unittest.main()
