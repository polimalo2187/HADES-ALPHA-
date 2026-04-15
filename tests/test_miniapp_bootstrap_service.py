import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_bootstrap_payload


class MiniAppBootstrapServiceTests(unittest.TestCase):
    def test_bootstrap_payload_is_lightweight_and_skips_heavy_builders(self):
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
             patch('app.miniapp.service.get_bot_username', return_value='NeoTrade_bot'), \
             patch('app.miniapp.service.build_dashboard_payload') as dashboard_mock, \
             patch('app.miniapp.service.build_signals_payload') as signals_mock, \
             patch('app.miniapp.service.build_history_payload') as history_mock, \
             patch('app.miniapp.service.build_market_payload') as market_mock, \
             patch('app.miniapp.service.build_watchlist_context') as watchlist_context_mock, \
             patch('app.miniapp.service.build_watchlist_payload') as watchlist_mock, \
             patch('app.miniapp.service.build_plans_payload') as plans_mock, \
             patch('app.miniapp.service.build_account_center_payload') as account_mock:
            payload = build_bootstrap_payload(user)

        dashboard_mock.assert_not_called()
        signals_mock.assert_not_called()
        history_mock.assert_not_called()
        market_mock.assert_not_called()
        watchlist_context_mock.assert_not_called()
        watchlist_mock.assert_not_called()
        plans_mock.assert_not_called()
        account_mock.assert_not_called()

        self.assertEqual(payload['bootstrap_mode'], 'light')
        self.assertEqual(payload['me']['user_id'], 99)
        self.assertEqual(payload['watchlist_meta']['max_symbols'], 15)
        self.assertEqual(payload['bot_username'], 'NeoTrade_bot')
        self.assertEqual(payload['signals'], [])
        self.assertEqual(payload['history'], [])
        self.assertEqual(payload['plans'], {'plus': [], 'premium': []})
        self.assertEqual(payload['account'], {})


if __name__ == '__main__':
    unittest.main()
