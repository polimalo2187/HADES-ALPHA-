import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_bootstrap_payload


class MiniAppBootstrapServiceTests(unittest.TestCase):
    def test_bootstrap_payload_preserves_complete_data_contract(self):
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
        dashboard_payload = {
            'summary_7d': {'resolved': 3},
            'summary_30d': {'resolved': 8},
            'recent_signals': [{'signal_id': 'sig-1'}],
            'recent_history': [{'signal_id': 'hist-1'}],
            'active_signals_count': 1,
            'watchlist_count': 2,
            'signal_mix': {'free': 1, 'plus': 1, 'premium': 0},
            'active_mix': {'free': 1, 'plus': 0, 'premium': 0},
            'active_payment_order': None,
        }
        history_payload = [{'signal_id': 'hist-1'}]
        signals_payload = [{'signal_id': 'sig-1'}]
        market_payload = {'radar': [{'symbol': 'BTCUSDT'}], 'radar_summary': {'total': 1}, 'radar_context': {'bias': 'bullish'}}
        watchlist_payload = [{'symbol': 'BTCUSDT'}]
        watchlist_context = {
            'items': watchlist_payload,
            'meta': {
                'symbols': ['BTCUSDT'],
                'symbols_count': 1,
                'max_symbols': 15,
                'slots_left': 14,
                'plan': 'plus',
                'plan_name': 'PLUS',
                'can_add_more': True,
            },
        }
        plans_payload = {'plus': [{'days': 30}], 'premium': [{'days': 30}]}
        account_payload = {'overview': {'user_id': 99}, 'billing': {'summary': {'total': 0}}}

        with patch('app.miniapp.service.build_me_payload', return_value=me_payload), \
             patch('app.miniapp.service.get_watchlist_limit_for_plan', return_value=15), \
             patch('app.miniapp.service.get_payment_configuration_status', return_value={'ready': True, 'checks': [], 'missing_keys': []}), \
             patch('app.miniapp.service.get_bot_username', return_value='NeoTrade_bot'), \
             patch('app.miniapp.service.build_dashboard_payload', return_value=dashboard_payload), \
             patch('app.miniapp.service.build_signals_payload', return_value=signals_payload), \
             patch('app.miniapp.service.build_history_payload', return_value=history_payload), \
             patch('app.miniapp.service.build_market_payload', return_value=market_payload), \
             patch('app.miniapp.service.build_watchlist_payload', return_value=watchlist_payload), \
             patch('app.miniapp.service.build_watchlist_context', return_value=watchlist_context), \
             patch('app.miniapp.service.build_plans_payload', return_value=plans_payload), \
             patch('app.miniapp.service.build_account_center_payload', return_value=account_payload):
            payload = build_bootstrap_payload(user)

        self.assertEqual(payload['me']['user_id'], 99)
        self.assertEqual(payload['dashboard']['active_signals_count'], 1)
        self.assertEqual(payload['signals'][0]['signal_id'], 'sig-1')
        self.assertEqual(payload['history'][0]['signal_id'], 'hist-1')
        self.assertEqual(payload['market']['radar'][0]['symbol'], 'BTCUSDT')
        self.assertEqual(payload['watchlist_meta']['max_symbols'], 15)
        self.assertEqual(payload['account']['overview']['user_id'], 99)
        self.assertEqual(payload['plans']['plus'][0]['days'], 30)
        self.assertEqual(payload['bot_username'], 'NeoTrade_bot')
        self.assertTrue(payload['payment_config_ready'])


if __name__ == '__main__':
    unittest.main()
