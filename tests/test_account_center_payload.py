import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_account_center_payload, build_bootstrap_payload


class AccountCenterPayloadTests(unittest.TestCase):
    def test_build_account_center_payload_includes_subscription_billing_and_referrals(self):
        user = {
            'user_id': 42,
            'username': 'jarold',
            'language': 'es',
            'plan': 'premium',
            'subscription_status': 'active',
            'plan_end': None,
            'trial_end': None,
            'ref_code': 'ref_42',
            'valid_referrals_total': 3,
            'reward_days_total': 25,
            'last_purchase_plan': 'premium',
            'last_purchase_days': 30,
            'last_entitlement_source': 'payment_bep20',
        }
        active_order = {
            'order_id': 'ord-active',
            'plan': 'premium',
            'days': 30,
            'base_price_usdt': 20.0,
            'amount_usdt': 20.042,
            'network': 'bep20',
            'token_symbol': 'USDT',
            'deposit_address': '0xabc',
            'status': 'awaiting_payment',
        }
        recent_orders = [
            {
                'order_id': 'ord-completed',
                'plan': 'plus',
                'days': 15,
                'base_price_usdt': 7.5,
                'amount_usdt': 7.542,
                'network': 'bep20',
                'token_symbol': 'USDT',
                'deposit_address': '0xabc',
                'status': 'completed',
                'created_at': None,
                'updated_at': None,
            }
        ]
        referral_stats = {
            'total_referred': 5,
            'plus_referred': 2,
            'premium_referred': 3,
            'current_plus': 0,
            'current_premium': 3,
            'valid_referrals_total': 3,
            'reward_days_total': 25,
            'pending_rewards': ['30 días comprados → 15 días de recompensa'],
        }
        referral_rewards = [
            {
                'referred_id': 99,
                'activated_plan': 'premium',
                'activated_days': 30,
                'reward_days_applied': 15,
                'reward_plan_applied': 'premium',
            }
        ]
        timeline = [
            {
                'event_type': 'purchase',
                'plan': 'premium',
                'days': 30,
                'source': 'payment_bep20',
                'before_plan': 'free',
                'after_plan': 'premium',
                'metadata': {'order_id': 'ord-completed'},
            }
        ]
        with patch('app.miniapp.service.build_watchlist_context', return_value={'meta': {'symbols': ['BTCUSDT'], 'symbols_count': 1, 'max_symbols': 25, 'slots_left': 24, 'plan': 'premium', 'plan_name': 'PREMIUM', 'can_add_more': True}}), \
             patch('app.miniapp.service.get_active_payment_order_for_user', return_value=active_order), \
             patch('app.miniapp.service._load_recent_payment_orders', return_value=recent_orders), \
             patch('app.miniapp.service._load_payment_order_summary', return_value={'open': 1, 'completed': 2, 'expired': 0, 'cancelled': 1, 'total': 4}), \
             patch('app.miniapp.service.get_user_referral_stats', return_value=referral_stats), \
             patch('app.miniapp.service.get_referral_link', return_value='https://t.me/HADES_ALPHA_bot?start=ref_42'), \
             patch('app.miniapp.service._load_recent_referral_rewards', return_value=referral_rewards), \
             patch('app.miniapp.service._load_recent_subscription_events', return_value=timeline), \
             patch('app.miniapp.service.is_payment_configuration_ready', return_value=True), \
             patch('app.miniapp.service.plan_status', return_value={'plan': 'premium', 'status': 'active', 'expires': None, 'days_left': 12}):
            payload = build_account_center_payload(user)

        self.assertEqual(payload['overview']['user_id'], 42)
        self.assertEqual(payload['overview']['watchlist_limit'], 25)
        self.assertEqual(payload['subscription']['plan'], 'premium')
        self.assertEqual(payload['billing']['summary']['completed'], 2)
        self.assertTrue(payload['billing']['payment_config_ready'])
        self.assertEqual(payload['billing']['active_order']['order_id'], 'ord-active')
        self.assertEqual(payload['referrals']['total_referred'], 5)
        self.assertEqual(payload['referrals']['recent_rewards'][0]['reward_days'], 15)
        self.assertEqual(payload['timeline'][0]['event_label'], 'Compra aplicada')
        self.assertIn('Únete a HADES Alpha', payload['referrals']['share_text'])
        self.assertIn('premium', payload['plans'])

    def test_bootstrap_payload_embeds_account_center(self):
        user = {'user_id': 7, 'plan': 'free', 'subscription_status': 'trial', 'username': 'dev', 'language': 'es'}
        with patch('app.miniapp.service.build_me_payload', return_value={'user_id': 7}), \
             patch('app.miniapp.service.build_dashboard_payload', return_value={'active_payment_order': None}), \
             patch('app.miniapp.service.build_signals_payload', return_value=[]), \
             patch('app.miniapp.service.build_history_payload', return_value=[]), \
             patch('app.miniapp.service.build_market_payload', return_value={'radar': [], 'radar_summary': {'total': 0}}), \
             patch('app.miniapp.service.build_watchlist_payload', return_value=[]), \
             patch('app.miniapp.service.build_watchlist_context', return_value={'meta': {'symbols': [], 'symbols_count': 0, 'max_symbols': 2, 'slots_left': 2, 'plan': 'free', 'plan_name': 'FREE', 'can_add_more': True}}), \
             patch('app.miniapp.service.build_plans_payload', return_value={'plus': [], 'premium': []}), \
             patch('app.miniapp.service.build_account_center_payload', return_value={'overview': {'user_id': 7}, 'billing': {'summary': {'total': 0}}}):
            payload = build_bootstrap_payload(user)

        self.assertIn('account', payload)
        self.assertEqual(payload['account']['overview']['user_id'], 7)


if __name__ == '__main__':
    unittest.main()
