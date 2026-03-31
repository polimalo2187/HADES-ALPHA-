import tests._bootstrap
import unittest
from unittest.mock import MagicMock, patch

from app.miniapp.service import build_risk_center_payload
from app.risk import RiskConfigurationError


class MiniAppRiskServiceTests(unittest.TestCase):
    def test_free_plan_forces_moderate_profile_options(self):
        user = {'user_id': 10, 'plan': 'free'}
        collection = MagicMock()
        collection.find.return_value.sort.return_value.limit.return_value = []

        with patch('app.miniapp.service.plan_status', return_value={'plan': 'free'}), \
             patch('app.miniapp.service.get_user_risk_profile', return_value={
                 'capital_usdt': 500,
                 'risk_percent': 1,
                 'exchange': 'binance',
                 'fee_percent_per_side': 0.02,
                 'slippage_percent': 0.03,
                 'default_profile': 'agresivo',
                 'default_leverage': 20,
                 'entry_mode': 'limit_wait',
                 'updated_at': None,
             }), \
             patch('app.miniapp.service.ensure_risk_profile_ready', return_value={}), \
             patch('app.miniapp.service.user_signals_collection', return_value=collection), \
             patch('app.miniapp.service.get_history_entries_for_user', return_value=[]):
            payload = build_risk_center_payload(user, profile_name='agresivo')

        self.assertEqual(payload['overview']['feature_tier'], 'basic')
        self.assertEqual(payload['overview']['profile_options'], ['moderado'])
        self.assertEqual(payload['profile']['default_profile'], 'moderado')
        self.assertEqual(payload['signals']['selected_profile'], 'moderado')

    def test_preview_error_is_exposed_without_breaking_payload(self):
        user = {'user_id': 11, 'plan': 'plus'}
        collection = MagicMock()
        collection.find.return_value.sort.return_value.limit.return_value = []
        signal_doc = {
            'signal_id': 'sig-1',
            'symbol': 'BTCUSDT',
            'direction': 'LONG',
            'visibility': 'plus',
            'entry_price': 100.0,
            'created_at': None,
        }
        with patch('app.miniapp.service.plan_status', return_value={'plan': 'plus'}), \
             patch('app.miniapp.service.get_user_risk_profile', return_value={
                 'capital_usdt': 500,
                 'risk_percent': 1,
                 'exchange': 'binance',
                 'fee_percent_per_side': 0.02,
                 'slippage_percent': 0.03,
                 'default_profile': 'moderado',
                 'default_leverage': 20,
                 'entry_mode': 'limit_wait',
                 'updated_at': None,
             }), \
             patch('app.miniapp.service.ensure_risk_profile_ready', return_value={}), \
             patch('app.miniapp.service.user_signals_collection', return_value=collection), \
             patch('app.miniapp.service.get_history_entries_for_user', return_value=[]), \
             patch('app.miniapp.service.get_user_signal_by_signal_id', return_value=signal_doc), \
             patch('app.miniapp.service.build_risk_preview_from_user_signal', side_effect=RiskConfigurationError('capital faltante')):
            payload = build_risk_center_payload(user, signal_id='sig-1', profile_name='moderado')

        self.assertEqual(payload['signals']['selected_signal']['signal_id'], 'sig-1')
        self.assertEqual(payload['preview_error'], 'capital faltante')
        self.assertIsNone(payload['preview'])


if __name__ == '__main__':
    unittest.main()
