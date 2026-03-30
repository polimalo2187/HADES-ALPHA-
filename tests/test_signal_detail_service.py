import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_signal_detail_payload


class SignalDetailPayloadTests(unittest.TestCase):
    def test_free_plan_is_forced_to_basic_profile_and_hides_advanced_breakdown(self):
        user = {'user_id': 10, 'plan': 'free'}
        tracking_payload = {
            'signal_id': 'sig-1',
            'symbol': 'BTCUSDT',
            'direction': 'LONG',
            'visibility': 'premium',
            'normalized_score': 81.2,
            'setup_group': 'momentum',
            'entry_price': 100.0,
            'status': 'active',
            'created_at': None,
            'telegram_valid_until': None,
            'selected_profile': 'moderado',
            'state_label': 'ACTIVA',
            'entry_state_label': 'EN ZONA DE ENTRADA',
            'result_label': 'Aún sin cierre final',
            'recommendation': 'Todavía operable.',
            'current_price': 100.5,
            'entry_zone_low': 99.0,
            'entry_zone_high': 101.0,
            'stop_loss': 97.0,
            'take_profits': [103.0, 106.0],
            'current_move_pct': 0.005,
            'distance_to_entry_pct': 0.005,
            'stop_distance_pct': 0.03,
            'tp1_distance_pct': 0.03,
            'tp1_hit_now': False,
            'tp2_hit_now': False,
            'stop_hit_now': False,
            'in_entry_zone': True,
            'is_operable_now': True,
            'warnings': ['warning tracking'],
        }
        analysis_payload = {
            'setup_group': 'momentum',
            'score': 78.0,
            'normalized_score': 81.2,
            'atr_pct': 0.021,
            'timeframes': ['5m', '15m'],
            'components': [('trend_structure', 18.5), ('entry_freshness', 12.0)],
            'raw_components': [('trend_structure', 17.9)],
            'normalized_components': [('trend_structure', 18.5)],
            'selected_stop_distance_pct': 0.03,
            'selected_tp1_distance_pct': 0.03,
            'selected_tp2_distance_pct': 0.06,
            'warnings': ['warning analysis'],
            'selected_profile_payload': {'leverage': 'x5'},
            'score_profile': 'shared',
            'score_calibration': 'balanced',
            'market_validity_minutes': 45,
        }

        with patch('app.miniapp.service.plan_status', return_value={'plan': 'free'}), \
             patch('app.miniapp.service.get_signal_tracking_for_user', return_value=tracking_payload) as tracking_mock, \
             patch('app.miniapp.service.get_signal_analysis_for_user', return_value=analysis_payload):
            payload = build_signal_detail_payload(user, 'sig-1', profile_name='agresivo')

        self.assertIsNotNone(payload)
        self.assertEqual(payload['tracking_tier'], 'basic')
        self.assertEqual(payload['selected_profile'], 'moderado')
        self.assertEqual(payload['profile_options'], ['moderado'])
        self.assertEqual(payload['analysis']['components'][0]['label'], 'Estructura de tendencia')
        self.assertNotIn('raw_components', payload['analysis'])
        self.assertIsNotNone(payload['upgrade_hint'])
        tracking_mock.assert_called_once_with(10, 'sig-1', profile_name='moderado')

    def test_premium_plan_includes_advanced_breakdown(self):
        user = {'user_id': 11, 'plan': 'premium'}
        tracking_payload = {
            'signal_id': 'sig-2',
            'symbol': 'ETHUSDT',
            'direction': 'SHORT',
            'visibility': 'premium',
            'normalized_score': 88.4,
            'setup_group': 'reversal',
            'entry_price': 200.0,
            'status': 'active',
            'created_at': None,
            'telegram_valid_until': None,
            'selected_profile': 'agresivo',
            'state_label': 'ACTIVA',
            'entry_state_label': 'ESPERANDO ENTRADA',
            'result_label': 'Aún sin cierre final',
            'recommendation': 'Espera entrada.',
            'current_price': 198.0,
            'entry_zone_low': 197.0,
            'entry_zone_high': 201.0,
            'stop_loss': 205.0,
            'take_profits': [194.0, 190.0],
            'current_move_pct': 0.01,
            'distance_to_entry_pct': 0.01,
            'stop_distance_pct': 0.025,
            'tp1_distance_pct': 0.03,
            'tp1_hit_now': False,
            'tp2_hit_now': False,
            'stop_hit_now': False,
            'in_entry_zone': False,
            'is_operable_now': True,
            'warnings': [],
        }
        analysis_payload = {
            'setup_group': 'reversal',
            'score': 84.0,
            'normalized_score': 88.4,
            'atr_pct': 0.018,
            'timeframes': ['5m', '1h'],
            'components': [('trend_structure', 16.0), ('profile_penalty', -2.0)],
            'raw_components': [('trend_structure', 15.0)],
            'normalized_components': [('trend_structure', 16.0)],
            'selected_stop_distance_pct': 0.025,
            'selected_tp1_distance_pct': 0.03,
            'selected_tp2_distance_pct': 0.05,
            'warnings': [],
            'selected_profile_payload': {'leverage': 'x8'},
            'score_profile': 'shared',
            'score_calibration': 'premium-calibrated',
            'market_validity_minutes': 60,
        }

        with patch('app.miniapp.service.plan_status', return_value={'plan': 'premium'}), \
             patch('app.miniapp.service.get_signal_tracking_for_user', return_value=tracking_payload) as tracking_mock, \
             patch('app.miniapp.service.get_signal_analysis_for_user', return_value=analysis_payload):
            payload = build_signal_detail_payload(user, 'sig-2', profile_name='agresivo')

        self.assertEqual(payload['tracking_tier'], 'advanced')
        self.assertEqual(payload['selected_profile'], 'agresivo')
        self.assertEqual(payload['analysis']['score_profile'], 'shared')
        self.assertEqual(payload['analysis']['raw_components'][0]['label'], 'Estructura de tendencia')
        self.assertIsNone(payload['upgrade_hint'])
        tracking_mock.assert_called_once_with(11, 'sig-2', profile_name='agresivo')


if __name__ == '__main__':
    unittest.main()
