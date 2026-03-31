import tests._bootstrap
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app
from app.risk import RiskConfigurationError


class MiniAppRiskEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_risk_endpoint_returns_payload(self):
        payload = {
            'overview': {'plan': 'plus', 'feature_tier': 'full', 'profile_options': ['conservador', 'moderado', 'agresivo']},
            'profile': {'capital_usdt': 250.0, 'risk_percent': 1.0},
            'signals': {'live': [], 'history': [], 'selected_signal_id': None},
            'preview': None,
            'preview_error': None,
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.build_risk_center_payload', return_value=payload) as mocked_builder:
            with self._build_client() as client:
                response = client.get('/api/miniapp/risk?signal_id=sig-1&profile=agresivo&leverage=20', headers={'Authorization': 'Bearer token'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['profile']['capital_usdt'], 250.0)
        mocked_builder.assert_called_once()
        _, kwargs = mocked_builder.call_args
        self.assertEqual(kwargs['signal_id'], 'sig-1')
        self.assertEqual(kwargs['profile_name'], 'agresivo')
        self.assertEqual(kwargs['override_leverage'], 20.0)

    def test_risk_profile_update_forces_free_profile_and_applies_exchange_preset(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'free'}), \
             patch('app.miniapp.app.plan_status', return_value={'plan': 'free'}), \
             patch('app.miniapp.app.build_risk_center_payload', side_effect=[{'profile': {'exchange': 'binance', 'entry_mode': 'limit_wait'}}, {'ok': True}]), \
             patch('app.miniapp.app.get_exchange_fee_preset', return_value={'exchange': 'coinw', 'entry_mode': 'limit_fast', 'fee_percent_per_side': 0.01, 'slippage_percent': 0.06}), \
             patch('app.miniapp.app.save_user_risk_profile') as mocked_save:
            with self._build_client() as client:
                response = client.post(
                    '/api/miniapp/risk/profile',
                    headers={'Authorization': 'Bearer token'},
                    json={
                        'capital_usdt': 500,
                        'risk_percent': 1.5,
                        'exchange': 'coinw',
                        'entry_mode': 'limit_fast',
                        'default_profile': 'agresivo',
                    },
                )

        self.assertEqual(response.status_code, 200)
        mocked_save.assert_called_once()
        args, _ = mocked_save.call_args
        self.assertEqual(args[0], 10)
        patch_payload = args[1]
        self.assertEqual(patch_payload['exchange'], 'coinw')
        self.assertEqual(patch_payload['entry_mode'], 'limit_fast')
        self.assertEqual(patch_payload['fee_percent_per_side'], 0.01)
        self.assertEqual(patch_payload['slippage_percent'], 0.06)
        self.assertEqual(patch_payload['default_profile'], 'moderado')

    def test_risk_profile_update_returns_400_for_invalid_profile(self):
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.plan_status', return_value={'plan': 'plus'}), \
             patch('app.miniapp.app.save_user_risk_profile', side_effect=RiskConfigurationError('capital inválido')):
            with self._build_client() as client:
                response = client.post(
                    '/api/miniapp/risk/profile',
                    headers={'Authorization': 'Bearer token'},
                    json={'capital_usdt': -1},
                )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'capital inválido')


if __name__ == '__main__':
    unittest.main()
