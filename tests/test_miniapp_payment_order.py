import tests._bootstrap
import unittest
from datetime import timedelta
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.miniapp.app import create_mini_app
from app.models import utcnow


class MiniAppPaymentOrderEndpointTests(unittest.TestCase):
    def _build_client(self):
        app = create_mini_app()
        return TestClient(app)

    def test_create_payment_order_endpoint_serializes_open_order(self):
        order = {
            "order_id": "ord-123",
            "user_id": 10,
            "plan": "plus",
            "days": 15,
            "base_price_usdt": 7.5,
            "amount_usdt": 7.51,
            "network": "bep20",
            "token_symbol": "USDT",
            "deposit_address": "0xreceiver",
            "status": "awaiting_payment",
            "confirmations": 0,
            "expires_at": utcnow() + timedelta(minutes=10),
            "created_at": utcnow(),
            "updated_at": utcnow(),
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'plus'}), \
             patch('app.miniapp.app.create_payment_order', return_value=order):
            with self._build_client() as client:
                response = client.post('/api/miniapp/payment-order', headers={'Authorization': 'Bearer token'}, json={'plan': 'plus', 'days': 15})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['order']['order_id'], 'ord-123')
        self.assertEqual(body['order']['status'], 'awaiting_payment')
        self.assertIsInstance(body['order']['expires_in_minutes'], int)


if __name__ == '__main__':
    unittest.main()
