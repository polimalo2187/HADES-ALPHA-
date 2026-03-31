import tests._bootstrap
import unittest
from datetime import timedelta
from unittest.mock import patch

from bson import ObjectId
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


    def test_confirm_payment_endpoint_serializes_order_object_id(self):
        now = utcnow()
        order = {
            "_id": ObjectId(),
            "order_id": "ord-456",
            "user_id": 10,
            "plan": "premium",
            "days": 30,
            "base_price_usdt": 20.0,
            "amount_usdt": 20.141,
            "network": "bep20",
            "token_symbol": "USDT",
            "deposit_address": "0xreceiver",
            "status": "completed",
            "confirmations": 3,
            "expires_at": now + timedelta(minutes=10),
            "created_at": now,
            "updated_at": now,
            "matched_tx_hash": "0xtxhash",
        }
        result = {
            "ok": True,
            "reason": "payment_confirmed",
            "order": order,
            "verification": {
                "status": "confirmed",
                "reason": "payment_confirmed",
                "tx_hash": "0xtxhash",
                "confirmations": 3,
            },
        }
        with patch('app.miniapp.app.initialize_database'), \
             patch('app.miniapp.app.start_background_heartbeat'), \
             patch('app.miniapp.app.get_mini_app_cors_origins', return_value=['https://hades.example.com']), \
             patch('app.miniapp.app.is_mini_app_dev_auth_enabled', return_value=False), \
             patch('app.miniapp.app.parse_session_token', return_value={'uid': 10}), \
             patch('app.miniapp.app.get_user_by_id', return_value={'user_id': 10, 'banned': False, 'plan': 'premium'}), \
             patch('app.miniapp.app.confirm_payment_order', return_value=result):
            with self._build_client() as client:
                response = client.post('/api/miniapp/payment-order/confirm', headers={'Authorization': 'Bearer token'}, json={'order_id': 'ord-456'})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['order']['order_id'], 'ord-456')
        self.assertNotIn('_id', body['order'])
        self.assertEqual(body['verification']['tx_hash'], '0xtxhash')


if __name__ == '__main__':
    unittest.main()
