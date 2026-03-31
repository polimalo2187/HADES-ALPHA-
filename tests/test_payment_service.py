import tests._bootstrap
import unittest
from datetime import timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.models import utcnow
from app.payment_service import (
    build_unique_amount_candidates,
    confirm_payment_order,
    create_payment_order,
    format_payment_amount,
)


class PaymentServiceTests(unittest.TestCase):
    def test_unique_amount_candidates_are_deterministic_and_unique(self):
        with patch('app.payment_service.get_payment_unique_max_delta', return_value=0.150):
            candidates = build_unique_amount_candidates(20.0, 12345, limit=10)
        self.assertEqual(len(candidates), 10)
        self.assertEqual(len(set(candidates)), 10)
        self.assertEqual(candidates, build_unique_amount_candidates(20.0, 12345, limit=10))
        self.assertLessEqual(max(candidates) - Decimal('20.000'), Decimal('0.150'))

    def test_unique_amount_candidates_respect_configured_delta_cap(self):
        with patch('app.payment_service.get_payment_unique_max_delta', return_value=0.120):
            candidates = build_unique_amount_candidates(10.0, 99999, limit=200)
        self.assertEqual(len(candidates), 120)
        self.assertEqual(min(candidates), Decimal('10.001'))
        self.assertEqual(max(candidates), Decimal('10.120'))

    def test_unique_amount_candidates_never_exceed_fifteen_cents(self):
        with patch('app.payment_service.get_payment_unique_max_delta', return_value=0.999):
            candidates = build_unique_amount_candidates(5.0, 42, limit=999)
        self.assertEqual(len(candidates), 150)
        self.assertEqual(min(candidates), Decimal('5.001'))
        self.assertEqual(max(candidates), Decimal('5.150'))

    def test_format_payment_amount_keeps_three_decimals(self):
        self.assertEqual(format_payment_amount(Decimal('20.1299')), '20.129')
        self.assertEqual(format_payment_amount(5), '5.000')

    def test_invalid_unique_amount_inputs_raise(self):
        with self.assertRaises(ValueError):
            build_unique_amount_candidates(0, 1)
        with self.assertRaises(ValueError):
            build_unique_amount_candidates(10, 0)

    def test_create_payment_order_reuses_existing_matching_open_order(self):
        existing = {
            'order_id': 'ord-existing',
            'user_id': 77,
            'plan': 'premium',
            'days': 30,
            'status': 'awaiting_payment',
            'expires_at': utcnow() + timedelta(minutes=10),
        }
        collection = MagicMock()
        with patch('app.payment_service.get_payment_configuration_status', return_value={'ready': True, 'missing_keys': []}), \
             patch('app.payment_service.get_active_payment_order_for_user', return_value=existing), \
             patch('app.payment_service.payment_orders_collection', return_value=collection), \
             patch('app.payment_service.cancel_open_orders_for_user') as mocked_cancel, \
             patch('app.payment_service.record_audit_event') as mocked_audit:
            order = create_payment_order(77, 'premium', 30)

        self.assertEqual(order['order_id'], 'ord-existing')
        mocked_cancel.assert_not_called()
        collection.insert_one.assert_not_called()
        self.assertTrue(any(call.kwargs.get('event_type') == 'payment_order_reused' for call in mocked_audit.mock_calls))

    def test_create_payment_order_reissues_matching_order_when_unique_delta_is_too_high(self):
        existing = {
            'order_id': 'ord-legacy',
            'user_id': 77,
            'plan': 'premium',
            'days': 7,
            'amount_usdt': 5.513,
            'base_price_usdt': 5.0,
            'status': 'awaiting_payment',
            'expires_at': utcnow() + timedelta(minutes=10),
        }
        collection = MagicMock()
        collection.find_one.return_value = None
        with patch('app.payment_service.get_payment_configuration_status', return_value={'ready': True, 'missing_keys': []}),              patch('app.payment_service.get_active_payment_order_for_user', return_value=existing),              patch('app.payment_service.payment_orders_collection', return_value=collection),              patch('app.payment_service.cancel_open_orders_for_user', return_value=1) as mocked_cancel,              patch('app.payment_service.get_payment_network', return_value='bep20'),              patch('app.payment_service.get_payment_token_symbol', return_value='USDT'),              patch('app.payment_service.get_payment_token_contract', return_value='0xabc'),              patch('app.payment_service.get_payment_receiver_address', return_value='0xreceiver'),              patch('app.payment_service.record_audit_event'),              patch('app.payment_service.heartbeat'):
            order = create_payment_order(77, 'premium', 7)

        self.assertEqual(order['plan'], 'premium')
        mocked_cancel.assert_called_once_with(77, reason='reissued_for_lower_unique_delta')
        collection.insert_one.assert_called_once()
        inserted_order = collection.insert_one.call_args.args[0]
        self.assertLessEqual(Decimal(str(inserted_order['amount_usdt'])) - Decimal(str(inserted_order['base_price_usdt'])), Decimal('0.150'))

    def test_create_payment_order_supersedes_existing_mismatched_order(self):
        existing = {
            'order_id': 'ord-old',
            'user_id': 77,
            'plan': 'plus',
            'days': 15,
            'status': 'awaiting_payment',
            'expires_at': utcnow() + timedelta(minutes=10),
        }
        collection = MagicMock()
        collection.find_one.return_value = None
        with patch('app.payment_service.get_payment_configuration_status', return_value={'ready': True, 'missing_keys': []}), \
             patch('app.payment_service.get_active_payment_order_for_user', return_value=existing), \
             patch('app.payment_service.payment_orders_collection', return_value=collection), \
             patch('app.payment_service.cancel_open_orders_for_user', return_value=1) as mocked_cancel, \
             patch('app.payment_service.get_payment_network', return_value='bep20'), \
             patch('app.payment_service.get_payment_token_symbol', return_value='USDT'), \
             patch('app.payment_service.get_payment_token_contract', return_value='0xabc'), \
             patch('app.payment_service.get_payment_receiver_address', return_value='0xreceiver'), \
             patch('app.payment_service.record_audit_event'), \
             patch('app.payment_service.heartbeat'):
            order = create_payment_order(77, 'premium', 30)

        self.assertEqual(order['plan'], 'premium')
        mocked_cancel.assert_called_once_with(77, reason='superseded_by_new_order')
        collection.insert_one.assert_called_once()
        inserted_order = collection.insert_one.call_args.args[0]
        self.assertNotIn('matched_tx_hash', inserted_order)


    def test_create_payment_order_rejects_when_payment_configuration_is_incomplete(self):
        with patch('app.payment_service.get_payment_configuration_status', return_value={'ready': False, 'missing_keys': ['BSC_RPC_HTTP_URL']}):
            with self.assertRaises(RuntimeError) as ctx:
                create_payment_order(77, 'premium', 30)
        self.assertIn('Configuración de pagos incompleta', str(ctx.exception))

    def test_confirm_payment_order_returns_in_progress_when_lock_is_already_held(self):
        now = utcnow()
        initial = {
            'order_id': 'ord-lock',
            'user_id': 55,
            'plan': 'plus',
            'days': 30,
            'amount_usdt': 15.055,
            'base_price_usdt': 15.0,
            'status': 'awaiting_payment',
            'expires_at': now + timedelta(minutes=10),
        }
        current = {**initial, 'status': 'verification_in_progress', 'verification_started_at': now}
        collection = MagicMock()
        collection.find_one_and_update.return_value = None
        with patch('app.payment_service.get_payment_order', side_effect=[initial, current]), \
             patch('app.payment_service.payment_orders_collection', return_value=collection), \
             patch('app.payment_service._payment_purchase_already_applied', return_value=False), \
             patch('app.payment_service.verify_payment') as mocked_verify:
            result = confirm_payment_order('ord-lock', 55)

        self.assertFalse(result['ok'])
        self.assertEqual(result['reason'], 'verification_in_progress')
        mocked_verify.assert_not_called()

    def test_confirm_payment_order_is_idempotent_when_purchase_was_already_applied(self):
        order = {
            'order_id': 'ord-done',
            'user_id': 99,
            'plan': 'premium',
            'days': 30,
            'amount_usdt': 20.099,
            'base_price_usdt': 20.0,
            'status': 'awaiting_payment',
            'expires_at': utcnow() + timedelta(minutes=10),
        }
        finalized = {**order, 'status': 'completed', 'last_verification_reason': 'activation_already_applied'}
        collection = MagicMock()
        collection.find_one_and_update.return_value = finalized
        with patch('app.payment_service.get_payment_order', return_value=order), \
             patch('app.payment_service.payment_orders_collection', return_value=collection), \
             patch('app.payment_service._payment_purchase_already_applied', return_value=True), \
             patch('app.payment_service.verify_payment') as mocked_verify, \
             patch('app.payment_service.activate_plan_purchase') as mocked_activate:
            result = confirm_payment_order('ord-done', 99)

        self.assertTrue(result['ok'])
        self.assertEqual(result['reason'], 'already_completed')
        self.assertEqual(result['order']['status'], 'completed')
        mocked_verify.assert_not_called()
        mocked_activate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
