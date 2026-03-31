import tests._bootstrap
import unittest
from datetime import timedelta
from unittest.mock import patch

from app.bep20_verifier import _get_transfer_logs, verify_payment
from app.models import utcnow


class Bep20VerifierTests(unittest.TestCase):
    def test_get_transfer_logs_retries_with_smaller_window_on_limit_exceeded(self):
        calls = []

        def fake_rpc(method, params):
            self.assertEqual(method, 'eth_getLogs')
            current_filter = params[0]
            current_from = int(current_filter['fromBlock'], 16)
            current_to = int(current_filter['toBlock'], 16)
            calls.append((current_from, current_to))
            if current_to - current_from + 1 > 100:
                raise RuntimeError("RPC error en eth_getLogs: {'code': -32005, 'message': 'limit exceeded'}")
            return []

        with patch('app.bep20_verifier._rpc_call', side_effect=fake_rpc):
            logs = _get_transfer_logs('0xtoken', '0xreceiver', 1000, 1249)

        self.assertEqual(logs, [])
        self.assertGreaterEqual(len(calls), 4)
        self.assertEqual(calls[0], (1000, 1249))
        self.assertEqual(calls[1], (1000, 1124))
        self.assertEqual(calls[2], (1000, 1061))
        self.assertEqual(calls[3], (1062, 1123))

    def test_verify_payment_confirms_match_with_chunked_log_queries(self):
        order = {
            'amount_usdt': 20.141,
            'created_at': utcnow() - timedelta(minutes=2),
            'expires_at': utcnow() + timedelta(minutes=28),
            'deposit_address': '0x000000000000000000000000000000000000beef',
            'token_contract': '0x000000000000000000000000000000000000cafe',
        }
        amount_raw = hex(20141000000000000000)
        matched_log = {
            'blockNumber': hex(1980),
            'transactionHash': '0xabc123',
            'data': amount_raw,
            'topics': [
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                '0x0000000000000000000000000000000000000000000000000000000000001234',
                '0x000000000000000000000000000000000000000000000000000000000000beef',
            ],
        }

        with patch('app.bep20_verifier.get_payment_token_contract', return_value='0x000000000000000000000000000000000000cafe'), \
             patch('app.bep20_verifier.get_payment_receiver_address', return_value='0x000000000000000000000000000000000000beef'), \
             patch('app.bep20_verifier.get_payment_min_confirmations', return_value=1), \
             patch('app.bep20_verifier.get_payment_token_decimals', return_value=18), \
             patch('app.bep20_verifier.get_payment_lookback_blocks', return_value=2500), \
             patch('app.bep20_verifier._get_latest_block', return_value=2000), \
             patch('app.bep20_verifier._get_transfer_logs', return_value=[matched_log]), \
             patch('app.bep20_verifier._get_block_timestamp', return_value=utcnow()):
            verification = verify_payment(order)

        self.assertEqual(verification['status'], 'confirmed')
        self.assertEqual(verification['reason'], 'payment_confirmed')
        self.assertEqual(verification['tx_hash'], '0xabc123')
        self.assertEqual(verification['confirmations'], 21)


if __name__ == '__main__':
    unittest.main()
