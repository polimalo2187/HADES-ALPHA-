import tests._bootstrap
import unittest
from decimal import Decimal

from app.payment_service import build_unique_amount_candidates, format_payment_amount


class PaymentServiceTests(unittest.TestCase):
    def test_unique_amount_candidates_are_deterministic_and_unique(self):
        candidates = build_unique_amount_candidates(20.0, 12345, limit=10)
        self.assertEqual(len(candidates), 10)
        self.assertEqual(len(set(candidates)), 10)
        self.assertEqual(candidates, build_unique_amount_candidates(20.0, 12345, limit=10))

    def test_format_payment_amount_keeps_three_decimals(self):
        self.assertEqual(format_payment_amount(Decimal('20.1299')), '20.129')
        self.assertEqual(format_payment_amount(5), '5.000')

    def test_invalid_unique_amount_inputs_raise(self):
        with self.assertRaises(ValueError):
            build_unique_amount_candidates(0, 1)
        with self.assertRaises(ValueError):
            build_unique_amount_candidates(10, 0)


if __name__ == '__main__':
    unittest.main()
