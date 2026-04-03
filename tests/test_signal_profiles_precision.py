import tests._bootstrap
import unittest

from app.signals import build_user_signal_document


class SignalProfilesPrecisionTests(unittest.TestCase):
    def test_build_user_signal_document_preserves_small_price_precision(self):
        base_signal = {
            '_id': 'sig-precision',
            'symbol': 'PUMPUSDT',
            'direction': 'SHORT',
            'entry_price': 0.00172345,
            'timeframes': ['15M'],
            'valid_until': None,
            'evaluation_valid_until': None,
            'telegram_valid_until': None,
            'visibility': 'plus',
            'profiles': {
                'conservador': {
                    'stop_loss': 0.00174123,
                    'take_profits': [0.00169654, 0.00166987],
                    'leverage': '20x-30x',
                },
                'moderado': {
                    'stop_loss': 0.00173456,
                    'take_profits': [0.00169321, 0.00166234],
                    'leverage': '30x-40x',
                },
                'agresivo': {
                    'stop_loss': 0.00173001,
                    'take_profits': [0.00168888, 0.00165555],
                    'leverage': '40x-50x',
                },
            },
        }

        doc = build_user_signal_document(base_signal, user_id=99)

        self.assertAlmostEqual(doc['entry_price'], 0.00172345, places=8)
        self.assertAlmostEqual(doc['profiles']['moderado']['stop_loss'], 0.00173456, places=8)
        self.assertAlmostEqual(doc['profiles']['moderado']['take_profits'][0], 0.00169321, places=8)
        self.assertNotEqual(doc['entry_price'], doc['profiles']['moderado']['stop_loss'])


if __name__ == '__main__':
    unittest.main()
