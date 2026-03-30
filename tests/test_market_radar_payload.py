import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_market_payload


class MarketRadarPayloadTests(unittest.TestCase):
    def test_build_market_payload_enriches_radar_cards_and_summary(self):
        snapshot = {
            'bias': 'Alcista',
            'preferred_side': 'LONGS',
            'regime': 'Expansión',
            'volatility': 'Alta',
            'participation': 'Amplia',
            'recommendation': 'Buscar continuación.',
            'top_gainers': [],
            'top_losers': [],
            'top_volume': [],
            'top_open_interest': [],
            'btc': {},
            'eth': {},
            'adv_ratio_pct': 61.2,
        }
        radar_rows = [
            {
                'symbol': 'BTCUSDT',
                'score': 87,
                'final_score': 91,
                'direction': 'LONG',
                'change_pct': 6.2,
                'last_price': 68000,
                'quote_volume': 150000000,
                'trades': 98000,
                'momentum': 'Muy alto',
            },
            {
                'symbol': 'ETHUSDT',
                'score': 75,
                'final_score': 78,
                'direction': 'SHORT',
                'change_pct': -4.1,
                'last_price': 3200,
                'quote_volume': 96000000,
                'trades': 54000,
                'momentum': 'Alto',
            },
        ]
        tickers = [
            {
                'symbol': 'BTCUSDT',
                'lastPrice': '68000',
                'priceChangePercent': '6.2',
                'quoteVolume': '150000000',
                'highPrice': '69000',
                'lowPrice': '64000',
                'count': '98000',
                'priceChange': '3900',
            },
            {
                'symbol': 'ETHUSDT',
                'lastPrice': '3200',
                'priceChangePercent': '-4.1',
                'quoteVolume': '96000000',
                'highPrice': '3420',
                'lowPrice': '3160',
                'count': '54000',
                'priceChange': '-136',
            },
        ]
        latest_signal = {
            'signal_id': 'sig-btc',
            'symbol': 'BTCUSDT',
            'direction': 'LONG',
            'visibility': 'premium',
            'normalized_score': 88,
            'setup_group': 'breakout',
            'status': 'active',
        }
        with patch('app.miniapp.service.get_market_state_snapshot', return_value=snapshot), \
             patch('app.miniapp.service.get_radar_opportunities', return_value=radar_rows), \
             patch('app.miniapp.service.get_futures_24h_tickers', return_value=tickers), \
             patch('app.miniapp.service._load_watchlist_signal_context', return_value=({'BTCUSDT': latest_signal}, {'BTCUSDT': latest_signal})), \
             patch('app.miniapp.service.get_premium_index', side_effect=[{'lastFundingRate': '0.0004'}, {'lastFundingRate': '-0.0001'}]), \
             patch('app.miniapp.service.get_open_interest', side_effect=[{'openInterest': '2500000'}, {'openInterest': '1800000'}]):
            payload = build_market_payload({'user_id': 10, 'plan': 'premium'})

        self.assertEqual(payload['bias'], 'Alcista')
        self.assertEqual(len(payload['radar']), 2)
        self.assertEqual(payload['radar_summary']['total'], 2)
        self.assertEqual(payload['radar_summary']['longs'], 1)
        self.assertEqual(payload['radar_summary']['shorts'], 1)
        self.assertEqual(payload['radar_summary']['active_signals'], 1)

        btc = payload['radar'][0]
        self.assertEqual(btc['symbol'], 'BTCUSDT')
        self.assertEqual(btc['direction'], 'LONG')
        self.assertTrue(btc['has_active_signal'])
        self.assertEqual(btc['proximity_label'], 'Activa')
        self.assertEqual(btc['window_label'], 'Seguimiento activo')
        self.assertEqual(btc['conviction_label'], 'Seguimiento')
        self.assertGreaterEqual(btc['priority_score'], 90.0)
        self.assertEqual(btc['latest_signal']['signal_id'], 'sig-btc')
        self.assertAlmostEqual(btc['funding_rate_pct'], 0.04, places=4)
        self.assertEqual(btc['open_interest'], 2500000.0)
        self.assertTrue(any('señal activa' in reason.lower() for reason in btc['reasons']))

        eth = payload['radar'][1]
        self.assertEqual(eth['direction'], 'SHORT')
        self.assertFalse(eth['has_active_signal'])
        self.assertIn(eth['proximity_label'], {'Inmediata', 'Cercana', 'Preparando', 'Temprana'})
        self.assertAlmostEqual(eth['funding_rate_pct'], -0.01, places=4)


if __name__ == '__main__':
    unittest.main()
