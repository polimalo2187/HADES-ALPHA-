import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp import service


class WatchlistEnrichmentTests(unittest.TestCase):
    def test_build_watchlist_context_returns_enriched_operational_fields(self):
        ticker = {
            'symbol': 'BTCUSDT',
            'lastPrice': '50000',
            'priceChangePercent': '5.5',
            'priceChange': '2600',
            'quoteVolume': '1200000000',
            'volume': '24000',
            'highPrice': '51000',
            'lowPrice': '47000',
            'count': '123456',
        }
        user = {'user_id': 10, 'plan': 'free'}
        with patch('app.miniapp.service.get_watchlist', return_value=['BTCUSDT']), \
             patch('app.miniapp.service.plan_status', return_value={'plan': 'free'}), \
             patch('app.miniapp.service.get_futures_24h_tickers', return_value=[ticker]):
            payload = service.build_watchlist_context(user)

        self.assertEqual(payload['meta']['symbols_count'], 1)
        row = payload['items'][0]
        self.assertEqual(row['symbol'], 'BTCUSDT')
        self.assertEqual(row['trade_count'], 123456)
        self.assertAlmostEqual(row['range_pct_24h'], ((51000 - 47000) / 47000) * 100.0, places=4)
        self.assertAlmostEqual(row['range_position_pct'], 75.0, places=2)
        self.assertEqual(row['range_bias_label'], 'Zona media 24h')
        self.assertEqual(row['volatility_label'], 'Activo')
        self.assertEqual(row['price_change_abs'], 2600.0)

    def test_build_watchlist_context_marks_missing_symbol_without_crashing(self):
        user = {'user_id': 10, 'plan': 'free'}
        with patch('app.miniapp.service.get_watchlist', return_value=['ETHUSDT']), \
             patch('app.miniapp.service.plan_status', return_value={'plan': 'free'}), \
             patch('app.miniapp.service.get_futures_24h_tickers', return_value=[]):
            payload = service.build_watchlist_context(user)

        row = payload['items'][0]
        self.assertEqual(row['symbol'], 'ETHUSDT')
        self.assertEqual(row['range_bias_label'], 'Sin datos de Binance')
        self.assertEqual(row['volatility_label'], 'Sin datos')
        self.assertIsNone(row['range_position_pct'])


if __name__ == '__main__':
    unittest.main()
