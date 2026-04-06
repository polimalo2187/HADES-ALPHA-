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
        radar_row = {
            'symbol': 'BTCUSDT',
            'score': 82,
            'final_score': 78,
            'direction': 'LONG',
            'momentum': 'Alto',
        }
        latest_signal = {
            'signal_id': 'sig-1',
            'symbol': 'BTCUSDT',
            'direction': 'LONG',
            'visibility': 'plus',
            'normalized_score': 74,
            'setup_group': 'shared',
            'status': 'active',
        }
        user = {'user_id': 10, 'plan': 'free'}
        with patch('app.miniapp.service.get_watchlist', return_value=['BTCUSDT']),              patch('app.miniapp.service.plan_status', return_value={'plan': 'free'}),              patch('app.miniapp.service.get_futures_24h_tickers', return_value=[ticker]),              patch('app.miniapp.service.get_radar_opportunities', return_value=[radar_row]),              patch('app.miniapp.service._load_watchlist_signal_context', return_value=({'BTCUSDT': latest_signal}, {})):
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
        self.assertEqual(row['radar_direction'], 'LONG')
        self.assertEqual(row['radar_momentum'], 'Alto')
        self.assertGreater(row['setup_priority_score'], 60)
        self.assertIn(row['setup_priority_label'], {'Alta', 'Máxima'})
        self.assertIn('Radar', row['priority_reason_short'])
        self.assertEqual(row['latest_signal']['signal_id'], 'sig-1')
        self.assertFalse(row['has_active_signal'])


    def test_build_watchlist_context_fetches_full_radar_set_for_selected_symbol(self):
        ticker = {
            'symbol': 'SIRENUSDT',
            'lastPrice': '0.5973',
            'priceChangePercent': '40.8',
            'priceChange': '0.1735',
            'quoteVolume': '580800000',
            'volume': '972000000',
            'highPrice': '0.6120',
            'lowPrice': '0.4237',
            'count': '45678',
        }
        radar_row = {
            'symbol': 'SIRENUSDT',
            'score': 43.0,
            'final_score': 43.3,
            'direction': 'LONG',
            'momentum': 'Media',
        }
        user = {'user_id': 10, 'plan': 'free'}
        with patch('app.miniapp.service.get_watchlist', return_value=['SIRENUSDT']), \
             patch('app.miniapp.service.plan_status', return_value={'plan': 'free'}), \
             patch('app.miniapp.service.get_futures_24h_tickers', return_value=[ticker]), \
             patch('app.miniapp.service.get_radar_opportunities', return_value=[radar_row]) as radar_mock, \
             patch('app.miniapp.service._load_watchlist_signal_context', return_value=({}, {})):
            payload = service.build_watchlist_context(user)

        row = payload['items'][0]
        self.assertEqual(row['radar_direction'], 'LONG')
        self.assertEqual(row['radar_score'], 43.3)
        self.assertGreaterEqual(radar_mock.call_args.kwargs['limit'], 60)

    def test_build_watchlist_context_marks_active_signal_as_setup_activo(self):
        ticker = {
            'symbol': 'ETHUSDT',
            'lastPrice': '3500',
            'priceChangePercent': '-3.2',
            'priceChange': '-120',
            'quoteVolume': '450000000',
            'volume': '150000',
            'highPrice': '3700',
            'lowPrice': '3400',
            'count': '88888',
        }
        active_signal = {
            'signal_id': 'sig-active',
            'symbol': 'ETHUSDT',
            'direction': 'SHORT',
            'visibility': 'premium',
            'normalized_score': 86,
            'setup_group': 'shared',
            'status': 'active',
        }
        user = {'user_id': 10, 'plan': 'premium'}
        with patch('app.miniapp.service.get_watchlist', return_value=['ETHUSDT']),              patch('app.miniapp.service.plan_status', return_value={'plan': 'premium'}),              patch('app.miniapp.service.get_futures_24h_tickers', return_value=[ticker]),              patch('app.miniapp.service.get_radar_opportunities', return_value=[]),              patch('app.miniapp.service._load_watchlist_signal_context', return_value=({'ETHUSDT': active_signal}, {'ETHUSDT': active_signal})):
            payload = service.build_watchlist_context(user)

        row = payload['items'][0]
        self.assertTrue(row['has_active_signal'])
        self.assertEqual(row['setup_proximity_label'], 'Setup activo')
        self.assertEqual(row['setup_proximity_score'], 100.0)
        self.assertIn('señal activa', row['setup_action_label'].lower())
        self.assertEqual(row['active_signal']['visibility_name'], 'PREMIUM')
        self.assertIn('Señal activa', row['priority_reasons'][0])

    def test_build_watchlist_context_marks_missing_symbol_without_crashing(self):
        user = {'user_id': 10, 'plan': 'free'}
        with patch('app.miniapp.service.get_watchlist', return_value=['ETHUSDT']),              patch('app.miniapp.service.plan_status', return_value={'plan': 'free'}),              patch('app.miniapp.service.get_futures_24h_tickers', return_value=[]),              patch('app.miniapp.service.get_radar_opportunities', return_value=[]),              patch('app.miniapp.service._load_watchlist_signal_context', return_value=({}, {})):
            payload = service.build_watchlist_context(user)

        row = payload['items'][0]
        self.assertEqual(row['symbol'], 'ETHUSDT')
        self.assertEqual(row['range_bias_label'], 'Sin datos de Binance')
        self.assertEqual(row['volatility_label'], 'Sin datos')
        self.assertIsNone(row['range_position_pct'])
        self.assertEqual(row['setup_priority_label'], 'Baja')
        self.assertIn('Sin datos frescos', row['priority_reason_short'])


if __name__ == '__main__':
    unittest.main()
