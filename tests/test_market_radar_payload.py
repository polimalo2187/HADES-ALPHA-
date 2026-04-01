import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_market_payload, build_radar_symbol_payload


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
            'ranked_gainers': [{'symbol': 'BTCUSDT'}, {'symbol': 'SOLUSDT'}, {'symbol': 'ETHUSDT'}, {'symbol': 'XRPUSDT'}, {'symbol': 'ADAUSDT'}],
            'ranked_losers': [{'symbol': 'ETHUSDT'}, {'symbol': 'XRPUSDT'}, {'symbol': 'ADAUSDT'}, {'symbol': 'DOGEUSDT'}, {'symbol': 'LINKUSDT'}],
            'ranked_volume': [{'symbol': 'BTCUSDT'}, {'symbol': 'ETHUSDT'}, {'symbol': 'SOLUSDT'}, {'symbol': 'XRPUSDT'}, {'symbol': 'ADAUSDT'}],
            'btc': {},
            'eth': {},
            'adv_ratio_pct': 61.2,
        }
        radar_rows = [
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
                'symbol': 'SOLUSDT',
                'score': 68,
                'final_score': 69,
                'direction': 'LONG',
                'change_pct': 2.1,
                'last_price': 152,
                'quote_volume': 45000000,
                'trades': 33000,
                'momentum': 'Medio',
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
            {
                'symbol': 'SOLUSDT',
                'lastPrice': '152',
                'priceChangePercent': '2.1',
                'quoteVolume': '45000000',
                'highPrice': '156',
                'lowPrice': '147',
                'count': '33000',
                'priceChange': '3.1',
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
             patch('app.miniapp.service.get_premium_index', side_effect=[{'lastFundingRate': '-0.0001'}, {'lastFundingRate': '0.0004'}, {'lastFundingRate': '0.0002'}]), \
             patch('app.miniapp.service.get_open_interest', side_effect=[{'openInterest': '1800000'}, {'openInterest': '2500000'}, {'openInterest': '950000'}]):
            payload = build_market_payload({'user_id': 10, 'plan': 'premium'})

        self.assertEqual(payload['bias'], 'Alcista')
        self.assertEqual(payload['market_rotation']['chunk_size'], 4)
        self.assertEqual(payload['market_rotation']['gainers_total'], 5)
        self.assertEqual(len(payload['top_gainers']), 4)
        self.assertEqual(len(payload['top_gainers_ranked']), 5)
        self.assertEqual(len(payload['radar']), 3)
        self.assertEqual(payload['radar_summary']['total'], 3)
        self.assertEqual(payload['radar_summary']['longs'], 2)
        self.assertEqual(payload['radar_summary']['shorts'], 1)
        self.assertEqual(payload['radar_summary']['active_signals'], 1)
        self.assertEqual(payload['radar_summary']['priority_mix']['maxima'], 1)
        self.assertEqual(payload['radar_summary']['signal_mix']['activa'], 1)
        self.assertEqual(payload['radar_summary']['execution_mix']['seguimiento'], 1)
        self.assertEqual(payload['radar_summary']['alignment_mix']['a_favor'], 2)
        self.assertEqual(payload['radar_summary']['focus_now'], 1)
        self.assertEqual(payload['radar_summary']['aligned_now'], 2)
        self.assertEqual(payload['radar_summary']['sort_default'], 'ranking')

        btc = payload['radar'][0]
        self.assertEqual(btc['symbol'], 'BTCUSDT')
        self.assertEqual(btc['direction'], 'LONG')
        self.assertTrue(btc['has_active_signal'])
        self.assertEqual(btc['proximity_label'], 'Activa')
        self.assertEqual(btc['window_label'], 'Seguimiento activo')
        self.assertEqual(btc['conviction_label'], 'Seguimiento')
        self.assertGreaterEqual(btc['priority_score'], 90.0)
        self.assertGreaterEqual(btc['ranking_score'], 96.0)
        self.assertEqual(btc['signal_context_label'], 'Activa')
        self.assertEqual(btc['alignment_label'], 'A favor')
        self.assertEqual(btc['execution_state_label'], 'Seguimiento')
        self.assertEqual(btc['setup_mode_label'], 'Continuación')
        self.assertEqual(btc['risk_label'], 'Gestionar')
        self.assertTrue(any('Riesgo:' in step for step in btc['trade_plan']))
        self.assertEqual(btc['priority_rank'], 5)
        self.assertEqual(btc['proximity_rank'], 5)
        self.assertEqual(btc['latest_signal']['signal_id'], 'sig-btc')
        self.assertAlmostEqual(btc['funding_rate_pct'], 0.04, places=4)
        self.assertEqual(btc['open_interest'], 2500000.0)
        self.assertTrue(any('señal activa' in reason.lower() for reason in btc['reasons']))

        eth = next(item for item in payload['radar'] if item['symbol'] == 'ETHUSDT')
        self.assertEqual(eth['direction'], 'SHORT')
        self.assertFalse(eth['has_active_signal'])
        self.assertIn(eth['proximity_label'], {'Inmediata', 'Cercana', 'Preparando', 'Temprana'})
        self.assertEqual(eth['signal_context_label'], 'Sin señal')
        self.assertEqual(eth['alignment_label'], 'Contratendencia')
        self.assertIn(eth['execution_state_label'], {'Ejecutable', 'Preparación', 'Observación'})
        self.assertIn(eth['risk_label'], {'Reducido', 'Selectivo', 'Cauto'})
        self.assertAlmostEqual(eth['funding_rate_pct'], -0.01, places=4)


    def test_build_radar_symbol_payload_adds_scanner_and_signal_context(self):
        snapshot = {
            'bias': 'Alcista',
            'preferred_side': 'LONGS',
            'regime': 'Expansión',
            'environment': 'Momentum',
            'recommendation': 'Buscar continuación.',
            'top_gainers': [],
            'top_losers': [],
            'top_volume': [],
            'top_open_interest': [],
            'btc': {},
            'eth': {},
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
        scanner = {
            'status': 'confirmed',
            'label': 'Setup confirmado',
            'summary': 'Scanner confirma LONG BREAKOUT con score 86.0.',
            'direction': 'LONG',
            'direction_alignment': True,
            'setup_group': 'breakout',
            'score': 86.0,
            'atr_pct': 0.0123,
            'timeframes': ['5M'],
            'score_profile': 'shared',
            'score_calibration': 'v1',
            'components': [{'label': 'Volumen', 'score': 22.0}],
            'strongest_component': {'label': 'Volumen', 'score': 22.0},
            'weakest_component': {'label': 'Volumen', 'score': 22.0},
            'profiles': [{'profile': 'moderado', 'label': 'Moderado', 'entry_price': 68000, 'stop_loss': 67200, 'tp1': 69000, 'tp2': 69800, 'leverage': 5}],
        }
        with patch('app.miniapp.service.get_market_state_snapshot', return_value=snapshot), \
             patch('app.miniapp.service.get_radar_opportunities', return_value=radar_rows), \
             patch('app.miniapp.service.get_futures_24h_tickers', return_value=tickers), \
             patch('app.miniapp.service._load_watchlist_signal_context', return_value=({'BTCUSDT': latest_signal}, {'BTCUSDT': latest_signal})), \
             patch('app.miniapp.service.get_premium_index', side_effect=[{'lastFundingRate': '0.0004'}, {'lastFundingRate': '-0.0001'}]), \
             patch('app.miniapp.service.get_open_interest', side_effect=[{'openInterest': '2500000'}, {'openInterest': '1800000'}]), \
             patch('app.miniapp.service._build_radar_scanner_snapshot', return_value=scanner):
            payload = build_radar_symbol_payload({'user_id': 10, 'plan': 'premium'}, 'BTCUSDT')

        self.assertIsNotNone(payload)
        self.assertEqual(payload['symbol'], 'BTCUSDT')
        self.assertEqual(payload['market_context']['bias'], 'Alcista')
        self.assertEqual(payload['radar']['symbol'], 'BTCUSDT')
        self.assertEqual(payload['scanner']['status'], 'confirmed')
        self.assertTrue(payload['signal_context']['has_active_signal'])
        self.assertTrue(payload['signal_context']['signal_detail_available'])
        self.assertTrue(any('Scanner confirma' in item for item in payload['tactical_checks']))
        self.assertTrue(any('alineados' in item.lower() for item in payload['tactical_checks']))


if __name__ == '__main__':
    unittest.main()
