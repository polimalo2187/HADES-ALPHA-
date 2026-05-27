import tests._bootstrap
import unittest
from datetime import datetime
from unittest.mock import patch

from app.statistics import build_admin_strategy_observability


class ScannerStrategyObservabilityTests(unittest.TestCase):
    def test_build_admin_strategy_observability_aggregates_cycles(self):
        rows = [
            {
                'cycle_started_at': datetime(2026, 4, 20, 10, 0, 0),
                'attempted_symbols_total': 48,
                'candidate_pool_total': 3,
                'selected_signals_total': 1,
                'rejected_symbols_total': 45,
                'risk_off_symbols_total': 0,
                'failure_symbols_total': 0,
                'market_regime': {'state': 'continuation_clean'},
                'attempts_by_strategy': {'breakout_reset': 48},
                'candidate_pool_by_strategy': {'breakout_reset': 3},
                'selected_by_strategy': {'breakout_reset': 1},
                'rejected_by_strategy': {'breakout_reset': 45},
                'reject_reasons_by_strategy': {'breakout_reset': {'trend_structure': 20, 'continuation_candle': 10}},
            },
            {
                'cycle_started_at': datetime(2026, 4, 20, 10, 1, 0),
                'attempted_symbols_total': 48,
                'candidate_pool_total': 5,
                'selected_signals_total': 2,
                'rejected_symbols_total': 43,
                'risk_off_symbols_total': 0,
                'failure_symbols_total': 1,
                'market_regime': {'state': 'sweep_reversal'},
                'attempts_by_strategy': {'liquidity_sweep_reversal': 48},
                'candidate_pool_by_strategy': {'liquidity_sweep_reversal': 5},
                'selected_by_strategy': {'liquidity_sweep_reversal': 2},
                'rejected_by_strategy': {'liquidity_sweep_reversal': 43},
                'reject_reasons_by_strategy': {'liquidity_sweep_reversal': {'liquidity_confirmation': 12}},
            },
        ]
        latest_cycle = {
            'available': True,
            'status': 'ok',
            'cycle_number': 501,
            'market_strategy_key': 'liquidity_sweep_reversal',
            'market_strategy_label': 'Liquidity Sweep Reversal',
        }

        with patch('app.statistics._fetch_scanner_cycle_stats', return_value=rows), \
             patch('app.statistics.get_latest_scanner_cycle_snapshot', return_value=latest_cycle):
            payload = build_admin_strategy_observability(days=30)

        self.assertTrue(payload['overview']['telemetry_ready'])
        self.assertEqual(payload['overview']['cycles_total'], 2)
        self.assertEqual(payload['overview']['attempted_symbols_total'], 96)
        self.assertEqual(payload['overview']['selected_signals_total'], 3)
        self.assertEqual(payload['strategy_pipeline'][0]['strategy_key'], 'breakout_reset')
        self.assertEqual(payload['strategy_pipeline'][0]['attempted_symbols'], 48)
        self.assertEqual(payload['strategy_pipeline'][1]['strategy_key'], 'liquidity_sweep_reversal')
        self.assertEqual(payload['strategy_pipeline'][1]['selected_signals'], 2)
        breakout_rejects = next(item for item in payload['reject_reasons_by_strategy'] if item['strategy_key'] == 'breakout_reset')
        self.assertEqual(breakout_rejects['top_reasons'][0]['reason'], 'trend_structure')
        self.assertEqual(payload['regime_distribution'][0]['regime_state'], 'continuation_clean')
        matrix_keys = {(row['regime_state'], row['strategy_key']) for row in payload['regime_strategy_matrix']}
        self.assertIn(('continuation_clean', 'breakout_reset'), matrix_keys)
        self.assertIn(('sweep_reversal', 'liquidity_sweep_reversal'), matrix_keys)
        self.assertEqual(payload['latest_cycle']['cycle_number'], 501)


if __name__ == '__main__':
    unittest.main()
