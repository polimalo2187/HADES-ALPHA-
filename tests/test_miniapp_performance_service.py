import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_admin_performance_payload, build_performance_center_payload


class MiniAppPerformanceServiceTests(unittest.TestCase):
    def test_performance_center_payload_exposes_public_operational_summary_only(self):
        user = {'user_id': 10, 'plan': 'plus'}
        snapshot = {
            'summary_7d': {'resolved': 5, 'filled_total': 6, 'winrate': 60.0, 'profit_factor': 1.8, 'expectancy_r': 0.25, 'tp1': 1, 'tp2': 2, 'sl': 2, 'expired': 1, 'expired_no_fill': 1, 'expired_after_entry': 0, 'fill_rate': 85.71, 'no_fill_rate': 14.29, 'post_fill_expiry_rate': 0.0, 'after_entry_failure_rate': 0.0},
            'summary_30d': {'resolved': 12, 'filled_total': 14, 'winrate': 58.33, 'profit_factor': 1.5, 'expectancy_r': 0.18, 'tp1': 3, 'tp2': 4, 'sl': 5, 'expired': 2, 'expired_no_fill': 1, 'expired_after_entry': 1, 'fill_rate': 87.5, 'no_fill_rate': 6.25, 'post_fill_expiry_rate': 6.25, 'after_entry_failure_rate': 7.14},
            'activity_7d': {'signals_total': 9, 'avg_score': 87.2},
            'activity_30d': {'signals_total': 24, 'avg_score': 84.6},
            'by_plan_30d': {
                'free': {'resolved': 2, 'filled_total': 3, 'winrate': 50.0, 'profit_factor': 1.0, 'expectancy_r': 0.0, 'expired_no_fill': 1, 'expired_after_entry': 0},
                'plus': {'resolved': 5, 'filled_total': 6, 'winrate': 60.0, 'profit_factor': 1.5, 'expectancy_r': 0.2, 'expired_no_fill': 0, 'expired_after_entry': 1},
                'premium': {'resolved': 5, 'filled_total': 5, 'winrate': 60.0, 'profit_factor': float('inf'), 'expectancy_r': 0.4, 'expired_no_fill': 0, 'expired_after_entry': 0},
            },
            'activity_by_plan_30d': {
                'free': {'signals_total': 4, 'avg_score': 80.0},
                'plus': {'signals_total': 8, 'avg_score': 84.5},
                'premium': {'signals_total': 12, 'avg_score': 89.1},
            },
            'direction_30d': [{'direction': 'LONG', 'resolved': 7, 'won': 4, 'lost': 3, 'expired': 1, 'expired_no_fill': 1, 'expired_after_entry': 0, 'winrate': 57.14, 'profit_factor': 1.2, 'expectancy_r': 0.1}],
            'strategy_30d': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'primary_send_mode': 'market_on_close', 'primary_send_mode_label': 'Entrada al envío', 'signals_total': 11, 'avg_score': 88.2, 'resolved': 6, 'won': 4, 'lost': 2, 'expired': 1, 'expired_no_fill': 0, 'expired_after_entry': 1, 'fill_rate': 85.71, 'winrate': 66.67, 'profit_factor': 2.0, 'expectancy_r': 0.5, 'tp1': 2, 'tp2': 2, 'sl': 2}],
            'strategy_direction_30d': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'direction': 'LONG', 'resolved': 4, 'won': 3, 'lost': 1, 'expired': 1, 'expired_no_fill': 0, 'expired_after_entry': 1, 'winrate': 75.0, 'profit_factor': 3.0, 'expectancy_r': 0.75}],
            'setup_groups_30d': [{'setup_group': 'breakout', 'resolved': 6, 'won': 4, 'lost': 2, 'expired': 1, 'expired_no_fill': 0, 'expired_after_entry': 1, 'winrate': 66.67, 'profit_factor': 2.0, 'expectancy_r': 0.5}],
            'worst_symbols_30d': [{'symbol': 'BTCUSDT', 'resolved': 3, 'won': 1, 'lost': 2, 'expired': 0, 'expired_no_fill': 0, 'expired_after_entry': 0, 'winrate': 33.33, 'loss_rate': 66.67, 'profit_factor': 0.5, 'expectancy_r': -0.33}],
            'by_score_30d': {'buckets': [{'label': '90+', 'n': 4, 'won': 3, 'lost': 1, 'winrate': 75.0, 'net_r': 2.0}]},
            'diagnostics_30d': {'pending_to_evaluate': 3, 'profit_factor': float('inf'), 'avg_result_score': 86.3, 'expired_no_fill': 1, 'expired_after_entry': 1, 'fill_rate': 87.5, 'after_entry_failure_rate': 7.14},
            'materialized_7d': True,
            'materialized_30d': True,
        }
        total_payload = {
            'summary': {'resolved': 30, 'filled_total': 33, 'winrate': 63.33, 'profit_factor': 1.9, 'expectancy_r': 0.27, 'tp1': 7, 'tp2': 10, 'sl': 13, 'expired': 4, 'expired_no_fill': 2, 'expired_after_entry': 1},
            'activity': {'signals_total': 60, 'avg_score': 83.1},
            'computed_for_range': {'from': None, 'to': None},
        }

        with patch('app.miniapp.service.plan_status', return_value={'plan': 'plus'}), \
             patch('app.miniapp.service.get_performance_snapshot', return_value=snapshot), \
             patch('app.miniapp.service.get_materialized_window', return_value=None), \
             patch('app.miniapp.service.build_performance_window', return_value=total_payload):
            payload = build_performance_center_payload(user, focus_days=3650)

        self.assertEqual(payload['overview']['focus_days'], 3650)
        self.assertEqual(payload['windows'][0]['summary']['expired_no_fill'], 1)
        self.assertEqual(payload['windows'][1]['summary']['expired_after_entry'], 1)
        self.assertEqual(payload['focus']['summary']['filled_total'], 33)
        self.assertEqual(payload['focus']['label'], 'Total')
        self.assertEqual(len(payload['windows']), 3)
        self.assertNotIn('plan_breakdown_30d', payload)
        self.assertNotIn('strategy_30d', payload)
        self.assertNotIn('direction_30d', payload)
        self.assertNotIn('diagnostics_30d', payload)

    def test_admin_performance_payload_exposes_strategy_and_internal_breakdowns(self):
        snapshot = {
            'summary_7d': {'resolved': 5, 'filled_total': 6, 'winrate': 60.0, 'profit_factor': 1.8, 'expectancy_r': 0.25, 'tp1': 1, 'tp2': 2, 'sl': 2, 'expired': 1, 'expired_no_fill': 1, 'expired_after_entry': 0, 'fill_rate': 85.71, 'no_fill_rate': 14.29, 'post_fill_expiry_rate': 0.0, 'after_entry_failure_rate': 0.0},
            'summary_30d': {'resolved': 12, 'filled_total': 14, 'winrate': 58.33, 'profit_factor': 1.5, 'expectancy_r': 0.18, 'tp1': 3, 'tp2': 4, 'sl': 5, 'expired': 2, 'expired_no_fill': 1, 'expired_after_entry': 1, 'fill_rate': 87.5, 'no_fill_rate': 6.25, 'post_fill_expiry_rate': 6.25, 'after_entry_failure_rate': 7.14},
            'activity_7d': {'signals_total': 9, 'avg_score': 87.2},
            'activity_30d': {'signals_total': 24, 'avg_score': 84.6},
            'by_plan_30d': {
                'free': {'resolved': 2, 'filled_total': 3, 'winrate': 50.0, 'profit_factor': 1.0, 'expectancy_r': 0.0, 'expired_no_fill': 1, 'expired_after_entry': 0},
                'plus': {'resolved': 5, 'filled_total': 6, 'winrate': 60.0, 'profit_factor': 1.5, 'expectancy_r': 0.2, 'expired_no_fill': 0, 'expired_after_entry': 1},
                'premium': {'resolved': 5, 'filled_total': 5, 'winrate': 60.0, 'profit_factor': float('inf'), 'expectancy_r': 0.4, 'expired_no_fill': 0, 'expired_after_entry': 0},
            },
            'activity_by_plan_30d': {
                'free': {'signals_total': 4, 'avg_score': 80.0},
                'plus': {'signals_total': 8, 'avg_score': 84.5},
                'premium': {'signals_total': 12, 'avg_score': 89.1},
            },
            'direction_30d': [{'direction': 'LONG', 'resolved': 7, 'won': 4, 'lost': 3, 'expired': 1, 'expired_no_fill': 1, 'expired_after_entry': 0, 'winrate': 57.14, 'profit_factor': 1.2, 'expectancy_r': 0.1}],
            'strategy_30d': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'primary_send_mode': 'market_on_close', 'primary_send_mode_label': 'Entrada al envío', 'signals_total': 11, 'avg_score': 88.2, 'resolved': 6, 'won': 4, 'lost': 2, 'expired': 1, 'expired_no_fill': 0, 'expired_after_entry': 1, 'fill_rate': 85.71, 'winrate': 66.67, 'profit_factor': 2.0, 'expectancy_r': 0.5, 'tp1': 2, 'tp2': 2, 'sl': 2}],
            'strategy_direction_30d': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'direction': 'LONG', 'resolved': 4, 'won': 3, 'lost': 1, 'expired': 1, 'expired_no_fill': 0, 'expired_after_entry': 1, 'winrate': 75.0, 'profit_factor': 3.0, 'expectancy_r': 0.75}],
            'setup_groups_30d': [{'setup_group': 'breakout', 'resolved': 6, 'won': 4, 'lost': 2, 'expired': 1, 'expired_no_fill': 0, 'expired_after_entry': 1, 'winrate': 66.67, 'profit_factor': 2.0, 'expectancy_r': 0.5}],
            'worst_symbols_30d': [{'symbol': 'BTCUSDT', 'resolved': 3, 'won': 1, 'lost': 2, 'expired': 0, 'expired_no_fill': 0, 'expired_after_entry': 0, 'winrate': 33.33, 'loss_rate': 66.67, 'profit_factor': 0.5, 'expectancy_r': -0.33}],
            'by_score_30d': {'buckets': [{'label': '90+', 'n': 4, 'won': 3, 'lost': 1, 'winrate': 75.0, 'net_r': 2.0}]},
            'diagnostics_30d': {'pending_to_evaluate': 3, 'profit_factor': float('inf'), 'avg_result_score': 86.3, 'expired_no_fill': 1, 'expired_after_entry': 1, 'fill_rate': 87.5, 'after_entry_failure_rate': 7.14},
            'materialized_7d': True,
            'materialized_30d': True,
        }
        total_payload = {
            'summary': {'resolved': 30, 'filled_total': 33, 'winrate': 63.33, 'profit_factor': 1.9, 'expectancy_r': 0.27, 'tp1': 7, 'tp2': 10, 'sl': 13, 'expired': 4, 'expired_no_fill': 2, 'expired_after_entry': 1},
            'activity': {'signals_total': 60, 'avg_score': 83.1},
            'computed_for_range': {'from': None, 'to': None},
        }

        strategy_observability = {
            'overview': {'window_days': 30, 'cycles_total': 120, 'attempted_symbols_total': 2400, 'candidate_pool_total': 80, 'selected_signals_total': 40, 'rejected_symbols_total': 2320, 'risk_off_symbols_total': 10, 'failure_symbols_total': 2, 'telemetry_ready': True, 'coverage_started_at': None, 'latest_cycle_at': None},
            'strategy_pipeline': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'attempted_symbols': 1000, 'candidate_pool': 20, 'selected_signals': 8, 'rejected_symbols': 980, 'candidate_rate': 2.0, 'publish_rate': 0.8, 'selection_from_candidates_rate': 40.0}],
            'reject_reasons_by_strategy': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'rejected_symbols': 980, 'top_reasons': [{'reason': 'trend_structure', 'reason_label': 'TREND STRUCTURE', 'count': 400}]}],
            'regime_distribution': [{'regime_state': 'continuation_clean', 'regime_label': 'Continuation clean', 'cycles': 20, 'attempted_symbols': 1000, 'candidate_pool': 20, 'selected_signals': 8}],
            'regime_strategy_matrix': [{'regime_state': 'continuation_clean', 'regime_label': 'Continuation clean', 'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'candidate_pool': 20, 'selected_signals': 8, 'publish_rate': 40.0}],
            'latest_cycle': {'available': True, 'generated_at': None, 'status': 'ok', 'cycle_number': 55, 'attempted_symbols_total': 48, 'candidate_pool_total': 2, 'selected_signals_total': 1, 'rejected_symbols_total': 46, 'risk_off_symbols_total': 0, 'failure_symbols_total': 0, 'market_regime_state': 'continuation_clean', 'market_regime_label': 'Continuation clean', 'market_regime_bias': 'bullish', 'market_regime_reason': 'clean_breakout', 'market_strategy_key': 'breakout_reset', 'market_strategy_label': 'Breakout + Reset', 'attempts_by_strategy': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'count': 48}], 'candidate_pool_by_strategy': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'count': 2}], 'selected_by_strategy': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'count': 1}], 'rejected_by_strategy': [{'strategy_key': 'breakout_reset', 'strategy_label': 'Breakout + Reset', 'count': 46}], 'top_reject_reasons': [{'reason': 'trend_structure', 'reason_label': 'TREND STRUCTURE', 'count': 16}]},
        }

        with patch('app.miniapp.service.get_performance_snapshot', return_value=snapshot), \
             patch('app.miniapp.service.get_materialized_window', return_value=None), \
             patch('app.miniapp.service.build_performance_window', return_value=total_payload), \
             patch('app.miniapp.service.build_admin_strategy_observability', return_value=strategy_observability):
            payload = build_admin_performance_payload(focus_days=3650)

        self.assertEqual(payload['focus']['label'], 'Total')
        self.assertEqual(payload['plan_breakdown_30d'][2]['summary']['profit_factor'], None)
        self.assertTrue(payload['plan_breakdown_30d'][2]['summary']['profit_factor_infinite'])
        self.assertEqual(payload['strategy_30d'][0]['strategy_key'], 'breakout_reset')
        self.assertEqual(payload['strategy_30d'][0]['primary_send_mode_label'], 'Entrada al envío')
        self.assertEqual(payload['strategy_direction_30d'][0]['direction'], 'LONG')
        self.assertEqual(payload['weak_symbols_30d'][0]['symbol'], 'BTCUSDT')
        self.assertEqual(payload['score_buckets_30d'][0]['label'], '90+')
        self.assertEqual(payload['diagnostics_30d']['expired_after_entry'], 1)
        self.assertEqual(payload['strategy_observability_30d']['overview']['cycles_total'], 120)
        self.assertEqual(payload['strategy_observability_30d']['strategy_pipeline'][0]['attempted_symbols'], 1000)
        self.assertEqual(payload['strategy_observability_30d']['latest_cycle']['market_strategy_key'], 'breakout_reset')


if __name__ == '__main__':
    unittest.main()
