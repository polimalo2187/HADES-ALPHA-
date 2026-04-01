import tests._bootstrap
import unittest
from unittest.mock import patch

from app.miniapp.service import build_performance_center_payload


class MiniAppPerformanceServiceTests(unittest.TestCase):
    def test_performance_center_payload_exposes_total_window_and_breakdowns(self):
        user = {'user_id': 10, 'plan': 'plus'}
        snapshot = {
            'summary_7d': {'resolved': 5, 'winrate': 60.0, 'profit_factor': 1.8, 'expectancy_r': 0.25, 'tp1': 1, 'tp2': 2, 'sl': 2, 'expired': 1},
            'summary_30d': {'resolved': 12, 'winrate': 58.33, 'profit_factor': 1.5, 'expectancy_r': 0.18, 'tp1': 3, 'tp2': 4, 'sl': 5, 'expired': 2},
            'activity_7d': {'signals_total': 9, 'avg_score': 87.2},
            'activity_30d': {'signals_total': 24, 'avg_score': 84.6},
            'by_plan_30d': {
                'free': {'resolved': 2, 'winrate': 50.0, 'profit_factor': 1.0, 'expectancy_r': 0.0},
                'plus': {'resolved': 5, 'winrate': 60.0, 'profit_factor': 1.5, 'expectancy_r': 0.2},
                'premium': {'resolved': 5, 'winrate': 60.0, 'profit_factor': float('inf'), 'expectancy_r': 0.4},
            },
            'activity_by_plan_30d': {
                'free': {'signals_total': 4, 'avg_score': 80.0},
                'plus': {'signals_total': 8, 'avg_score': 84.5},
                'premium': {'signals_total': 12, 'avg_score': 89.1},
            },
            'direction_30d': [{'direction': 'LONG', 'resolved': 7, 'won': 4, 'lost': 3, 'expired': 1, 'winrate': 57.14, 'profit_factor': 1.2, 'expectancy_r': 0.1}],
            'setup_groups_30d': [{'setup_group': 'breakout', 'resolved': 6, 'won': 4, 'lost': 2, 'expired': 1, 'winrate': 66.67, 'profit_factor': 2.0, 'expectancy_r': 0.5}],
            'worst_symbols_30d': [{'symbol': 'BTCUSDT', 'resolved': 3, 'won': 1, 'lost': 2, 'expired': 0, 'winrate': 33.33, 'loss_rate': 66.67, 'profit_factor': 0.5, 'expectancy_r': -0.33}],
            'by_score_30d': {'buckets': [{'label': '90+', 'n': 4, 'won': 3, 'lost': 1, 'winrate': 75.0, 'net_r': 2.0}]},
            'diagnostics_30d': {'pending_to_evaluate': 3, 'profit_factor': float('inf'), 'avg_result_score': 86.3},
            'materialized_7d': True,
            'materialized_30d': True,
        }
        total_payload = {
            'summary': {'resolved': 30, 'winrate': 63.33, 'profit_factor': 1.9, 'expectancy_r': 0.27, 'tp1': 7, 'tp2': 10, 'sl': 13, 'expired': 4},
            'activity': {'signals_total': 60, 'avg_score': 83.1},
            'computed_for_range': {'from': None, 'to': None},
        }

        with patch('app.miniapp.service.plan_status', return_value={'plan': 'plus'}), \
             patch('app.miniapp.service.get_performance_snapshot', return_value=snapshot), \
             patch('app.miniapp.service.get_materialized_window', return_value=None), \
             patch('app.miniapp.service.build_performance_window', return_value=total_payload):
            payload = build_performance_center_payload(user, focus_days=3650)

        self.assertEqual(payload['overview']['focus_days'], 3650)
        self.assertEqual(payload['focus']['label'], 'Total')
        self.assertEqual(len(payload['windows']), 3)
        self.assertEqual(payload['plan_breakdown_30d'][2]['summary']['profit_factor'], None)
        self.assertTrue(payload['plan_breakdown_30d'][2]['summary']['profit_factor_infinite'])
        self.assertEqual(payload['weak_symbols_30d'][0]['symbol'], 'BTCUSDT')
        self.assertEqual(payload['score_buckets_30d'][0]['label'], '90+')


if __name__ == '__main__':
    unittest.main()
