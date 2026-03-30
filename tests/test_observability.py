import tests._bootstrap
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from app.observability import assess_component, build_health_report, compact_context


class ObservabilityTests(unittest.TestCase):
    def test_compact_context_serializes_nested_values(self):
        payload = compact_context(
            dt=datetime(2026, 1, 1, 0, 0, 0),
            data={'a': 1, 'b': [1, 2]},
            ignore=None,
        )
        self.assertEqual(payload['dt'], '2026-01-01T00:00:00')
        self.assertEqual(payload['data']['a'], 1)
        self.assertNotIn('ignore', payload)

    def test_assess_component_marks_stale_when_heartbeat_is_old(self):
        now = datetime(2026, 3, 30, 12, 0, 0, tzinfo=UTC)
        row = {
            'status': 'ok',
            'updated_at': now - timedelta(seconds=500),
            'details': {'stage': 'running'},
        }

        with patch('app.observability.get_component_stale_after_seconds', return_value=180):
            report = assess_component('miniapp', row, now=now)

        self.assertEqual(report['status'], 'ok')
        self.assertEqual(report['effective_status'], 'stale')
        self.assertTrue(report['is_stale'])
        self.assertEqual(report['age_seconds'], 500)

    def test_build_health_report_flags_missing_required_component(self):
        now = datetime(2026, 3, 30, 12, 0, 0, tzinfo=UTC)
        snapshot = {
            'miniapp': {
                'status': 'ok',
                'updated_at': now,
                'details': {},
            }
        }

        with patch('app.observability.get_health_snapshot', return_value=snapshot):
            report = build_health_report(role='web', required_components=['miniapp', 'database'])

        self.assertFalse(report['ok'])
        self.assertEqual(report['overall_status'], 'degraded')
        self.assertIn('database', report['missing_required_components'])
        self.assertEqual(report['components']['database']['status'], 'missing')


if __name__ == '__main__':
    unittest.main()
