import tests._bootstrap
import unittest
from datetime import datetime

from app.observability import compact_context


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


if __name__ == '__main__':
    unittest.main()
