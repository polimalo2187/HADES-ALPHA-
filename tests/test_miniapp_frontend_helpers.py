import tests._bootstrap
import unittest
from pathlib import Path


class MiniAppFrontendHelpersTests(unittest.TestCase):
    def test_account_helpers_are_defined_in_app_js(self):
        app_js = Path(__file__).resolve().parents[1] / 'app' / 'miniapp' / 'static' / 'app.js'
        text = app_js.read_text(encoding='utf-8')
        for name in [
            'accountMetricCard',
            'billingFocusCard',
            'paymentConfigDiagnosticsCard',
            'recentOrderItem',
            'referralRewardItem',
            'accountTimelineItem',
        ]:
            self.assertIn(f'function {name}(', text)


if __name__ == '__main__':
    unittest.main()
