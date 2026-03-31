import tests._bootstrap
import unittest
from pathlib import Path


class MiniAppFrontendHelpersTests(unittest.TestCase):
    def test_expected_frontend_helpers_are_defined_in_app_js(self):
        app_js = Path(__file__).resolve().parents[1] / 'app' / 'miniapp' / 'static' / 'app.js'
        text = app_js.read_text(encoding='utf-8')
        for name in [
            'accountMetricCard',
            'billingFocusCard',
            'paymentConfigDiagnosticsCard',
            'recentOrderItem',
            'referralRewardItem',
            'accountTimelineItem',
            'detailInfoChip',
            'detailStatCard',
            'scoreListsEqual',
            'renderScoreBreakdown',
            'renderRadarDetailModal',
            'openRadarDetail',
            'closeSignalDetailModal',
            'renderSignalDetailModal',
            'openSignalDetail',
            'ensurePayloadShell',
            'applyPaymentOrderPreview',
            'refreshAccountState',
            'focusPaymentCard',
        ]:
            self.assertIn(f'function {name}(', text)

    def test_payment_card_is_rendered_before_plan_blocks_when_active_order_exists(self):
        app_js = Path(__file__).resolve().parents[1] / 'app' / 'miniapp' / 'static' / 'app.js'
        text = app_js.read_text(encoding='utf-8')
        payment_idx = text.index("${paymentInstructions(activeOrder, billingFocus) || '<div class=\"card card-span-12\"><h2>Pago actual</h2><div class=\"empty-state\">No tienes una orden de pago pendiente.</div></div>'}")
        plus_idx = text.index("${planBlock('plus', plans.plus || [], me.plan, billing, { hidden: isPremiumActive })}")
        premium_idx = text.index("${planBlock('premium', plans.premium || [], me.plan, billing)}")
        self.assertLess(payment_idx, plus_idx)
        self.assertLess(payment_idx, premium_idx)


if __name__ == '__main__':
    unittest.main()
