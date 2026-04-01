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
            'focusPlanBlock',
            'setAccountNotice',
            'accountNoticeCard',
            'setRiskNotice',
            'riskNoticeCard',
            'refreshRiskCenter',
            'openRiskCenter',
            'renderRisk',
            'setPerformanceNotice',
            'performanceNoticeCard',
            'refreshPerformanceCenter',
            'openPerformanceCenter',
            'renderPerformance',
            'setSettingsNotice',
            'settingsNoticeCard',
            'refreshSettingsCenter',
            'openSettingsCenter',
            'renderSettings',
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


    def test_billing_focus_and_account_notice_hooks_are_present(self):
        app_js = Path(__file__).resolve().parents[1] / 'app' / 'miniapp' / 'static' / 'app.js'
        text = app_js.read_text(encoding='utf-8')
        self.assertIn('data-billing-focus-action="open-plans"', text)
        self.assertIn('data-billing-focus-action="focus-order"', text)
        self.assertIn('data-billing-focus-action="refresh-account"', text)
        self.assertIn('${accountNoticeCard(state.accountNotice)}', text)
        self.assertIn('data-plan-block="${escapeHtml(planKey)}"', text)
        self.assertIn('data-open-risk-center="true"', text)
        self.assertIn('data-open-performance-center="true"', text)
        self.assertIn('data-open-settings-center="true"', text)
        self.assertIn('data-open-risk-signal="${escapeHtml(item.signal_id)}"', text)
        self.assertIn('data-radar-rotate', text)
        self.assertIn('function getRadarWindow(', text)
        self.assertIn('function rotateRadarWindow(', text)
        self.assertIn('function resetRadarView(', text)

if __name__ == '__main__':
    unittest.main()
