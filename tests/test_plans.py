import tests._bootstrap
import unittest
from datetime import timedelta

from app.models import new_user, utcnow
from app.plans import (
    PLAN_PLUS,
    PLAN_PREMIUM,
    _apply_entitlement_to_user,
    can_access_feature,
    get_plan_price,
    get_referral_reward_days,
    is_valid_plan_duration,
    validate_plan_duration,
)


class PlanLogicTests(unittest.TestCase):
    def test_price_table_matches_expected_values(self):
        self.assertEqual(get_plan_price(PLAN_PREMIUM, 7), 5.0)
        self.assertEqual(get_plan_price(PLAN_PREMIUM, 15), 10.0)
        self.assertEqual(get_plan_price(PLAN_PREMIUM, 21), 15.0)
        self.assertEqual(get_plan_price(PLAN_PREMIUM, 30), 20.0)

    def test_referral_reward_days(self):
        self.assertEqual(get_referral_reward_days(7), 3)
        self.assertEqual(get_referral_reward_days(15), 7)
        self.assertEqual(get_referral_reward_days(21), 10)
        self.assertEqual(get_referral_reward_days(30), 15)

    def test_validate_plan_duration(self):
        self.assertTrue(is_valid_plan_duration(PLAN_PLUS, 7))
        self.assertEqual(validate_plan_duration(PLAN_PREMIUM, 30), (PLAN_PREMIUM, 30))
        with self.assertRaises(ValueError):
            validate_plan_duration(PLAN_PLUS, 10)

    def test_lower_tier_reward_does_not_downgrade_active_user(self):
        user = new_user(123, 'tester')
        user['plan'] = PLAN_PREMIUM
        user['trial_end'] = None
        user['plan_end'] = utcnow() + timedelta(days=30)
        updated = _apply_entitlement_to_user(user, target_plan=PLAN_PLUS, days=3, source='test', purchase=False)
        self.assertEqual(updated['plan'], PLAN_PREMIUM)

    def test_feature_access_by_plan(self):
        self.assertTrue(can_access_feature(PLAN_PLUS, 'history'))
        self.assertFalse(can_access_feature('free', 'signals_premium'))


if __name__ == '__main__':
    unittest.main()
