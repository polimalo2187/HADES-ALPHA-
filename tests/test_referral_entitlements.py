import tests._bootstrap
import unittest
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from app.models import new_user, utcnow
from app.plans import PLAN_PLUS, PLAN_PREMIUM, activate_plus, expire_plans
from app.referrals import register_valid_referral


class FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, field, direction):
        reverse = direction == -1
        self._docs.sort(key=lambda item: item.get(field), reverse=reverse)
        return self

    def limit(self, n):
        self._docs = self._docs[: int(n)]
        return self

    def __iter__(self):
        return iter(self._docs)


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = [deepcopy(doc) for doc in (docs or [])]

    def _matches(self, doc, criteria):
        if not criteria:
            return True
        for key, expected in criteria.items():
            if key == "$or":
                return any(self._matches(doc, item) for item in expected)
            value = doc.get(key)
            if isinstance(expected, dict):
                for op, op_value in expected.items():
                    if op == "$lt":
                        if value is None or not (value < op_value):
                            return False
                    elif op == "$gt":
                        if value is None or not (value > op_value):
                            return False
                    elif op == "$ne":
                        if value == op_value:
                            return False
                    else:
                        raise AssertionError(f"Unsupported operator: {op}")
            else:
                if value != expected:
                    return False
        return True

    def find_one(self, criteria, projection=None):
        for doc in self.docs:
            if self._matches(doc, criteria):
                if projection:
                    out = {key: doc.get(key) for key, include in projection.items() if include and key in doc}
                    return deepcopy(out)
                return deepcopy(doc)
        return None

    def insert_one(self, doc):
        self.docs.append(deepcopy(doc))
        return SimpleNamespace(inserted_id=len(self.docs))

    def update_one(self, criteria, update, upsert=False):
        for index, doc in enumerate(self.docs):
            if self._matches(doc, criteria):
                self._apply_update(doc, update)
                self.docs[index] = doc
                return SimpleNamespace(modified_count=1)
        if upsert:
            new_doc = dict(criteria)
            self._apply_update(new_doc, update)
            self.docs.append(new_doc)
            return SimpleNamespace(modified_count=1)
        return SimpleNamespace(modified_count=0)

    def count_documents(self, criteria):
        return sum(1 for doc in self.docs if self._matches(doc, criteria))

    def find(self, criteria=None, projection=None):
        matched = []
        for doc in self.docs:
            if self._matches(doc, criteria or {}):
                if projection:
                    out = {key: doc.get(key) for key, include in projection.items() if include and key in doc}
                    matched.append(deepcopy(out))
                else:
                    matched.append(deepcopy(doc))
        return FakeCursor(matched)

    def distinct(self, field, criteria=None):
        values = []
        for doc in self.docs:
            if self._matches(doc, criteria or {}):
                value = doc.get(field)
                if value not in values:
                    values.append(value)
        return values

    @staticmethod
    def _apply_update(doc, update):
        for key, payload in update.items():
            if key == "$set":
                for field, value in payload.items():
                    doc[field] = value
            elif key == "$inc":
                for field, value in payload.items():
                    doc[field] = doc.get(field, 0) + value
            elif key == "$unset":
                for field in payload.keys():
                    doc.pop(field, None)
            else:
                raise AssertionError(f"Unsupported update operator: {key}")


class ReferralEntitlementTests(unittest.TestCase):
    def _patch_collections(self, users, referrals, events):
        return patch.multiple(
            'app.plans',
            users_collection=lambda: users,
            subscription_events_collection=lambda: events,
        ), patch.multiple(
            'app.referrals',
            users_collection=lambda: users,
            referrals_collection=lambda: referrals,
        )

    def test_plus_referrer_upgrades_to_premium_and_restores_plus_after_expiry(self):
        now = utcnow()
        referrer = new_user(1, 'referrer')
        referrer['plan'] = PLAN_PLUS
        referrer['trial_end'] = None
        referrer['plan_end'] = now + timedelta(days=20)
        referrer['subscription_status'] = 'active'
        referred = new_user(2, 'buyer', referred_by=1)

        users = FakeCollection([referrer, referred])
        referrals = FakeCollection([])
        events = FakeCollection([])

        plans_patch, refs_patch = self._patch_collections(users, referrals, events)
        with plans_patch, refs_patch:
            applied = register_valid_referral(2, PLAN_PREMIUM, purchased_days=30, purchase_key='ord-001')
            self.assertTrue(applied)

            updated_referrer = users.find_one({'user_id': 1})
            self.assertEqual(updated_referrer['plan'], PLAN_PREMIUM)
            self.assertGreaterEqual(int(updated_referrer.get('queued_plus_seconds') or 0), 20 * 24 * 60 * 60 - 5)

            updated_referrer['plan_end'] = utcnow() - timedelta(seconds=1)
            users.update_one({'user_id': 1}, {'$set': updated_referrer})
            processed = expire_plans()
            self.assertEqual(processed, 1)

            restored = users.find_one({'user_id': 1})
            self.assertEqual(restored['plan'], PLAN_PLUS)
            self.assertEqual(int(restored.get('queued_plus_seconds') or 0), 0)
            self.assertGreater(restored['plan_end'], utcnow())

    def test_premium_referrer_stores_plus_reward_for_later(self):
        now = utcnow()
        referrer = new_user(10, 'premium-ref')
        referrer['plan'] = PLAN_PREMIUM
        referrer['trial_end'] = None
        referrer['plan_end'] = now + timedelta(days=9)
        referrer['subscription_status'] = 'active'
        referred = new_user(20, 'buyer', referred_by=10)

        users = FakeCollection([referrer, referred])
        referrals = FakeCollection([])
        events = FakeCollection([])

        plans_patch, refs_patch = self._patch_collections(users, referrals, events)
        with plans_patch, refs_patch:
            applied = register_valid_referral(20, PLAN_PLUS, purchased_days=15, purchase_key='ord-002')
            self.assertTrue(applied)
            updated_referrer = users.find_one({'user_id': 10})
            self.assertEqual(updated_referrer['plan'], PLAN_PREMIUM)
            self.assertGreaterEqual(int(updated_referrer.get('queued_plus_seconds') or 0), 7 * 24 * 60 * 60 - 5)

    def test_same_referred_can_generate_multiple_rewards_with_distinct_purchase_keys(self):
        referrer = new_user(100, 'referrer')
        referred = new_user(200, 'buyer', referred_by=100)
        users = FakeCollection([referrer, referred])
        referrals = FakeCollection([])
        events = FakeCollection([])

        plans_patch, refs_patch = self._patch_collections(users, referrals, events)
        with plans_patch, refs_patch:
            self.assertTrue(register_valid_referral(200, PLAN_PREMIUM, purchased_days=15, purchase_key='ord-100'))
            self.assertTrue(register_valid_referral(200, PLAN_PREMIUM, purchased_days=15, purchase_key='ord-101'))

            updated_referrer = users.find_one({'user_id': 100})
            self.assertEqual(updated_referrer['valid_referrals_total'], 2)
            self.assertEqual(len(referrals.docs), 2)

    def test_duplicate_purchase_key_is_idempotent(self):
        referrer = new_user(300, 'referrer')
        referred = new_user(400, 'buyer', referred_by=300)
        users = FakeCollection([referrer, referred])
        referrals = FakeCollection([])
        events = FakeCollection([])

        plans_patch, refs_patch = self._patch_collections(users, referrals, events)
        with plans_patch, refs_patch:
            self.assertTrue(register_valid_referral(400, PLAN_PLUS, purchased_days=30, purchase_key='ord-dup'))
            self.assertFalse(register_valid_referral(400, PLAN_PLUS, purchased_days=30, purchase_key='ord-dup'))
            updated_referrer = users.find_one({'user_id': 300})
            self.assertEqual(updated_referrer['valid_referrals_total'], 1)
            self.assertEqual(len(referrals.docs), 1)

    def test_admin_activate_plus_does_not_trigger_referral_registration(self):
        with patch('app.plans.activate_plan_purchase', return_value=True) as mocked_activate:
            result = activate_plus(777, days=21)
        self.assertTrue(result)
        mocked_activate.assert_called_once_with(777, PLAN_PLUS, days=21, source='admin_manual', trigger_referral=False)


if __name__ == '__main__':
    unittest.main()
