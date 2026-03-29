import tests._bootstrap
import unittest

from app.models import USER_SCHEMA_VERSION
from app.user_service import build_user_patch, ensure_valid_user_id


class UserServiceTests(unittest.TestCase):
    def test_ensure_valid_user_id(self):
        self.assertEqual(ensure_valid_user_id(5), 5)
        with self.assertRaises(ValueError):
            ensure_valid_user_id(0)

    def test_build_user_patch_backfills_critical_fields(self):
        existing = {
            'user_id': 77,
            'username': 'old',
            'language': '',
        }
        patch = build_user_patch(
            existing_user=existing,
            user_id=77,
            username='newname',
            telegram_language='en',
            referred_by=11,
        )
        self.assertEqual(patch['language'], 'en')
        self.assertEqual(patch['username'], 'newname')
        self.assertEqual(patch['referred_by'], 11)
        self.assertEqual(patch['schema_version'], USER_SCHEMA_VERSION)
        self.assertIn('daily_signal_date', patch)

    def test_build_user_patch_requires_existing_user(self):
        with self.assertRaises(ValueError):
            build_user_patch(
                existing_user=None,
                user_id=1,
                username='x',
                telegram_language='es',
                referred_by=None,
            )


if __name__ == '__main__':
    unittest.main()
