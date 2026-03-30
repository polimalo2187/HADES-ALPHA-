import tests._bootstrap
import os
import unittest
from unittest.mock import patch

from app.config import get_mini_app_cors_origins, get_runtime_role, is_mini_app_dev_auth_enabled


class MiniAppConfigTests(unittest.TestCase):
    def test_dev_auth_requires_explicit_flag_and_non_production_env(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "development", "MINI_APP_ALLOW_DEV_AUTH": "true"}, clear=False):
            self.assertTrue(is_mini_app_dev_auth_enabled())

        with patch.dict(os.environ, {"ENVIRONMENT": "production", "MINI_APP_ALLOW_DEV_AUTH": "true"}, clear=False):
            self.assertFalse(is_mini_app_dev_auth_enabled())

        with patch.dict(os.environ, {"ENVIRONMENT": "development", "MINI_APP_ALLOW_DEV_AUTH": "false"}, clear=False):
            self.assertFalse(is_mini_app_dev_auth_enabled())

    def test_cors_origins_fall_back_to_mini_app_url(self):
        env = {
            "MINI_APP_CORS_ORIGINS": "",
            "MINI_APP_URL": "https://hades.example.com/miniapp",
            "MINI_APP_ALLOW_DEV_AUTH": "false",
            "ENVIRONMENT": "production",
        }
        with patch.dict(os.environ, env, clear=False):
            self.assertEqual(get_mini_app_cors_origins(), ["https://hades.example.com"])

    def test_runtime_role_defaults_and_aliases(self):
        with patch.dict(os.environ, {"APP_RUNTIME_ROLE": "scheduler"}, clear=False):
            self.assertEqual(get_runtime_role(), "scheduler")

        with patch.dict(os.environ, {"APP_RUNTIME_ROLE": "scanner"}, clear=False):
            self.assertEqual(get_runtime_role(), "signal_worker")

        with patch.dict(os.environ, {"APP_RUNTIME_ROLE": "", "ENABLE_MINI_APP_SERVER": "true"}, clear=False):
            self.assertEqual(get_runtime_role(), "web")


if __name__ == '__main__':
    unittest.main()
