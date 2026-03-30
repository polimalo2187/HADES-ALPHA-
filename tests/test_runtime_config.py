import tests._bootstrap
import os
import unittest
import importlib
from unittest.mock import patch

from app.config import get_runtime_configuration_errors, validate_runtime_configuration


class RuntimeConfigTests(unittest.TestCase):
    def test_scheduler_does_not_require_bot_token(self):
        env = {
            "APP_RUNTIME_ROLE": "scheduler",
            "ENVIRONMENT": "production",
            "MONGODB_URI": "mongodb://example",
            "DATABASE_NAME": "hades",
            "BOT_TOKEN": "",
        }
        with patch.dict(os.environ, env, clear=False):
            self.assertEqual(get_runtime_configuration_errors("scheduler"), [])
            validate_runtime_configuration("scheduler")

    def test_web_requires_explicit_session_secret_outside_development(self):
        env = {
            "APP_RUNTIME_ROLE": "web",
            "ENVIRONMENT": "production",
            "ENABLE_MINI_APP_SERVER": "true",
            "BOT_TOKEN": "token",
            "MONGODB_URI": "mongodb://example",
            "DATABASE_NAME": "hades",
            "MINI_APP_URL": "https://hades.example.com/miniapp",
            "MINI_APP_SESSION_SECRET": "",
        }
        with patch.dict(os.environ, env, clear=False):
            errors = get_runtime_configuration_errors("web")
            self.assertTrue(any("MINI_APP_SESSION_SECRET" in error for error in errors))
            with self.assertRaises(RuntimeError):
                validate_runtime_configuration("web")

    def test_scheduler_module_imports_without_bot_token(self):
        env = {
            "BOT_TOKEN": "",
            "MONGODB_URI": "mongodb://example",
            "DATABASE_NAME": "hades",
        }
        with patch.dict(os.environ, env, clear=False):
            module = importlib.import_module("app.scheduler")
            self.assertTrue(hasattr(module, "run_scheduler_worker"))


if __name__ == '__main__':
    unittest.main()
