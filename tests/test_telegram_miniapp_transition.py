import tests._bootstrap
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class _FakeWebAppInfo:
    def __init__(self, url):
        self.url = url


class _FakeInlineKeyboardButton:
    def __init__(self, text, callback_data=None, url=None, web_app=None):
        self.text = text
        self.callback_data = callback_data
        self.url = url
        self.web_app = web_app


class _FakeInlineKeyboardMarkup:
    def __init__(self, inline_keyboard):
        self.inline_keyboard = inline_keyboard


class _FakeCallbackQueryHandler:
    def __init__(self, callback, pattern=None):
        self.callback = callback
        self.pattern = pattern


class _FakeCommandHandler:
    def __init__(self, command, callback):
        self.command = command
        self.callback = callback


class _FakeMessageHandler:
    def __init__(self, filters_value, callback):
        self.filters = filters_value
        self.callback = callback


telegram_stub = types.ModuleType('telegram')
telegram_stub.InlineKeyboardButton = _FakeInlineKeyboardButton
telegram_stub.InlineKeyboardMarkup = _FakeInlineKeyboardMarkup
telegram_stub.WebAppInfo = _FakeWebAppInfo

telegram_ext_stub = types.ModuleType('telegram.ext')
telegram_ext_stub.CallbackQueryHandler = _FakeCallbackQueryHandler
telegram_ext_stub.CommandHandler = _FakeCommandHandler
telegram_ext_stub.MessageHandler = _FakeMessageHandler
telegram_ext_stub.filters = SimpleNamespace(TEXT=1, COMMAND=2)
telegram_ext_stub.ContextTypes = SimpleNamespace(DEFAULT_TYPE=object)

sys.modules.setdefault('telegram', telegram_stub)
sys.modules.setdefault('telegram.ext', telegram_ext_stub)

from app.menus import get_menu_text, main_menu
from app.telegram_handlers.onboarding import handle_onboarding_callback
from app.telegram_handlers.text_router import handle_text_messages
from app.handlers import handle_menu


class TelegramMiniAppTransitionTests(unittest.IsolatedAsyncioTestCase):
    def test_main_menu_exposes_single_miniapp_button(self):
        with patch('app.menus.get_mini_app_url', return_value='https://hades.example.com/miniapp'):
            markup = main_menu(language='es', is_admin=False)

        self.assertEqual(len(markup.inline_keyboard), 1)
        self.assertEqual(len(markup.inline_keyboard[0]), 1)
        button = markup.inline_keyboard[0][0]
        self.assertEqual(button.text, '🚀 Abrir MiniApp')
        self.assertEqual(button.web_app.url, 'https://hades.example.com/miniapp')

    def test_get_menu_text_mentions_miniapp_operating_model(self):
        text = get_menu_text('es', is_admin=False)
        self.assertIn('MiniApp', text)
        self.assertIn('Telegram ahora funciona como canal de notificaciones', text)

    async def test_text_messages_redirect_to_miniapp_entry(self):
        message = SimpleNamespace(reply_text=AsyncMock())
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123), message=message)
        context = SimpleNamespace(user_data={})

        with patch('app.telegram_handlers.text_router.users_collection') as mocked_users, \
             patch('app.telegram_handlers.text_router.is_effectively_banned', return_value=False), \
             patch('app.telegram_handlers.text_router.is_admin', return_value=False), \
             patch('app.telegram_handlers.text_router.main_menu', return_value='MENU'):
            mocked_users.return_value.find_one.return_value = {'user_id': 123, 'language': 'es'}
            await handle_text_messages(update, context)

        message.reply_text.assert_awaited_once()
        args, kwargs = message.reply_text.await_args
        self.assertIn('MiniApp', args[0])
        self.assertEqual(kwargs['reply_markup'], 'MENU')

    async def test_legacy_callback_redirects_to_miniapp_entry(self):
        query = SimpleNamespace(
            data='view_signals',
            from_user=SimpleNamespace(id=321),
            answer=AsyncMock(),
            edit_message_text=AsyncMock(),
        )
        update = SimpleNamespace(callback_query=query)
        context = SimpleNamespace()

        with patch('app.handlers.users_collection') as mocked_users, \
             patch('app.handlers.is_effectively_banned', return_value=False), \
             patch('app.handlers.is_admin', return_value=False), \
             patch('app.handlers.main_menu', return_value='MENU'):
            mocked_users.return_value.find_one.return_value = {'user_id': 321, 'language': 'es'}
            await handle_menu(update, context)

        query.answer.assert_awaited_once()
        query.edit_message_text.assert_awaited_once()
        args, kwargs = query.edit_message_text.await_args
        self.assertIn('MiniApp', args[0])
        self.assertEqual(kwargs['reply_markup'], 'MENU')

    async def test_language_callback_finishes_onboarding_and_redirects_to_entry(self):
        query = SimpleNamespace(edit_message_text=AsyncMock())
        user = {'user_id': 99, 'language': 'es'}

        with patch('app.telegram_handlers.onboarding.users_collection') as mocked_users, \
             patch('app.telegram_handlers.onboarding.main_menu', return_value='MENU'):
            result = await handle_onboarding_callback(query, user, 'lang:en', admin=False)

        self.assertTrue(result)
        mocked_users.return_value.update_one.assert_called_once()
        query.edit_message_text.assert_awaited_once()
        args, kwargs = query.edit_message_text.await_args
        self.assertIn('MiniApp', args[0])
        self.assertEqual(kwargs['reply_markup'], 'MENU')


if __name__ == '__main__':
    unittest.main()
