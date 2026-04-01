from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

from app.config import get_bot_display_name, get_mini_app_url
from app.i18n import normalize_language, tr

SUPPORT_GROUP_URL = "https://chat.whatsapp.com/JXxSGjaKtqRH9c0jTlGv2l?mode=gi_t"


def _t(language: str | None, key: str, **kwargs) -> str:
    kwargs.setdefault("bot_name", get_bot_display_name())
    return tr(normalize_language(language), key, **kwargs)


def get_menu_text(language: str | None = "es", is_admin: bool = False) -> str:
    key = "common.admin_menu_full" if is_admin else "common.main_menu_full"
    return _t(language, key)


def _mini_app_button(language: str | None = "es") -> InlineKeyboardButton | None:
    mini_app_url = get_mini_app_url()
    if not mini_app_url:
        return None
    return InlineKeyboardButton(_t(language, "menu.open_app"), web_app=WebAppInfo(url=mini_app_url))


def main_menu(language: str | None = "es", is_admin: bool = False) -> InlineKeyboardMarkup:
    mini_app_button = _mini_app_button(language)
    keyboard = [[mini_app_button]] if mini_app_button else []
    return InlineKeyboardMarkup(keyboard)


def back_to_menu(language: str | None = "es") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(_t(language, "common.back_menu"), callback_data="back_menu")]])


def admin_menu(language: str | None = "es") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_t(language, "menu.admin_activate_plus"), callback_data="admin_activate_plus")],
        [InlineKeyboardButton(_t(language, "menu.admin_activate_premium"), callback_data="admin_activate_premium")],
        [InlineKeyboardButton(_t(language, "menu.admin_extend_plan"), callback_data="admin_extend_plan")],
        [InlineKeyboardButton(_t(language, "menu.admin_stats"), callback_data="admin_stats")],
        [InlineKeyboardButton(_t(language, "common.back"), callback_data="back_menu")],
    ])


def my_account_menu(language: str | None = "es") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_t(language, "menu.risk_menu"), callback_data="risk_menu")],
        [InlineKeyboardButton(_t(language, "common.language"), callback_data="language_menu")],
        [InlineKeyboardButton(_t(language, "common.back_menu"), callback_data="back_menu")],
    ])
