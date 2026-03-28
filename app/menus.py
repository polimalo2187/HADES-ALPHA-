from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import get_bot_display_name
from app.i18n import normalize_language, tr


def _t(language: str | None, key: str, **kwargs) -> str:
    kwargs.setdefault("bot_name", get_bot_display_name())
    return tr(normalize_language(language), key, **kwargs)



def get_menu_text(language: str | None = "es", is_admin: bool = False) -> str:
    key = "common.admin_menu_full" if is_admin else "common.main_menu_full"
    return _t(language, key)



def main_menu(language: str | None = "es", is_admin: bool = False) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(_t(language, "menu.signals"), callback_data="view_signals"),
            InlineKeyboardButton(_t(language, "menu.radar"), callback_data="radar"),
            InlineKeyboardButton(_t(language, "menu.performance"), callback_data="performance"),
        ],
        [
            InlineKeyboardButton(_t(language, "menu.movers"), callback_data="movers"),
            InlineKeyboardButton(_t(language, "menu.market"), callback_data="market"),
            InlineKeyboardButton(_t(language, "menu.watchlist"), callback_data="watchlist"),
        ],
        [
            InlineKeyboardButton(_t(language, "menu.alerts"), callback_data="alerts"),
            InlineKeyboardButton(_t(language, "menu.history"), callback_data="history"),
            InlineKeyboardButton(_t(language, "menu.plans"), callback_data="plans"),
        ],
        [
            InlineKeyboardButton(_t(language, "menu.referrals"), callback_data="referrals"),
            InlineKeyboardButton(_t(language, "menu.account"), callback_data="my_account"),
            InlineKeyboardButton(_t(language, "menu.support"), callback_data="support"),
        ],
    ]
    if is_admin:
        keyboard.insert(1, [InlineKeyboardButton(_t(language, "menu.admin_panel"), callback_data="admin_panel")])
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
