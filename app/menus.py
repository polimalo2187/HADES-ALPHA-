from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def normalize_language(value: str | None) -> str:
    if not value:
        return "es"
    value = str(value).lower().strip()
    if value.startswith("en"):
        return "en"
    return "es"


TEXTS = {
    "es": {
        "menu_text": (
            "🏠 MENÚ PRINCIPAL | HADES ALPHA V2\n\n"
            "Señales, análisis, rendimiento, mercado y herramientas del bot en un solo lugar. "
            "Selecciona una opción abajo:"
        ),
        "admin_text": "🛠 PANEL ADMIN — Selecciona una opción abajo",
        "signals": "🚨 Señales",
        "radar": "📡 Radar",
        "performance": "🎯 Rendimiento",
        "movers": "🔥 Movers",
        "market": "📊 Mercado",
        "watchlist": "⭐ Watchlist",
        "alerts": "🔔 Alertas",
        "history": "🧾 Historial",
        "plans": "💼 Planes",
        "referrals": "👥 Referidos",
        "account": "👤 Mi cuenta",
        "support": "📩 Soporte",
        "admin_panel": "🛠 Panel Admin",
        "back_menu": "⬅️ Volver al menú",
        "risk_menu": "⚙️ Gestión de riesgo",
        "admin_activate_plus": "➕ Activar plan PLUS",
        "admin_activate_premium": "👑 Activar plan PREMIUM",
        "admin_extend_plan": "⏳ Extender plan actual",
        "admin_stats": "📊 Estadísticas",
        "back": "⬅️ Volver",
    },
    "en": {
        "menu_text": (
            "🏠 MAIN MENU | HADES ALPHA V2\n\n"
            "Signals, analysis, performance, market data, and bot tools in one place. "
            "Select an option below:"
        ),
        "admin_text": "🛠 ADMIN PANEL — Select an option below",
        "signals": "🚨 Signals",
        "radar": "📡 Radar",
        "performance": "🎯 Performance",
        "movers": "🔥 Movers",
        "market": "📊 Market",
        "watchlist": "⭐ Watchlist",
        "alerts": "🔔 Alerts",
        "history": "🧾 History",
        "plans": "💼 Plans",
        "referrals": "👥 Referrals",
        "account": "👤 My account",
        "support": "📩 Support",
        "admin_panel": "🛠 Admin Panel",
        "back_menu": "⬅️ Back to menu",
        "risk_menu": "⚙️ Risk management",
        "admin_activate_plus": "➕ Activate PLUS plan",
        "admin_activate_premium": "👑 Activate PREMIUM plan",
        "admin_extend_plan": "⏳ Extend current plan",
        "admin_stats": "📊 Statistics",
        "back": "⬅️ Back",
    },
}


def _t(language: str | None, key: str) -> str:
    lang = normalize_language(language)
    return TEXTS.get(lang, TEXTS["es"]).get(key, TEXTS["es"][key])



def get_menu_text(language: str | None = "es", is_admin: bool = False) -> str:
    return _t(language, "admin_text" if is_admin else "menu_text")

def main_menu(language: str | None = "es", is_admin: bool = False) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(_t(language, "signals"), callback_data="view_signals"),
            InlineKeyboardButton(_t(language, "radar"), callback_data="radar"),
            InlineKeyboardButton(_t(language, "performance"), callback_data="performance"),
        ],
        [
            InlineKeyboardButton(_t(language, "movers"), callback_data="movers"),
            InlineKeyboardButton(_t(language, "market"), callback_data="market"),
            InlineKeyboardButton(_t(language, "watchlist"), callback_data="watchlist"),
        ],
        [
            InlineKeyboardButton(_t(language, "alerts"), callback_data="alerts"),
            InlineKeyboardButton(_t(language, "history"), callback_data="history"),
            InlineKeyboardButton(_t(language, "plans"), callback_data="plans"),
        ],
        [
            InlineKeyboardButton(_t(language, "referrals"), callback_data="referrals"),
            InlineKeyboardButton(_t(language, "account"), callback_data="my_account"),
            InlineKeyboardButton(_t(language, "support"), callback_data="support"),
        ],
    ]
    if is_admin:
        keyboard.insert(1, [InlineKeyboardButton(_t(language, "admin_panel"), callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)


def back_to_menu(language: str | None = "es") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(_t(language, "back_menu"), callback_data="back_menu")]])


def admin_menu(language: str | None = "es") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_t(language, "admin_activate_plus"), callback_data="admin_activate_plus")],
        [InlineKeyboardButton(_t(language, "admin_activate_premium"), callback_data="admin_activate_premium")],
        [InlineKeyboardButton(_t(language, "admin_extend_plan"), callback_data="admin_extend_plan")],
        [InlineKeyboardButton(_t(language, "admin_stats"), callback_data="admin_stats")],
        [InlineKeyboardButton(_t(language, "back"), callback_data="back_menu")],
    ])


def my_account_menu(language: str | None = "es") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_t(language, "risk_menu"), callback_data="risk_menu")],
        [InlineKeyboardButton(_t(language, "back_menu"), callback_data="back_menu")],
    ])
