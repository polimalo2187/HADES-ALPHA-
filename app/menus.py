from telegram import InlineKeyboardButton, InlineKeyboardMarkup

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
        "activate_plus": "➕ Activar plan PLUS",
        "activate_premium": "👑 Activar plan PREMIUM",
        "extend_plan": "⏳ Extender plan actual",
        "stats": "📊 Estadísticas",
        "back": "⬅️ Volver",
        "risk_menu": "⚙️ Gestión de riesgo",
    },
    "en": {
        "menu_text": (
            "🏠 MAIN MENU | HADES ALPHA V2\n\n"
            "Signals, analysis, performance, market tools, and bot features in one place. "
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
        "activate_plus": "➕ Activate PLUS plan",
        "activate_premium": "👑 Activate PREMIUM plan",
        "extend_plan": "⏳ Extend current plan",
        "stats": "📊 Statistics",
        "back": "⬅️ Back",
        "risk_menu": "⚙️ Risk management",
    },
}


def _lang(lang: str | None) -> str:
    return "en" if str(lang or "").lower().startswith("en") else "es"


def menu_text(lang: str = "es") -> str:
    return TEXTS[_lang(lang)]["menu_text"]


def admin_text(lang: str = "es") -> str:
    return TEXTS[_lang(lang)]["admin_text"]


def main_menu(is_admin: bool = False, lang: str = "es") -> InlineKeyboardMarkup:
    t = TEXTS[_lang(lang)]
    keyboard = [
        [
            InlineKeyboardButton(t["signals"], callback_data="view_signals"),
            InlineKeyboardButton(t["radar"], callback_data="radar"),
            InlineKeyboardButton(t["performance"], callback_data="performance"),
        ],
        [
            InlineKeyboardButton(t["movers"], callback_data="movers"),
            InlineKeyboardButton(t["market"], callback_data="market"),
            InlineKeyboardButton(t["watchlist"], callback_data="watchlist"),
        ],
        [
            InlineKeyboardButton(t["alerts"], callback_data="alerts"),
            InlineKeyboardButton(t["history"], callback_data="history"),
            InlineKeyboardButton(t["plans"], callback_data="plans"),
        ],
        [
            InlineKeyboardButton(t["referrals"], callback_data="referrals"),
            InlineKeyboardButton(t["account"], callback_data="my_account"),
            InlineKeyboardButton(t["support"], callback_data="support"),
        ],
    ]
    if is_admin:
        keyboard.insert(1, [InlineKeyboardButton(t["admin_panel"], callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)


def back_to_menu(lang: str = "es") -> InlineKeyboardMarkup:
    t = TEXTS[_lang(lang)]
    return InlineKeyboardMarkup([[InlineKeyboardButton(t["back_menu"], callback_data="back_menu")]])


def admin_menu(lang: str = "es") -> InlineKeyboardMarkup:
    t = TEXTS[_lang(lang)]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t["activate_plus"], callback_data="admin_activate_plus")],
        [InlineKeyboardButton(t["activate_premium"], callback_data="admin_activate_premium")],
        [InlineKeyboardButton(t["extend_plan"], callback_data="admin_extend_plan")],
        [InlineKeyboardButton(t["stats"], callback_data="admin_stats")],
        [InlineKeyboardButton(t["back"], callback_data="back_menu")],
    ])


def my_account_menu(lang: str = "es") -> InlineKeyboardMarkup:
    t = TEXTS[_lang(lang)]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t["risk_menu"], callback_data="risk_menu")],
        [InlineKeyboardButton(t["back_menu"], callback_data="back_menu")],
    ])
