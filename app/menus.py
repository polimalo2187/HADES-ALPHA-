from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def normalize_language(value: str | None) -> str:
    value = (value or "").lower()
    if value.startswith("en"):
        return "en"
    return "es"


TEXTS = {
    "es": {
        "menu_text": "🏠 MENÚ PRINCIPAL | HADES ALPHA V2\n\nSeñales, análisis, rendimiento, mercado y herramientas del bot. Selecciona una opción abajo:",
        "admin_text": "🛠 PANEL ADMIN | HADES ALPHA V2\n\nSelecciona una opción abajo:",
        "back_menu": "⬅️ Volver al menú",
        "admin_back": "⬅️ Volver",
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
        "admin_activate_plus": "➕ Activar plan PLUS",
        "admin_activate_premium": "👑 Activar plan PREMIUM",
        "admin_extend_plan": "⏳ Extender plan actual",
        "admin_stats": "📊 Estadísticas",
    },
    "en": {
        "menu_text": "🏠 MAIN MENU | HADES ALPHA V2\n\nSignals, analysis, performance, market, and bot tools. Select an option below:",
        "admin_text": "🛠 ADMIN PANEL | HADES ALPHA V2\n\nSelect an option below:",
        "back_menu": "⬅️ Back to menu",
        "admin_back": "⬅️ Back",
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
        "admin_activate_plus": "➕ Activate PLUS plan",
        "admin_activate_premium": "👑 Activate PREMIUM plan",
        "admin_extend_plan": "⏳ Extend current plan",
        "admin_stats": "📊 Statistics",
    },
}


def get_menu_text(language: str = "es", is_admin: bool = False) -> str:
    lang = normalize_language(language)
    return TEXTS[lang]["admin_text" if is_admin else "menu_text"]


def main_menu(is_admin: bool = False, language: str = "es") -> InlineKeyboardMarkup:
    lang = normalize_language(language)
    t = TEXTS[lang]
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
        keyboard.append([InlineKeyboardButton(t["admin_panel"], callback_data="admin_panel")])

    return InlineKeyboardMarkup(keyboard)


def back_to_menu(language: str = "es") -> InlineKeyboardMarkup:
    lang = normalize_language(language)
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(TEXTS[lang]["back_menu"], callback_data="back_menu")]]
    )


def admin_menu(language: str = "es") -> InlineKeyboardMarkup:
    lang = normalize_language(language)
    t = TEXTS[lang]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t["admin_activate_plus"], callback_data="admin_activate_plus")],
        [InlineKeyboardButton(t["admin_activate_premium"], callback_data="admin_activate_premium")],
        [InlineKeyboardButton(t["admin_extend_plan"], callback_data="admin_extend_plan")],
        [InlineKeyboardButton(t["admin_stats"], callback_data="admin_stats")],
        [InlineKeyboardButton(t["admin_back"], callback_data="back_menu")],
    ])
