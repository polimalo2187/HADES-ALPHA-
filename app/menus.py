from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# Texto de cabecera del menú principal.
MENU_TEXT = (
    "🏠 MENÚ PRINCIPAL | HADES ALPHA V2\n\n"
    "Señales, análisis, rendimiento, mercado y herramientas del bot en un solo lugar. "
    "Selecciona una opción abajo:"
)
ADMIN_TEXT = "🛠 PANEL ADMIN — Selecciona una opción abajo"


def main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
    """Menú principal del bot con layout 4x3 para usuarios."""
    keyboard = [
        [
            InlineKeyboardButton("🚨 Señales", callback_data="view_signals"),
            InlineKeyboardButton("📡 Radar", callback_data="radar"),
            InlineKeyboardButton("🎯 Rendimiento", callback_data="performance"),
        ],
        [
            InlineKeyboardButton("🔥 Movers", callback_data="movers"),
            InlineKeyboardButton("📊 Mercado", callback_data="market"),
            InlineKeyboardButton("⭐ Watchlist", callback_data="watchlist"),
        ],
        [
            InlineKeyboardButton("🔔 Alertas", callback_data="alerts"),
            InlineKeyboardButton("🧾 Historial", callback_data="history"),
            InlineKeyboardButton("💼 Planes", callback_data="plans"),
        ],
        [
            InlineKeyboardButton("👥 Referidos", callback_data="referrals"),
            InlineKeyboardButton("👤 Mi cuenta", callback_data="my_account"),
            InlineKeyboardButton("📩 Soporte", callback_data="support"),
        ],
    ]

    if is_admin:
        keyboard.insert(1, [InlineKeyboardButton("🛠 Panel Admin", callback_data="admin_panel")])

    return InlineKeyboardMarkup(keyboard)


def back_to_menu() -> InlineKeyboardMarkup:
    """Botón para volver al menú principal."""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Volver al menú", callback_data="back_menu")]]
    )


def admin_menu() -> InlineKeyboardMarkup:
    """Menú de administrador."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Activar plan PLUS", callback_data="admin_activate_plus")],
        [InlineKeyboardButton("👑 Activar plan PREMIUM", callback_data="admin_activate_premium")],
        [InlineKeyboardButton("⏳ Extender plan actual", callback_data="admin_extend_plan")],
        [InlineKeyboardButton("📊 Estadísticas", callback_data="admin_stats")],
        [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
    ])



def my_account_menu() -> InlineKeyboardMarkup:
    """Mi cuenta: solo datos del usuario y acceso a gestión de riesgo."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Gestión de riesgo", callback_data="risk_menu")],
        [InlineKeyboardButton("⬅️ Volver al menú", callback_data="back_menu")],
    ])
