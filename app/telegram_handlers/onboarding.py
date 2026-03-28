from datetime import datetime

from app.database import users_collection
from app.menus import main_menu
from app.onboarding_ui import ONBOARDING_VERSION, normalize_language
from app.telegram_handlers.common import _admin_panel_keyboard, _get_user_language, _show_onboarding_screen, _tr


async def handle_onboarding_callback(query, user, action: str, admin: bool) -> bool:
    users_col = users_collection()
    user_id = user["user_id"]

    if action in {"lang:es", "lang:en"}:
        lang = normalize_language(action.split(":", 1)[1])
        users_col.update_one(
            {"user_id": user_id},
            {"$set": {"language": lang, "onboarding_seen": True, "onboarding_completed": False, "onboarding_version": ONBOARDING_VERSION}},
        )
        user["language"] = lang
        user["onboarding_seen"] = True
        user["onboarding_completed"] = False
        user["onboarding_version"] = ONBOARDING_VERSION
        await _show_onboarding_screen(query, user, "home")
        return True

    if not action.startswith("ob:"):
        return False

    screen = action.split(":", 1)[1]
    if screen == "menu":
        users_col.update_one(
            {"user_id": user_id},
            {"$set": {"onboarding_seen": True, "onboarding_completed": True, "onboarding_version": ONBOARDING_VERSION, "onboarding_completed_at": datetime.utcnow()}},
        )
        await query.edit_message_text(
            _tr(_get_user_language(user), "🏠 MENÚ PRINCIPAL — Selecciona una opción abajo", "🏠 MAIN MENU — Select an option below"),
            reply_markup=main_menu(language=_get_user_language(user), is_admin=admin),
        )
        return True

    if screen == "start":
        screen = "how"

    screen_map = {
        "back:home": "home",
        "back:how": "how",
        "back:plans": "plans",
        "home": "home",
        "how": "how",
        "risk": "risk",
        "analysis": "analysis",
        "tracking": "tracking",
        "market": "market",
        "plans": "plans",
        "plus": "plus",
        "premium": "premium",
        "free": "free",
        "guide": "guide",
    }
    resolved = screen_map.get(screen)
    if not resolved:
        await query.answer(_tr(_get_user_language(user), "Pantalla no disponible.", "Screen not available."), show_alert=False)
        return True

    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"onboarding_seen": True, "onboarding_version": ONBOARDING_VERSION}},
    )
    await _show_onboarding_screen(query, user, resolved)
    return True
