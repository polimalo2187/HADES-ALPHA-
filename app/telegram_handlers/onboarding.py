from datetime import datetime

from app.database import users_collection
from app.menus import get_menu_text, main_menu
from app.onboarding_ui import ONBOARDING_VERSION, normalize_language
from app.telegram_handlers.common import _get_user_language


async def handle_onboarding_callback(query, user, action: str, admin: bool) -> bool:
    users_col = users_collection()
    user_id = user["user_id"]

    if action in {"lang:es", "lang:en"}:
        lang = normalize_language(action.split(":", 1)[1])
        users_col.update_one(
            {"user_id": user_id},
            {"$set": {"language": lang, "onboarding_seen": True, "onboarding_completed": True, "onboarding_version": ONBOARDING_VERSION, "onboarding_completed_at": datetime.utcnow()}},
        )
        user["language"] = lang
        user["onboarding_seen"] = True
        user["onboarding_completed"] = True
        user["onboarding_version"] = ONBOARDING_VERSION
        try:
            await query.edit_message_text(
                get_menu_text(lang, is_admin=admin),
                reply_markup=main_menu(language=lang, is_admin=admin),
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                raise
        return True

    if not action.startswith("ob:"):
        return False

    screen = action.split(":", 1)[1]
    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"onboarding_seen": True, "onboarding_completed": True, "onboarding_version": ONBOARDING_VERSION, "onboarding_completed_at": datetime.utcnow()}},
    )
    language = _get_user_language(user)
    try:
        await query.edit_message_text(
            get_menu_text(language, is_admin=admin),
            reply_markup=main_menu(language=language, is_admin=admin),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            raise
    return True
