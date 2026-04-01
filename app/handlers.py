import logging

from telegram.ext import CallbackQueryHandler, CommandHandler, MessageHandler, filters

from app.config import is_admin
from app.database import users_collection
from app.menus import get_menu_text, main_menu
from app.telegram_handlers.common import _banned_message, _get_user_language, _tr
from app.services.admin_service import is_effectively_banned
from app.telegram_handlers.onboarding import handle_onboarding_callback
from app.telegram_handlers.referrals import handle_copy_ref_code
from app.telegram_handlers.start import handle_start
from app.telegram_handlers.text_router import handle_text_messages
from app.observability import record_audit_event, log_event

logger = logging.getLogger(__name__)


async def _show_miniapp_entry(query, language: str, admin: bool) -> None:
    text = get_menu_text(language, is_admin=admin)
    markup = main_menu(language=language, is_admin=admin)
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            return
        raise


async def handle_menu(update, context):
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    user = None
    try:
        user_id = query.from_user.id
        users_col = users_collection()
        user = users_col.find_one({"user_id": user_id})

        if user and is_effectively_banned(user):
            await query.edit_message_text(_banned_message(_get_user_language(user)))
            return

        if not user:
            await query.edit_message_text(
                _tr(_get_user_language(user), "Usuario no encontrado. Usa /start nuevamente.", "User not found. Use /start again."),
                reply_markup=main_menu(language=_get_user_language(user)),
            )
            return

        action = query.data or ""
        admin = is_admin(user_id)

        if await handle_onboarding_callback(query, user, action, admin):
            return

        log_event(
            "telegram.legacy_ui_redirect",
            user_id=user_id,
            callback=action,
            is_admin=admin,
        )
        await _show_miniapp_entry(query, _get_user_language(user), admin)

    except Exception as e:
        log_event("telegram.handle_menu_error", level=logging.ERROR, callback=getattr(query, "data", None), user_id=getattr(getattr(query, "from_user", None), "id", None), error=str(e))
        record_audit_event(event_type="handle_menu_error", status="error", module="handlers", user_id=getattr(getattr(query, "from_user", None), "id", None), callback=getattr(query, "data", None), message=str(e))
        logger.error(f"Error en handle_menu: {e}", exc_info=True)
        await _show_miniapp_entry(query, _get_user_language(user), is_admin(getattr(getattr(query, "from_user", None), "id", 0)))


def get_handlers():
    return [
        CommandHandler("start", handle_start),
        CallbackQueryHandler(handle_copy_ref_code, pattern="^copy_ref_code$"),
        CallbackQueryHandler(handle_menu),
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_messages),
    ]
