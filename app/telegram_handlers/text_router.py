import logging

from app.database import users_collection
from app.menus import get_menu_text, main_menu
from app.telegram_handlers.common import _banned_message, _get_user_language
from app.services.admin_service import is_effectively_banned
from app.config import is_admin

logger = logging.getLogger(__name__)


async def handle_text_messages(update, context):
    """Telegram ya funciona como entrada rápida hacia la MiniApp, no como UI operativa."""
    user = None
    try:
        user_id = update.effective_user.id
        user = users_collection().find_one({"user_id": user_id})
        if user and is_effectively_banned(user):
            await update.message.reply_text(_banned_message(_get_user_language(user)))
            return
    except Exception:
        pass

    language = _get_user_language(user)
    await update.message.reply_text(
        get_menu_text(language, is_admin=is_admin(update.effective_user.id)),
        reply_markup=main_menu(language=language, is_admin=is_admin(update.effective_user.id)),
    )
