import logging

from telegram.ext import ContextTypes

from app.config import is_admin
from app.menus import get_menu_text, main_menu
from app.user_service import get_or_create_user
from app.services.admin_service import is_effectively_banned
from app.telegram_handlers.common import (
    _banned_message,
    _get_user_language,
    _needs_onboarding,
    _show_language_selector_message,
    parse_ref_code,
)

logger = logging.getLogger(__name__)


async def handle_start(update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja /start sobre un único contrato de usuario."""
    try:
        user_id = update.effective_user.id
        telegram_user = update.effective_user
        message = update.effective_message
        start_param = context.args[0] if context.args else None
        referrer_id = parse_ref_code(start_param)

        user, is_new_user = get_or_create_user(
            user_id=user_id,
            username=getattr(telegram_user, "username", None),
            telegram_language=getattr(telegram_user, "language_code", None),
            referred_by=referrer_id,
        )

        language = _get_user_language(user)

        if is_effectively_banned(user):
            await message.reply_text(_banned_message(language))
            return

        if is_new_user or _needs_onboarding(user):
            await _show_language_selector_message(message, user_id=user_id)
            return

        await message.reply_text(
            get_menu_text(language, is_admin=is_admin(user_id)),
            reply_markup=main_menu(language=language, is_admin=is_admin(user_id)),
        )

    except Exception as e:
        logger.error(f"Error en handle_start: {e}", exc_info=True)
        if update.effective_message is not None:
            await update.effective_message.reply_text("❌ Error starting the bot.")
