import logging

from app.database import users_collection
from app.telegram_handlers.admin import handle_admin_text_input
from app.telegram_handlers.common import _banned_message, _get_user_language
from app.telegram_handlers.risk import handle_risk_text_input
from app.telegram_handlers.watchlist import handle_exchange_text_input, handle_watchlist_text_input


async def handle_text_messages(update, context):
    """Maneja todos los mensajes de texto, decidiendo el flujo correcto"""
    try:
        user_id = update.effective_user.id
        user = users_collection().find_one({"user_id": user_id})
        if user and user.get("banned"):
            await update.message.reply_text(_banned_message(_get_user_language(user)))
            return
    except Exception:
        pass

    if context.user_data.get("watchlist_active"):
        await handle_watchlist_text_input(update, context)
        return

    if context.user_data.get("awaiting_user_id") or context.user_data.get("awaiting_delete_user_id") or context.user_data.get("awaiting_plan_days"):
        await handle_admin_text_input(update, context)
        return

    if context.user_data.get("awaiting_exchange"):
        await handle_exchange_text_input(update, context)
        return

    if context.user_data.get("awaiting_risk_field"):
        await handle_risk_text_input(update, context)
        return
