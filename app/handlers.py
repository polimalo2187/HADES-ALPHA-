import logging

from telegram.ext import CallbackQueryHandler, CommandHandler, MessageHandler, filters

from app.config import is_admin
from app.database import users_collection
from app.menus import main_menu
from app.telegram_handlers.admin import handle_admin_callback
from app.telegram_handlers.common import _banned_message, _get_user_language, _tr
from app.telegram_handlers.features import handle_standard_menu_action
from app.telegram_handlers.onboarding import handle_onboarding_callback
from app.telegram_handlers.referrals import handle_copy_ref_code, handle_referrals
from app.telegram_handlers.risk import _handle_risk_dynamic_callbacks
from app.telegram_handlers.start import handle_start
from app.telegram_handlers.text_router import handle_text_messages
from app.telegram_handlers.watchlist import handle_watchlist_callback

logger = logging.getLogger(__name__)


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

        if user and user.get("banned"):
            await query.edit_message_text(_banned_message(_get_user_language(user)))
            return

        if not user:
            await query.edit_message_text(
                _tr(_get_user_language(user), "Usuario no encontrado. Usa /start nuevamente.", "User not found. Use /start again."),
                reply_markup=main_menu(language=_get_user_language(user)),
            )
            return

        action = query.data
        admin = is_admin(user_id)

        if await _handle_risk_dynamic_callbacks(query, context, user, action):
            return

        if await handle_onboarding_callback(query, user, action, admin):
            return

        if await handle_admin_callback(query, context, user, action, admin):
            return

        if action == "referrals":
            await handle_referrals(query, user)
            return

        if await handle_watchlist_callback(query, context, user, action, admin):
            return

        if await handle_standard_menu_action(query, context, user, action, admin):
            return

    except Exception as e:
        logger.error(f"Error en handle_menu: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Ocurrió un error inesperado.",
            reply_markup=main_menu(language=_get_user_language(user)),
        )


def get_handlers():
    return [
        CommandHandler("start", handle_start),
        CallbackQueryHandler(
            handle_menu,
            pattern=r"^(view_signals|radar|radar_refresh|performance|reset_stats|confirm_reset_stats|cancel_reset_stats|movers|market|market_refresh|watchlist|wl_refresh|wl_clear|wl_rm:[A-Z0-9]+|alerts|alerts_refresh|history|history_refresh|plans|my_account|referrals|support|admin_panel|admin_activate_plan|admin_delete_user|confirm_delete_user:[0-9]+|cancel_admin_delete|back_menu|choose_plus_plan|choose_premium_plan|choose_plus_plan_days|choose_premium_plan_days|register_exchange|risk_menu|risk_set_capital|risk_set_risk|risk_set_exchange|risk_set_fee|risk_set_slippage|risk_set_leverage|risk_set_profile|risk_pick_exchange:[a-z]+|risk_pick_profile:[a-z]+|risk_test|sig_detail:[A-Za-z0-9]+|hist_detail:[A-Za-z0-9]+|risk_calc:(live|hist|test):[A-Za-z0-9]+|sig_an:(live|hist|test):[A-Za-z0-9]+|sig_trk:(live|hist|test):[A-Za-z0-9]+|risk_pf:(live|hist|test):[A-Za-z0-9]+|risk_cp:(live|hist|test):[A-Za-z0-9]+:[cma]|lang:(es|en)|ob:[A-Za-z_]+(?::[A-Za-z_]+)?)$"
        ),
        CallbackQueryHandler(handle_copy_ref_code, pattern="^copy_ref_code$"),
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_messages),
    ]
