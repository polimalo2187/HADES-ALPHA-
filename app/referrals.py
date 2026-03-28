import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.database import users_collection
from app.menus import back_to_menu
from app.referrals import get_referral_link, get_user_referral_stats
from app.telegram_handlers.common import _get_user_language, _tr

logger = logging.getLogger(__name__)


async def handle_referrals(query, user):
    language = _get_user_language(user)
    try:
        user_id = user["user_id"]
        stats = get_user_referral_stats(user_id)
        if not stats:
            await query.edit_message_text(
                _tr(language, "❌ No se pudo cargar la información de referidos.", "❌ Could not load referral information."),
                reply_markup=back_to_menu(language=language),
            )
            return

        ref_link = get_referral_link(user_id)
        valid_total = stats.get("valid_referrals_total")
        if valid_total is None:
            valid_total = stats.get("current_plus", 0) + stats.get("current_premium", 0)
        reward_days_total = stats.get("reward_days_total", valid_total * 7)

        if language == "en":
            message = (
                "👥 REFERRAL SYSTEM\n\n"
                f"🔗 Your referral link:\n{ref_link}\n\n"
                "📊 STATS:\n"
                f"• Total valid referrals: {valid_total}\n"
                f"• PLUS referrals: {stats.get('plus_referred', 0)}\n"
                f"• PREMIUM referrals: {stats.get('premium_referred', 0)}\n\n"
                "🎁 REWARDS:\n"
                f"• Accumulated referral days: +{reward_days_total} days\n"
                "• Each valid referral adds +7 days to your current plan\n\n"
                "📢 HOW TO REFER:\n"
                "1. Share your link\n"
                "2. They join the bot\n"
                "3. They activate a plan\n\n"
                "📌 CURRENT RULE:\n"
                "• Each valid referral adds +7 days to your current plan\n"
                "• Referral type (PLUS/PREMIUM) is shown only as a statistic\n"
            )
        else:
            message = (
                "👥 SISTEMA DE REFERIDOS\n\n"
                f"🔗 Tu enlace de referido:\n{ref_link}\n\n"
                "📊 ESTADÍSTICAS:\n"
                f"• Referidos válidos totales: {valid_total}\n"
                f"• Referidos PLUS: {stats.get('plus_referred', 0)}\n"
                f"• Referidos PREMIUM: {stats.get('premium_referred', 0)}\n\n"
                "🎁 RECOMPENSAS:\n"
                f"• Días acumulados por referidos: +{reward_days_total} días\n"
                "• Cada referido válido suma +7 días a tu plan actual\n\n"
                "📢 CÓMO REFERIR:\n"
                "1. Comparte tu enlace\n"
                "2. Ellos entran al bot\n"
                "3. Activan un plan\n\n"
                "📌 REGLA ACTUAL:\n"
                "• Cada referido válido agrega +7 días a tu plan actual\n"
                "• El tipo de referido (PLUS/PREMIUM) se muestra solo como estadística\n"
            )

        keyboard = [
            [InlineKeyboardButton(_tr(language, "📋 Copiar enlace", "📋 Copy link"), callback_data="copy_ref_code")],
            [InlineKeyboardButton(_tr(language, "⬅️ Volver al menú", "⬅️ Back to menu"), callback_data="back_menu")],
        ]
        await query.edit_message_text(text=message, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error en handle_referrals: {e}", exc_info=True)
        await query.edit_message_text(
            _tr(language, "❌ Error al cargar información de referidos.", "❌ Error loading referral information."),
            reply_markup=back_to_menu(language=language),
        )


async def handle_copy_ref_code(update, context):
    query = update.callback_query
    await query.answer()

    try:
        user_id = query.from_user.id
        user = users_collection().find_one({"user_id": user_id}) or {}
        language = _get_user_language(user)
        ref_link = get_referral_link(user_id)

        await query.edit_message_text(
            text=_tr(language, f"📋 Tu enlace de referido es:\n\n{ref_link}\n\nCópialo y compártelo.", f"📋 Your referral link is:\n\n{ref_link}\n\nCopy it and share it."),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "⬅️ Volver a referidos", "⬅️ Back to referrals"), callback_data="referrals")],
                [InlineKeyboardButton(_tr(language, "🏠 Menú principal", "🏠 Main menu"), callback_data="back_menu")],
            ]),
        )
    except Exception as e:
        logger.error(f"Error en handle_copy_ref_code: {e}", exc_info=True)
        await query.edit_message_text("❌ Error.")
