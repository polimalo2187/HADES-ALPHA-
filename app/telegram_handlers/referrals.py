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
        valid_total = int(stats.get("valid_referrals_total", 0) or 0)
        reward_days_total = int(stats.get("reward_days_total", 0) or 0)

        reward_rules = (
            "• 7 días comprados → 3 días de recompensa\n"
            "• 15 días comprados → 7 días de recompensa\n"
            "• 21 días comprados → 10 días de recompensa\n"
            "• 30 días comprados → 15 días de recompensa"
        )
        reward_rules_en = (
            "• 7-day plan → 3 reward days\n"
            "• 15-day plan → 7 reward days\n"
            "• 21-day plan → 10 reward days\n"
            "• 30-day plan → 15 reward days"
        )

        recent_rows = stats.get("recent_rewards", [])
        if recent_rows:
            if language == "en":
                recent_text = "\n".join(
                    f"• {str(row.get('activated_plan', '')).upper()} {int(row.get('activated_days', 0) or 0)}d → +{int(row.get('reward_days_applied', 0) or 0)}d"
                    for row in recent_rows
                )
            else:
                recent_text = "\n".join(
                    f"• {str(row.get('activated_plan', '')).upper()} {int(row.get('activated_days', 0) or 0)} días → +{int(row.get('reward_days_applied', 0) or 0)} días"
                    for row in recent_rows
                )
        else:
            recent_text = "—"

        if language == "en":
            message = (
                "👥 REFERRAL SYSTEM\n\n"
                f"🔗 Your referral link:\n{ref_link}\n\n"
                "📊 STATS:\n"
                f"• Total valid referrals: {valid_total}\n"
                f"• PLUS referrals: {stats.get('plus_referred', 0)}\n"
                f"• PREMIUM referrals: {stats.get('premium_referred', 0)}\n\n"
                "🎁 REWARDS:\n"
                f"• Total reward days earned: +{reward_days_total} days\n"
                f"{reward_rules_en}\n\n"
                "📌 RULES:\n"
                "• Only valid referrals count (users who actually buy a plan)\n"
                "• Reward tier matches the purchased tier\n"
                "• If you are on PLUS and your referral buys PREMIUM, you upgrade to PREMIUM\n"
                "• Lower-tier rewards never downgrade a higher-tier plan\n\n"
                "🕘 LAST REWARDS:\n"
                f"{recent_text}"
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
                f"{reward_rules}\n\n"
                "📌 REGLAS:\n"
                "• Solo cuentan referidos válidos (usuarios que realmente compran un plan)\n"
                "• El tier de la recompensa es el mismo tier comprado\n"
                "• Si estás en PLUS y tu referido compra PREMIUM, subes a PREMIUM\n"
                "• Una recompensa de tier menor nunca te baja de tier\n\n"
                "🕘 ÚLTIMAS RECOMPENSAS:\n"
                f"{recent_text}"
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
