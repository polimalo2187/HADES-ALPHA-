from __future__ import annotations

import asyncio
import logging
from functools import partial

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from app.config import is_admin
from app.observability import get_health_snapshot, record_audit_event
from app.plans import PLAN_PLUS, PLAN_PREMIUM, activate_plus, activate_premium
from app.services.admin_service import ban_user, can_block_target, get_user_by_id, validate_custom_plan_days
from app.telegram_handlers.common import _admin_panel_keyboard, _get_user_language, _tr

logger = logging.getLogger(__name__)


async def handle_admin_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_user_id_str = update.message.text.strip()

        if context.user_data.get("awaiting_plan_days"):
            try:
                days = int(target_user_id_str)
            except ValueError:
                await update.message.reply_text("❌ Días inválidos. Debe ser un número entero.")
                return

            valid_days, reason = validate_custom_plan_days(days)
            if not valid_days:
                if reason == "non_positive":
                    await update.message.reply_text("❌ La cantidad de días debe ser mayor que 0.")
                else:
                    await update.message.reply_text("❌ La cantidad de días es demasiado alta.")
                return

            if not is_admin(update.effective_user.id):
                await update.message.reply_text("❌ Permisos revocados.")
                context.user_data["awaiting_plan_days"] = False
                context.user_data.pop("custom_plan_type", None)
                context.user_data.pop("target_user_id", None)
                return

            target_user_id = context.user_data.get("target_user_id")
            custom_plan_type = context.user_data.get("custom_plan_type")
            if not target_user_id or custom_plan_type not in {PLAN_PLUS, PLAN_PREMIUM}:
                context.user_data["awaiting_plan_days"] = False
                context.user_data.pop("custom_plan_type", None)
                context.user_data.pop("target_user_id", None)
                await update.message.reply_text("❌ No hay una activación personalizada pendiente.")
                return

            loop = asyncio.get_event_loop()
            if custom_plan_type == PLAN_PLUS:
                success = await loop.run_in_executor(None, partial(activate_plus, target_user_id, days))
                plan_name = "PLUS"
            else:
                success = await loop.run_in_executor(None, partial(activate_premium, target_user_id, days))
                plan_name = "PREMIUM"

            context.user_data["awaiting_plan_days"] = False
            context.user_data.pop("custom_plan_type", None)
            context.user_data.pop("awaiting_plan_choice", None)
            context.user_data.pop("target_user_id", None)

            if success:
                record_audit_event(
                    event_type="admin_plan_activated",
                    status="success",
                    module="admin",
                    admin_id=update.effective_user.id,
                    user_id=target_user_id,
                    message=f"{plan_name} {days}d",
                    metadata={"days": days, "plan": plan_name.lower()},
                )
                await update.message.reply_text(f"✅ Plan {plan_name} activado correctamente por {days} días.")
            else:
                await update.message.reply_text(f"❌ No se pudo activar el plan {plan_name} por días.")
            return

        if context.user_data.get("awaiting_delete_user_id"):
            language = _get_user_language(get_user_by_id(update.effective_user.id) or {})
            try:
                target_user_id = int(target_user_id_str)
            except ValueError:
                await update.message.reply_text(_tr(language, "❌ ID inválido.", "❌ Invalid ID."))
                context.user_data["awaiting_delete_user_id"] = False
                return

            if not is_admin(update.effective_user.id):
                await update.message.reply_text(_tr(language, "❌ Permisos revocados.", "❌ Permissions revoked."))
                context.user_data["awaiting_delete_user_id"] = False
                return

            ok, reason = can_block_target(update.effective_user.id, target_user_id)
            if not ok:
                context.user_data["awaiting_delete_user_id"] = False
                if reason == "self":
                    await update.message.reply_text(_tr(language, "❌ No puedes banearte a ti mismo.", "❌ You cannot ban yourself."))
                else:
                    await update.message.reply_text(_tr(language, "❌ No puedes banear a otro administrador.", "❌ You cannot ban another administrator."))
                return

            loop = asyncio.get_event_loop()
            target_user = await loop.run_in_executor(None, lambda: get_user_by_id(target_user_id))

            if not target_user:
                context.user_data["awaiting_delete_user_id"] = False
                await update.message.reply_text(_tr(language, "❌ Usuario no encontrado.", "❌ User not found."))
                return

            context.user_data["awaiting_delete_user_id"] = False
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "✅ Confirmar bloqueo", "✅ Confirm block"), callback_data=f"confirm_delete_user:{target_user_id}")],
                [InlineKeyboardButton(_tr(language, "❌ Cancelar", "❌ Cancel"), callback_data="cancel_admin_delete")],
            ])
            await update.message.reply_text(
                _tr(
                    language,
                    f"⚠️ Vas a bloquear al usuario {target_user_id}. Esta acción revoca su acceso al bot y detiene señales futuras.\n\n¿Confirmas?",
                    f"⚠️ You are about to block user {target_user_id}. This revokes bot access and stops future signals.\n\nDo you confirm?",
                ),
                reply_markup=keyboard,
            )
            return

        logger.info("[ADMIN] Recibido User ID: %s", target_user_id_str)

        try:
            target_user_id = int(target_user_id_str)
        except ValueError:
            await update.message.reply_text("❌ ID inválido. Debe ser un número.")
            context.user_data["awaiting_user_id"] = False
            return

        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Permisos revocados.")
            context.user_data["awaiting_user_id"] = False
            return

        loop = asyncio.get_event_loop()
        target_user = await loop.run_in_executor(None, lambda: get_user_by_id(target_user_id))

        if not target_user:
            await update.message.reply_text("❌ Usuario no encontrado en la base de datos.")
            context.user_data["awaiting_user_id"] = False
            return

        context.user_data["awaiting_user_id"] = False
        context.user_data["awaiting_plan_choice"] = True
        context.user_data["target_user_id"] = target_user_id

        keyboard = [
            [InlineKeyboardButton("🟡 Activar PLAN PLUS · 30 días", callback_data="choose_plus_plan")],
            [InlineKeyboardButton("🔴 Activar PLAN PREMIUM · 30 días", callback_data="choose_premium_plan")],
            [InlineKeyboardButton("🟡 Activar PLAN PLUS por días", callback_data="choose_plus_plan_days")],
            [InlineKeyboardButton("🔴 Activar PLAN PREMIUM por días", callback_data="choose_premium_plan_days")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="back_menu")],
        ]

        await update.message.reply_text(
            f"✅ Usuario encontrado: {target_user_id}\nSeleccione el plan a activar:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    except Exception as e:
        logger.error("[ADMIN] Error en handle_admin_text: %s", e, exc_info=True)
        await update.message.reply_text("❌ Error procesando la solicitud.")
        context.user_data["awaiting_user_id"] = False


async def handle_admin_callback(query, context, user, action: str, admin: bool) -> bool:
    if not admin:
        return False

    language = _get_user_language(user)
    user_id = user["user_id"]

    if action == "admin_panel":
        await query.edit_message_text(
            _tr(language, "👑 PANEL ADMINISTRADOR", "👑 ADMIN PANEL"),
            reply_markup=_admin_panel_keyboard(language),
        )
        return True

    if action == "admin_health":
        snapshot = get_health_snapshot()
        if not snapshot:
            text = _tr(language, "🩺 No hay datos de salud todavía.", "🩺 No health data yet.")
        else:
            lines = [_tr(language, "🩺 Estado del sistema", "🩺 System health")]
            for component, item in snapshot.items():
                updated_at = item.get("updated_at")
                updated_str = updated_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(updated_at, "strftime") else "n/a"
                status = item.get("status") or "unknown"
                details = item.get("details") or {}
                detail_pairs = ", ".join(f"{k}={v}" for k, v in list(details.items())[:4])
                suffix = f"\n   {detail_pairs}" if detail_pairs else ""
                lines.append(f"• {component}: {status} ({updated_str}){suffix}")
            text = "\n".join(lines)
        await query.edit_message_text(text, reply_markup=_admin_panel_keyboard(language))
        return True

    if action == "admin_delete_user":
        context.user_data["awaiting_delete_user_id"] = True
        await query.edit_message_text(_tr(language, "🆔 Envía el User ID del usuario a eliminar:", "🆔 Send the User ID of the user to delete:"))
        return True

    if action.startswith("confirm_delete_user:"):
        try:
            target_user_id = int(action.split(":", 1)[1])
        except ValueError:
            await query.answer(_tr(language, "ID inválido.", "Invalid ID."), show_alert=True)
            return True

        ok, reason = can_block_target(user_id, target_user_id)
        if not ok:
            if reason == "self":
                await query.answer(_tr(language, "No puedes banearte a ti mismo.", "You cannot ban yourself."), show_alert=True)
            else:
                await query.answer(_tr(language, "No puedes banear a otro administrador.", "You cannot ban another administrator."), show_alert=True)
            return True

        ban_user(target_user_id=target_user_id, banned_by=user_id)
        record_audit_event(
            event_type="admin_user_blocked",
            status="warning",
            module="admin",
            admin_id=user_id,
            user_id=target_user_id,
            message="user_blocked",
        )
        logger.warning("[ADMIN] Usuario baneado | admin=%s target=%s", user_id, target_user_id)
        await query.edit_message_text(
            _tr(language, f"🚫 Usuario {target_user_id} bloqueado correctamente.", f"🚫 User {target_user_id} blocked successfully."),
            reply_markup=_admin_panel_keyboard(language),
        )
        return True

    if action == "cancel_admin_delete":
        await query.edit_message_text(
            _tr(language, "✅ Operación cancelada.", "✅ Operation cancelled."),
            reply_markup=_admin_panel_keyboard(language),
        )
        return True

    if action == "admin_activate_plan":
        context.user_data["awaiting_user_id"] = True
        await query.edit_message_text(_tr(language, "🆔 Envía el User ID del usuario:", "🆔 Send the User ID of the user:"))
        return True

    return False
