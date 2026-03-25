import asyncio
import logging
from typing import List
from telegram import Bot
from datetime import datetime

from app.database import users_collection, signals_collection
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
from app.config import is_admin
from app.models import is_trial_active, is_plan_active

logger = logging.getLogger(__name__)

# ======================================================
# CONFIGURACIÓN
# ======================================================

ALERT_AUTO_DELETE_SECONDS = 8

# ======================================================
# USUARIOS ELEGIBLES POR PLAN
# ======================================================

def _eligible_users_for_alert(signal_visibility: str) -> List[int]:
    """
    Retorna usuarios que DEBEN recibir el push.
    Reglas:
    - Cada usuario SOLO recibe push de su plan
    - Admin SOLO recibe PREMIUM
    """

    users_col = users_collection()
    eligible_users: List[int] = []

    users = users_col.find(
        {},
        {"user_id": 1, "plan": 1, "trial_end": 1, "plan_end": 1}
    )

    for user in users:
        user_id = user.get("user_id")
        user_plan = user.get("plan", PLAN_FREE)

        admin = is_admin(user_id)
        has_access = is_plan_active(user) or is_trial_active(user)

        # ❌ Sin acceso y no admin → fuera
        if not has_access and not admin:
            continue

        # 👑 ADMIN: SOLO PREMIUM
        if admin:
            if signal_visibility == PLAN_PREMIUM:
                eligible_users.append(user_id)
            continue

        # 👤 USUARIOS NORMALES: SOLO SU PLAN
        if user_plan == signal_visibility:
            eligible_users.append(user_id)

    return eligible_users

# ======================================================
# AUTO DELETE
# ======================================================

async def _auto_delete(bot: Bot, chat_id: int, message_id: int):
    await asyncio.sleep(ALERT_AUTO_DELETE_SECONDS)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

# ======================================================
# PUSH DE NUEVA SEÑAL (SIN BLOQUEO)
# ======================================================

async def notify_new_signal_alert(
    bot: Bot,
    signal_visibility: str,
    **kwargs,
):
    """
    Envía push inmediatamente cuando hay señal.
    Filtrado SOLO por plan exacto.
    """

    user_ids = _eligible_users_for_alert(signal_visibility)

    if not user_ids:
        logger.warning(
            f"📭 Push NO enviado: sin usuarios para plan {signal_visibility}"
        )
        return

    alert_text = (
        "📢 *NUEVA SEÑAL DISPONIBLE*\n\n"
        "👉 Entra al bot y toca *Ver señales*.\n\n"
        "⏳ Tiempo limitado."
    )

    sent = 0

    for user_id in user_ids:
        try:
            msg = await bot.send_message(
                chat_id=user_id,
                text=alert_text,
                parse_mode="Markdown",
            )
            asyncio.create_task(_auto_delete(bot, user_id, msg.message_id))
            sent += 1
        except Exception as e:
            logger.warning(f"⚠️ Push fallido a {user_id}: {e}")

    logger.info(
        f"📨 Push enviado ({signal_visibility}): {sent}/{len(user_ids)} usuarios"
    )

# ======================================================
# NOTIFICACIONES DE PLAN (SIN CAMBIOS)
# ======================================================

async def notify_plan_activation(
    bot: Bot,
    user_id: int,
    plan: str,
    expires_at: datetime,
):
    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                f"✅ Plan {plan.upper()} activado.\n\n"
                f"Vence el: {expires_at.strftime('%d/%m/%Y')}"
            ),
        )
    except Exception as e:
        logger.error(f"❌ Error notificando activación a {user_id}: {e}")


async def notify_plan_expired(
    bot: Bot,
    user_id: int,
):
    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "⚠️ Tu plan ha expirado.\n\n"
                "Contacta a un administrador para renovarlo."
            ),
        )
    except Exception as e:
        logger.error(f"❌ Error notificando expiración a {user_id}: {e}")
