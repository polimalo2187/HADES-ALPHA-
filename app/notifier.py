import asyncio
import logging
import threading
from typing import Dict, List, Optional
from telegram import Bot
from datetime import datetime

from app.database import users_collection
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
from app.config import is_admin
from app.models import is_trial_active, is_plan_active

logger = logging.getLogger(__name__)

ALERT_AUTO_DELETE_SECONDS = 8
MAX_PUSH_CONCURRENCY = 25


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
        {"user_id": 1, "plan": 1, "trial_end": 1, "plan_end": 1, "banned": 1}
    )

    now = datetime.utcnow()

    for user in users:
        if user.get("banned"):
            continue

        user_id = user.get("user_id")
        user_plan = user.get("plan", PLAN_FREE)

        admin = is_admin(user_id)
        has_access = is_plan_active(user) or is_trial_active(user)

        if not has_access and not admin:
            continue

        if user.get("plan_end") and user["plan_end"] < now and not admin:
            continue

        if admin:
            if signal_visibility == PLAN_PREMIUM:
                eligible_users.append(int(user_id))
            continue

        if user_plan == signal_visibility:
            eligible_users.append(int(user_id))

    return eligible_users


async def _delete_message_once(bot: Bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


def _schedule_auto_delete(bot: Bot, chat_id: int, message_id: int) -> None:
    def _runner() -> None:
        try:
            asyncio.run(_delete_message_once(bot, chat_id, message_id))
        except Exception:
            pass

    timer = threading.Timer(ALERT_AUTO_DELETE_SECONDS, _runner)
    timer.daemon = True
    timer.start()


async def send_signal_alerts(
    bot: Bot,
    signal_visibility: str,
    user_ids: Optional[List[int]] = None,
) -> Dict[str, object]:
    """
    Push concurrente para minimizar latencia de entrega.
    Devuelve métricas útiles para trazabilidad.
    """
    recipients = list(user_ids) if user_ids is not None else _eligible_users_for_alert(signal_visibility)

    if not recipients:
        logger.warning("📭 Push no enviado: sin usuarios elegibles para %s", signal_visibility)
        return {
            "requested": 0,
            "sent": 0,
            "failed": 0,
            "first_push_at": None,
            "last_push_at": None,
            "results": [],
        }

    alert_text = (
        "📢 *NUEVA SEÑAL DISPONIBLE*\n\n"
        "👉 Entra al bot y toca *Ver señales*.\n\n"
        "⏳ Tiempo limitado."
    )

    semaphore = asyncio.Semaphore(MAX_PUSH_CONCURRENCY)
    timestamps: List[datetime] = []

    async def _send_one(user_id: int) -> Dict[str, object]:
        async with semaphore:
            try:
                msg = await bot.send_message(
                    chat_id=user_id,
                    text=alert_text,
                    parse_mode="Markdown",
                )
                sent_at = datetime.utcnow()
                timestamps.append(sent_at)
                _schedule_auto_delete(bot, user_id, msg.message_id)
                return {
                    "user_id": int(user_id),
                    "status": "sent",
                    "sent_at": sent_at,
                    "error": None,
                }
            except Exception as exc:
                logger.warning("⚠️ Push fallido a %s: %s", user_id, exc)
                return {
                    "user_id": int(user_id),
                    "status": "failed",
                    "sent_at": None,
                    "error": str(exc),
                }

    results = await asyncio.gather(*(_send_one(user_id) for user_id in recipients))
    sent = sum(1 for result in results if result["status"] == "sent")
    failed = len(results) - sent
    first_push_at = min(timestamps) if timestamps else None
    last_push_at = max(timestamps) if timestamps else None

    logger.info(
        "📨 Push concurrente %s: enviados=%s fallidos=%s total=%s",
        signal_visibility,
        sent,
        failed,
        len(recipients),
    )

    return {
        "requested": len(recipients),
        "sent": sent,
        "failed": failed,
        "first_push_at": first_push_at,
        "last_push_at": last_push_at,
        "results": results,
    }


async def notify_new_signal_alert(
    bot: Bot,
    signal_visibility: str,
    **kwargs,
):
    """Compatibilidad: mantiene la firma previa y ahora usa envío concurrente."""
    return await send_signal_alerts(bot, signal_visibility)


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
