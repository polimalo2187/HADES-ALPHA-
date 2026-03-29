# app/bot.py

import asyncio
import os
import logging
import threading
import signal
import sys
from typing import Any

from telegram.ext import Application

from app.handlers import get_handlers
from app.scanner import scan_market
from app.scheduler import scheduler_loop
from app.config import get_bot_display_name
from app.database import initialize_database
from app.observability import heartbeat, log_event, record_audit_event
from app.realtime_pipeline import initialize_signal_pipeline

# Configurar logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ======================================================
# VARIABLES DE ENTORNO
# ======================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN no está definido")


async def application_error_handler(update: object, context) -> None:
    error = context.error
    callback = None
    user_id = None
    chat_id = None

    try:
        if getattr(update, "callback_query", None):
            callback = update.callback_query.data
            user_id = getattr(update.callback_query.from_user, "id", None)
            chat_id = getattr(update.callback_query.message.chat, "id", None) if update.callback_query.message else None
        elif getattr(update, "effective_user", None):
            user_id = getattr(update.effective_user, "id", None)
            chat_id = getattr(update.effective_chat, "id", None)
    except Exception:
        pass

    message = str(error) if error else "unknown_error"
    log_event(
        "telegram.application_error",
        level=logging.ERROR,
        user_id=user_id,
        chat_id=chat_id,
        callback=callback,
        error=message,
    )
    record_audit_event(
        event_type="telegram_application_error",
        status="error",
        module="bot",
        user_id=user_id,
        callback=callback,
        message=message,
        metadata={"chat_id": chat_id},
    )
    heartbeat("bot", status="degraded", details={"last_error": message})


# ======================================================
# RUN BOT (ENTRYPOINT ÚNICO)
# ======================================================

def run_bot(*, background: bool = False):
    initialize_database()
    heartbeat("database", status="ok", details={"stage": "initialized"})

    # Crear aplicación
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_error_handler(application_error_handler)

    # Handlers
    for handler in get_handlers():
        application.add_handler(handler)

    # Obtener el bot del application
    bot = application.bot

    # Pipeline de señales en tiempo real
    initialize_signal_pipeline(bot)
    heartbeat("signal_pipeline", status="starting")

    # ==============================
    # BACKGROUND THREADS CON MANEJO DE ERRORES
    # ==============================

    def run_scanner():
        """Ejecuta el scanner en un thread dedicado."""
        heartbeat("scanner", status="starting")
        try:
            logger.info("📡 Iniciando thread del scanner...")
            scan_market(bot)
        except Exception as e:
            heartbeat("scanner", status="error", details={"error": str(e)})
            record_audit_event(
                event_type="scanner_thread_crashed",
                status="error",
                module="bot",
                message=str(e),
            )
            logger.error(f"❌ Thread scanner falló: {e}", exc_info=True)

    def run_scheduler():
        """Ejecuta el scheduler en un thread dedicado (modo seguro)."""
        heartbeat("scheduler", status="starting")
        try:
            logger.info("⏰ Iniciando thread del scheduler (modo seguro)...")
            asyncio.run(scheduler_loop())
        except Exception as e:
            heartbeat("scheduler", status="error", details={"error": str(e)})
            record_audit_event(
                event_type="scheduler_thread_crashed",
                status="error",
                module="bot",
                message=str(e),
            )
            logger.error(f"❌ Thread scheduler falló: {e}", exc_info=True)

    # Iniciar threads con nombres para debugging
    scanner_thread = threading.Thread(
        target=run_scanner,
        daemon=True,
        name="ScannerThread"
    )

    scheduler_thread = threading.Thread(
        target=run_scheduler,
        daemon=True,
        name="SchedulerThread"
    )

    scanner_thread.start()
    scheduler_thread.start()

    heartbeat("bot", status="ok", details={"threads": [scanner_thread.name, scheduler_thread.name]})
    logger.info("✅ Threads de fondo iniciados correctamente")

    # ==============================
    # MANEJO DE SEÑALES PARA SHUTDOWN ELEGANTE
    # ==============================

    def signal_handler(sig: int, frame: Any):
        """Maneja señales de terminación."""
        logger.info(f"\n🛑 Recibida señal de terminación ({sig})...")
        heartbeat("bot", status="stopping", details={"signal": sig})

        # Detener la aplicación
        if application.running:
            logger.info("Deteniendo aplicación de Telegram...")
            application.stop()

        logger.info("Bot detenido correctamente")
        sys.exit(0)

    # Registrar manejadores de señales solo en foreground
    if not background:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    # ==============================
    # INICIAR POLLING
    # ==============================

    logger.info(f"🤖 {get_bot_display_name()} iniciando...")
    log_event("bot.starting", bot_name=get_bot_display_name())

    try:
        if background:
            application.run_polling(
                poll_interval=0.5,
                timeout=30,
                drop_pending_updates=True,
                stop_signals=None,
            )
        else:
            application.run_polling(
                poll_interval=0.5,
                timeout=30,
                drop_pending_updates=True,
            )
    except Exception as e:
        heartbeat("bot", status="error", details={"error": str(e)})
        record_audit_event(
            event_type="run_polling_error",
            status="error",
            module="bot",
            message=str(e),
        )
        logger.error(f"❌ Error en run_polling: {e}", exc_info=True)
        raise
