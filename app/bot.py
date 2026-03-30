# app/bot.py

import asyncio
import os
import logging
import threading
import signal
import sys
from typing import Any

from telegram import Bot
from telegram.ext import Application

from app.handlers import get_handlers
from app.scanner import scan_market
from app.scheduler import scheduler_loop
from app.config import get_bot_display_name, get_bot_token
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


def _require_bot_token() -> str:
    token = get_bot_token()
    if not token:
        raise RuntimeError("BOT_TOKEN no está definido")
    return token


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



def _create_raw_bot() -> Bot:
    return Bot(token=_require_bot_token())



def _start_scanner_thread(bot: Bot) -> threading.Thread:
    def run_scanner() -> None:
        heartbeat("scanner", status="starting")
        try:
            logger.info("📡 Iniciando thread del scanner...")
            scan_market(bot)
        except Exception as exc:
            heartbeat("scanner", status="error", details={"error": str(exc)})
            record_audit_event(
                event_type="scanner_thread_crashed",
                status="error",
                module="bot",
                message=str(exc),
            )
            logger.error("❌ Thread scanner falló: %s", exc, exc_info=True)

    scanner_thread = threading.Thread(
        target=run_scanner,
        daemon=True,
        name="ScannerThread",
    )
    scanner_thread.start()
    return scanner_thread



def _start_scheduler_thread() -> threading.Thread:
    def run_scheduler() -> None:
        heartbeat("scheduler", status="starting")
        try:
            logger.info("⏰ Iniciando thread del scheduler (modo seguro)...")
            asyncio.run(scheduler_loop())
        except Exception as exc:
            heartbeat("scheduler", status="error", details={"error": str(exc)})
            record_audit_event(
                event_type="scheduler_thread_crashed",
                status="error",
                module="bot",
                message=str(exc),
            )
            logger.error("❌ Thread scheduler falló: %s", exc, exc_info=True)

    scheduler_thread = threading.Thread(
        target=run_scheduler,
        daemon=True,
        name="SchedulerThread",
    )
    scheduler_thread.start()
    return scheduler_thread


# ======================================================
# RUN BOT (ENTRYPOINT ÚNICO)
# ======================================================

def run_bot(*, background: bool = False, enable_scanner: bool = True, enable_scheduler: bool = True) -> None:
    initialize_database()
    heartbeat("database", status="ok", details={"stage": "initialized"})

    application = Application.builder().token(_require_bot_token()).build()
    application.add_error_handler(application_error_handler)

    for handler in get_handlers():
        application.add_handler(handler)

    bot = application.bot
    started_threads: list[str] = []

    if enable_scanner:
        initialize_signal_pipeline(bot)
        heartbeat("signal_pipeline", status="starting")
        started_threads.append(_start_scanner_thread(bot).name)

    if enable_scheduler:
        started_threads.append(_start_scheduler_thread().name)

    heartbeat(
        "bot",
        status="ok",
        details={
            "threads": started_threads,
            "scanner_enabled": enable_scanner,
            "scheduler_enabled": enable_scheduler,
        },
    )
    logger.info("✅ Runtime bot inicializado (scanner=%s scheduler=%s)", enable_scanner, enable_scheduler)

    def signal_handler(sig: int, frame: Any) -> None:
        logger.info("🛑 Recibida señal de terminación (%s)...", sig)
        heartbeat("bot", status="stopping", details={"signal": sig})

        if application.running:
            logger.info("Deteniendo aplicación de Telegram...")
            application.stop()

        logger.info("Bot detenido correctamente")
        sys.exit(0)

    if not background:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info("🤖 %s iniciando...", get_bot_display_name())
    log_event(
        "bot.starting",
        bot_name=get_bot_display_name(),
        scanner_enabled=enable_scanner,
        scheduler_enabled=enable_scheduler,
    )

    try:
        run_kwargs = {
            "poll_interval": 0.5,
            "timeout": 30,
            "drop_pending_updates": True,
        }
        if background:
            run_kwargs["stop_signals"] = None
        application.run_polling(**run_kwargs)
    except Exception as exc:
        heartbeat("bot", status="error", details={"error": str(exc)})
        record_audit_event(
            event_type="run_polling_error",
            status="error",
            module="bot",
            message=str(exc),
        )
        logger.error("❌ Error en run_polling: %s", exc, exc_info=True)
        raise



def run_signal_worker() -> None:
    initialize_database()
    heartbeat("database", status="ok", details={"stage": "initialized"})
    heartbeat("signal_worker", status="starting")
    bot = _create_raw_bot()
    initialize_signal_pipeline(bot)
    try:
        logger.info("🚀 Iniciando signal worker dedicado...")
        heartbeat("signal_worker", status="ok", details={"stage": "running"})
        scan_market(bot)
    except Exception as exc:
        heartbeat("signal_worker", status="error", details={"error": str(exc)})
        record_audit_event(
            event_type="signal_worker_crashed",
            status="error",
            module="signal_worker",
            message=str(exc),
        )
        logger.error("❌ Signal worker falló: %s", exc, exc_info=True)
        raise



