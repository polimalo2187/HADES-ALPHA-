# app/bot.py

import asyncio
import os
import logging
import threading
import signal
import sys
from telegram.ext import Application

from app.handlers import get_handlers
from app.scanner import scan_market
from app.scheduler import scheduler_loop
from app.config import get_bot_display_name

# Configurar logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ======================================================
# VARIABLES DE ENTORNO
# ======================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN no está definido")


# ======================================================
# RUN BOT (ENTRYPOINT ÚNICO)
# ======================================================

def run_bot():
    # Crear aplicación
    application = Application.builder().token(BOT_TOKEN).build()

    # Handlers
    for handler in get_handlers():
        application.add_handler(handler)

    # Obtener el bot del application
    bot = application.bot

    # ==============================
    # BACKGROUND THREADS CON MANEJO DE ERRORES
    # ==============================

    def run_scanner():
        """Ejecuta el scanner en un thread dedicado."""
        try:
            logger.info("📡 Iniciando thread del scanner...")
            scan_market(bot)
        except Exception as e:
            logger.error(f"❌ Thread scanner falló: {e}", exc_info=True)

    def run_scheduler():
        """Ejecuta el scheduler en un thread dedicado (modo seguro)."""
        try:
            logger.info("⏰ Iniciando thread del scheduler (modo seguro)...")
            asyncio.run(scheduler_loop())
        except Exception as e:
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

    logger.info("✅ Threads de fondo iniciados correctamente")

    # ==============================
    # MANEJO DE SEÑALES PARA SHUTDOWN ELEGANTE
    # ==============================

    def signal_handler(sig, frame):
        """Maneja señales de terminación."""
        logger.info(f"\n🛑 Recibida señal de terminación ({sig})...")

        # Detener la aplicación
        if application.running:
            logger.info("Deteniendo aplicación de Telegram...")
            application.stop()

        logger.info("Bot detenido correctamente")
        sys.exit(0)

    # Registrar manejadores de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # ==============================
    # INICIAR POLLING
    # ==============================

    logger.info(f"🤖 {get_bot_display_name()} iniciando...")

    try:
        application.run_polling(
            poll_interval=0.5,
            timeout=30,
            drop_pending_updates=True
        )
    except Exception as e:
        logger.error(f"❌ Error en run_polling: {e}", exc_info=True)
        raise
