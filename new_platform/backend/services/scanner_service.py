"""
Servicio de Scanner - Escaneo de mercado para señales
"""

import asyncio
import logging
from datetime import datetime

from backend.config import settings

logger = logging.getLogger(__name__)

_scanner_task: asyncio.Task | None = None


async def _scanner_loop():
    """Loop principal del scanner"""
    logger.info("📡 Scanner iniciado")
    
    while True:
        try:
            # Aquí iría la lógica de escaneo de mercado
            # Llamadas a Binance API, análisis técnico, etc.
            
            await asyncio.sleep(60)  # Placeholder
            
            logger.debug(f"🔄 Scanner cycle completed at {datetime.utcnow()}")
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Error en scanner: {e}")
            await asyncio.sleep(10)  # Backoff en caso de error


async def start_scanner():
    """Inicia el scanner"""
    global _scanner_task
    
    if _scanner_task is not None and not _scanner_task.done():
        logger.warning("⚠️ Scanner ya está ejecutándose")
        return
    
    _scanner_task = asyncio.create_task(_scanner_loop())
    logger.info("✅ Scanner task creada")


async def stop_scanner():
    """Detiene el scanner"""
    global _scanner_task
    
    if _scanner_task:
        _scanner_task.cancel()
        try:
            await _scanner_task
        except asyncio.CancelledError:
            pass
        _scanner_task = None
        logger.info("🛑 Scanner detenido")


__all__ = ["start_scanner", "stop_scanner"]
