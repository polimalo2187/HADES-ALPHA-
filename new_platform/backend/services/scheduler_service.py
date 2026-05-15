"""
Servicio de Scheduler - Gestión de tareas programadas
"""

import asyncio
import logging
from datetime import datetime

from backend.config import settings

logger = logging.getLogger(__name__)

_scheduler_task: asyncio.Task | None = None


async def _scheduler_loop():
    """Loop principal del scheduler"""
    logger.info("🕐 Scheduler iniciado")
    
    while True:
        try:
            await asyncio.sleep(settings.SCHEDULER_CHECK_INTERVAL)
            
            # Aquí iría la lógica de evaluación de señales
            # Por ahora es un placeholder
            
            logger.debug(f"🔄 Scheduler cycle completed at {datetime.utcnow()}")
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Error en scheduler: {e}")
            await asyncio.sleep(10)  # Backoff en caso de error


async def start_scheduler():
    """Inicia el scheduler"""
    global _scheduler_task
    
    if _scheduler_task is not None and not _scheduler_task.done():
        logger.warning("⚠️ Scheduler ya está ejecutándose")
        return
    
    _scheduler_task = asyncio.create_task(_scheduler_loop())
    logger.info("✅ Scheduler task creada")


async def stop_scheduler():
    """Detiene el scheduler"""
    global _scheduler_task
    
    if _scheduler_task:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
        _scheduler_task = None
        logger.info("🛑 Scheduler detenido")


__all__ = ["start_scheduler", "stop_scheduler"]
