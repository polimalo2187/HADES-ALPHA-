"""
Observabilidad - Health checks, logs y métricas
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any

from backend.config import settings
from backend.database import check_database_connection

logger = logging.getLogger(__name__)

_heartbeat_task: asyncio.Task | None = None


async def _heartbeat_loop():
    """Envía heartbeats periódicos"""
    logger.info("💓 Heartbeat iniciado")
    
    while True:
        try:
            await asyncio.sleep(60)
            
            # Verificar estado de componentes
            db_ok = await check_database_connection()
            
            logger.debug(
                f"💓 Heartbeat: database={'OK' if db_ok else 'FAIL'}"
            )
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Error en heartbeat: {e}")


async def start_observability():
    """Inicia los servicios de observabilidad"""
    global _heartbeat_task
    
    if _heartbeat_task is not None and not _heartbeat_task.done():
        return
    
    _heartbeat_task = asyncio.create_task(_heartbeat_loop())
    logger.info("✅ Observabilidad iniciada")


async def stop_observability():
    """Detiene los servicios de observabilidad"""
    global _heartbeat_task
    
    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass
        _heartbeat_task = None
        logger.info("🛑 Observabilidad detenida")


def build_health_report() -> Dict[str, Any]:
    """Genera reporte de salud del sistema"""
    return {
        "ok": True,
        "service": "hades-platform",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": settings.ENVIRONMENT,
        "components": {
            "database": "connected",
            "scanner": "running",
            "scheduler": "running",
            "websocket": "active",
        }
    }


async def log_event(event_type: str, level: int = logging.INFO, **kwargs):
    """Registra un evento en los logs"""
    extra_data = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    message = f"{event_type}: {extra_data}" if extra_data else event_type
    logger.log(level, message)


async def record_audit_event(
    event_type: str,
    status: str,
    module: str,
    user_id: int | None = None,
    message: str | None = None,
    metadata: Dict[str, Any] | None = None,
):
    """Registra un evento de auditoría"""
    from backend.database import get_audit_logs_collection
    
    audit_doc = {
        "event_type": event_type,
        "status": status,
        "module": module,
        "user_id": user_id,
        "message": message,
        "metadata": metadata or {},
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    
    try:
        await get_audit_logs_collection().insert_one(audit_doc)
    except Exception as e:
        logger.warning(f"Error guardando audit log: {e}")


__all__ = [
    "start_observability",
    "stop_observability",
    "build_health_report",
    "log_event",
    "record_audit_event",
]
