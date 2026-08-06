"""
Servicio de Scheduler - Gestión de Tareas Programadas
Ejecuta tareas periódicas: limpieza, notificaciones, evaluaciones
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

class ScheduledTask:
    """Representa una tarea programada"""
    def __init__(self, name: str, interval: int, func: Callable, **kwargs):
        self.name = name
        self.interval = interval  # segundos
        self.func = func
        self.enabled = kwargs.get('enabled', True)
        self.last_run = None
        self.next_run = datetime.utcnow()
        self.run_count = 0
        self.error_count = 0
        
    async def execute(self):
        """Ejecutar la tarea"""
        if not self.enabled:
            return
            
        try:
            logger.debug(f"⚙️ Ejecutando tarea: {self.name}")
            if asyncio.iscoroutinefunction(self.func):
                await self.func()
            else:
                self.func()
            self.last_run = datetime.utcnow()
            self.run_count += 1
            logger.info(f"✅ Tarea {self.name} completada (ejecuciones: {self.run_count})")
        except Exception as e:
            self.error_count += 1
            logger.error(f"❌ Error en tarea {self.name}: {e}")
        finally:
            self.next_run = datetime.utcnow() + timedelta(seconds=self.interval)
    
    def is_due(self) -> bool:
        """Verificar si la tarea debe ejecutarse"""
        return datetime.utcnow() >= self.next_run and self.enabled
    
    def get_stats(self) -> Dict:
        """Obtener estadísticas de la tarea"""
        return {
            "name": self.name,
            "interval": self.interval,
            "enabled": self.enabled,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat(),
            "run_count": self.run_count,
            "error_count": self.error_count
        }


class SchedulerService:
    """
    Servicio de scheduler para tareas programadas
    Gestiona múltiples tareas con diferentes intervalos
    """
    
    def __init__(self):
        self.tasks: Dict[str, ScheduledTask] = {}
        self.is_running = False
        self.check_interval = 5  # Verificar tareas cada 5 segundos
        
    def register_task(self, name: str, interval: int, func: Callable, **kwargs):
        """Registrar una nueva tarea programada"""
        if name in self.tasks:
            logger.warning(f"⚠️ Tarea {name} ya existe, será reemplazada")
        
        task = ScheduledTask(name, interval, func, **kwargs)
        self.tasks[name] = task
        logger.info(f"📝 Tarea registrada: {name} (intervalo: {interval}s)")
        return task
    
    def unregister_task(self, name: str):
        """Eliminar una tarea registrada"""
        if name in self.tasks:
            del self.tasks[name]
            logger.info(f"🗑️ Tarea eliminada: {name}")
    
    def enable_task(self, name: str):
        """Habilitar una tarea"""
        if name in self.tasks:
            self.tasks[name].enabled = True
            self.tasks[name].next_run = datetime.utcnow()
            logger.info(f"✅ Tarea habilitada: {name}")
    
    def disable_task(self, name: str):
        """Deshabilitar una tarea"""
        if name in self.tasks:
            self.tasks[name].enabled = False
            logger.info(f"🚫 Tarea deshabilitada: {name}")
    
    async def start(self):
        """Iniciar el scheduler"""
        self.is_running = True
        logger.info("🕐 Scheduler Service started")
        asyncio.create_task(self._scheduler_loop())
    
    async def stop(self):
        """Detener el scheduler"""
        self.is_running = False
        logger.info("🛑 Scheduler Service stopped")
    
    async def _scheduler_loop(self):
        """Bucle principal del scheduler"""
        while self.is_running:
            try:
                await self._check_and_execute_tasks()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"❌ Error en scheduler loop: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def _check_and_execute_tasks(self):
        """Verificar y ejecutar tareas pendientes"""
        for name, task in self.tasks.items():
            if task.is_due():
                asyncio.create_task(task.execute())
    
    def get_all_stats(self) -> Dict:
        """Obtener estadísticas de todas las tareas"""
        return {
            "is_running": self.is_running,
            "total_tasks": len(self.tasks),
            "enabled_tasks": sum(1 for t in self.tasks.values() if t.enabled),
            "tasks": {name: task.get_stats() for name, task in self.tasks.items()}
        }
    
    def run_now(self, name: str):
        """Ejecutar una tarea inmediatamente"""
        if name in self.tasks:
            self.tasks[name].next_run = datetime.utcnow()
            logger.info(f"⚡ Ejecución inmediata solicitada: {name}")


# Instancia global
scheduler_service = SchedulerService()


# Funciones de tareas predefinidas
async def cleanup_old_data(db):
    """Limpieza de datos antiguos"""
    if not db:
        return
    
    # Limpiar sesiones expiradas (> 24h)
    cutoff = datetime.utcnow() - timedelta(hours=24)
    result = await db.push_sessions.delete_many({"last_seen": {"$lt": cutoff}})
    if result.deleted_count > 0:
        logger.info(f"🧹 Limpiadas {result.deleted_count} sesiones expiradas")
    
    # Limpiar oportunidades antiguas (> 7 días)
    cutoff = datetime.utcnow() - timedelta(days=7)
    result = await db.opportunities.delete_many({"detected_at": {"$lt": cutoff}})
    if result.deleted_count > 0:
        logger.info(f"🧹 Limpiadas {result.deleted_count} oportunidades antiguas")


async def check_expired_plans(db):
    """Verificar planes expirados"""
    if not db:
        return
    
    now = datetime.utcnow()
    result = await db.users.update_many(
        {"plan_expires_at": {"$lt": now}, "plan": {"$ne": "free"}},
        {"$set": {"plan": "free", "plan_expires_at": None}}
    )
    if result.modified_count > 0:
        logger.info(f"📅 {result.modified_count} planes expirados actualizados a free")


async def send_daily_summary(db, websocket_manager):
    """Enviar resumen diario a usuarios premium"""
    # Implementación futura para notificaciones diarias
    pass


def register_default_tasks(scheduler: SchedulerService, db=None, websocket_manager=None):
    """Registrar tareas por defecto"""
    logger.info("📋 Registrando tareas por defecto")
    
    # Limpieza de datos cada hora
    if db:
        scheduler.register_task(
            "cleanup_old_data",
            interval=3600,  # 1 hora
            func=lambda: asyncio.create_task(cleanup_old_data(db))
        )
        
        # Verificación de planes cada 15 minutos
        scheduler.register_task(
            "check_expired_plans",
            interval=900,  # 15 minutos
            func=lambda: asyncio.create_task(check_expired_plans(db))
        )
    
    # Resumen diario (si hay websocket manager)
    if websocket_manager and db:
        scheduler.register_task(
            "send_daily_summary",
            interval=86400,  # 24 horas
            func=lambda: asyncio.create_task(send_daily_summary(db, websocket_manager))
        )
