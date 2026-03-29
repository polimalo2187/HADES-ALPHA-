# app/scheduler.py

import os
import asyncio
import logging
from datetime import datetime, timedelta

from app.database import users_collection, signals_collection, user_signals_collection
from app.plans import PLAN_FREE, SUBSCRIPTION_STATUS_EXPIRED
from app.stats_engine import run_statistics_cycle
from app.database import signal_history_collection, signal_results_collection
from app.history_service import backfill_signal_history

logger = logging.getLogger(__name__)

# ======================================================
# CONFIGURACIÓN (VARIABLES DE ENTORNO)
# ======================================================

CHECK_INTERVAL_SECONDS = int(os.getenv("SCHEDULER_CHECK_INTERVAL", "300"))  # 5 min por defecto
BATCH_SIZE = int(os.getenv("SCHEDULER_BATCH_SIZE", "100"))  # Usuarios por batch
EVALUATION_LIMIT = int(os.getenv("SCHEDULER_EVALUATION_LIMIT", "200"))
STATS_REFRESH_EVERY_LOOPS = int(os.getenv("SCHEDULER_STATS_REFRESH_EVERY_LOOPS", "3"))
HISTORY_BACKFILL_EVERY_LOOPS = int(os.getenv("SCHEDULER_HISTORY_BACKFILL_EVERY_LOOPS", "6"))
BASE_SIGNALS_RETENTION_DAYS = int(os.getenv("BASE_SIGNALS_RETENTION_DAYS", "180"))
USER_SIGNALS_RETENTION_DAYS = int(os.getenv("USER_SIGNALS_RETENTION_DAYS", "45"))
SIGNAL_RESULTS_RETENTION_DAYS = int(os.getenv("SIGNAL_RESULTS_RETENTION_DAYS", "365"))
SIGNAL_HISTORY_RETENTION_DAYS = int(os.getenv("SIGNAL_HISTORY_RETENTION_DAYS", "730"))

# ======================================================
# TAREA: EXPIRACIÓN DE PLANES (CORREGIDA SIN BOT)
# ======================================================

async def check_expired_plans() -> int:
    """
    Revisa planes vencidos y actualiza a FREE.
    Retorna el número de usuarios procesados.
    """
    users_col = users_collection()
    now = datetime.utcnow()
    
    # Consulta optimizada: solo usuarios con plan activo que hayan expirado
    expired_users = users_col.find(
        {
            "plan_end": {"$lt": now, "$ne": None},
            "plan": {"$ne": PLAN_FREE}  # Solo usuarios con plan activo
        }
    ).limit(BATCH_SIZE)
    
    processed_count = 0
    
    for user in expired_users:
        try:
            user_id = user["user_id"]
            
            # Actualizar a FREE
            result = users_col.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "plan": PLAN_FREE,
                        "plan_end": None,
                        "subscription_status": SUBSCRIPTION_STATUS_EXPIRED,
                        "last_plan_change_at": now,
                        "updated_at": now,
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"📋 Plan expirado para usuario {user_id}, actualizado a FREE")
                processed_count += 1
                
        except Exception as e:
            logger.error(f"❌ Error procesando usuario {user.get('user_id', 'unknown')}: {e}")
    
    return processed_count

# ======================================================
# TAREAS DE MANTENIMIENTO (NO REQUIEREN BOT)
# ======================================================

async def cleanup_old_signals():
    """Limpia datos antiguos con retención comercial más seria."""
    try:
        now = datetime.utcnow()
        cutoff_base = now - timedelta(days=BASE_SIGNALS_RETENTION_DAYS)
        cutoff_user = now - timedelta(days=USER_SIGNALS_RETENTION_DAYS)
        cutoff_results = now - timedelta(days=SIGNAL_RESULTS_RETENTION_DAYS)
        cutoff_history = now - timedelta(days=SIGNAL_HISTORY_RETENTION_DAYS)

        result_base = signals_collection().delete_many({
            "created_at": {"$lt": cutoff_base},
            "evaluated": True,
        })
        result_user = user_signals_collection().delete_many({
            "created_at": {"$lt": cutoff_user}
        })
        result_results = signal_results_collection().delete_many({
            "evaluated_at": {"$lt": cutoff_results}
        })
        result_history = signal_history_collection().delete_many({
            "signal_created_at": {"$lt": cutoff_history}
        })

        if any([result_base.deleted_count, result_user.deleted_count, result_results.deleted_count, result_history.deleted_count]):
            logger.info(
                "🧹 Limpieza histórica | base=%s user=%s results=%s history=%s",
                result_base.deleted_count,
                result_user.deleted_count,
                result_results.deleted_count,
                result_history.deleted_count,
            )

    except Exception as e:
        logger.error(f"❌ Error en cleanup_old_signals: {e}")

async def check_database_health():
    """Verifica la salud de la base de datos."""
    try:
        from app.database import get_client
        
        client = get_client()
        
        # Verificar conexión
        client.admin.command('ping')
        
        # Verificar colecciones principales (las críticas)
        db = client.get_default_database()
        collections = db.list_collection_names()
        
        required_collections = ['users', 'signals', 'user_signals']
        missing = [col for col in required_collections if col not in collections]
        
        if missing:
            logger.warning(f"⚠️ Colecciones faltantes: {missing}")
        else:
            logger.debug("✅ Base de datos saludable")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en check_database_health: {e}")
        return False

# ======================================================
# LOOP PRINCIPAL DEL SCHEDULER (SIN FUNCIONES CON BOT)
# ======================================================

async def scheduler_loop():
    """Loop principal del scheduler - SIN USAR BOT para evitar errores de event loop."""
    logger.info("⏰ Scheduler iniciado correctamente (modo seguro)")
    
    iteration = 0
    errors_in_row = 0
    max_errors_in_row = 5
    
    while True:
        try:
            # Tarea 1: Revisar planes expirados (sin notificaciones por ahora)
            processed = await check_expired_plans()
            if processed > 0:
                logger.info(f"📋 Procesados {processed} planes expirados (actualizados a FREE)")
            
            # Tarea 2: evaluación automática y snapshots de estadísticas.
            # Se ejecuta siempre para que el módulo de rendimiento no dependa
            # de que un usuario abra una pantalla.
            refresh_now = (iteration % STATS_REFRESH_EVERY_LOOPS == 0)
            if refresh_now:
                run_statistics_cycle(evaluation_limit=EVALUATION_LIMIT)
            else:
                from app.signals import evaluate_expired_signals
                evaluated = evaluate_expired_signals(limit=EVALUATION_LIMIT)
                if evaluated:
                    logger.info("📊 Evaluación automática completada | evaluated=%s", evaluated)

            if iteration % HISTORY_BACKFILL_EVERY_LOOPS == 0:
                backfilled = backfill_signal_history(limit=EVALUATION_LIMIT)
                if backfilled:
                    logger.info("🧾 Backfill de histórico ejecutado | processed=%s", backfilled)

            # Tarea 3: Cada hora: limpiar señales antiguas (5 min * 12 = 60 min)
            if iteration % 12 == 0:
                await cleanup_old_signals()

            # Tarea 4: Cada 6 horas: verificar salud de base de datos (5 min * 72 = 6 horas)
            if iteration % 72 == 0:
                await check_database_health()

            iteration += 1
            errors_in_row = 0  # Reset error counter
            
            # Esperar intervalo
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            
        except asyncio.CancelledError:
            logger.info("🛑 Scheduler cancelado")
            break
        except Exception as e:
            errors_in_row += 1
            logger.error(f"❌ Error en scheduler loop (error #{errors_in_row}): {e}", exc_info=True)
            
            if errors_in_row >= max_errors_in_row:
                logger.critical(f"🚨 Demasiados errores consecutivos ({errors_in_row}), reiniciando scheduler...")
                # Pequeño delay antes de continuar
                await asyncio.sleep(60)
                errors_in_row = 0
            else:
                # Esperar antes de reintentar
                await asyncio.sleep(30)
