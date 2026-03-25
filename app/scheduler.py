# app/scheduler.py

import os
import asyncio
import logging
from datetime import datetime, timedelta

from app.database import users_collection, signals_collection, user_signals_collection
from app.plans import PLAN_FREE

logger = logging.getLogger(__name__)

# ======================================================
# CONFIGURACIÓN (VARIABLES DE ENTORNO)
# ======================================================

CHECK_INTERVAL_SECONDS = int(os.getenv("SCHEDULER_CHECK_INTERVAL", "300"))  # 5 min por defecto
BATCH_SIZE = int(os.getenv("SCHEDULER_BATCH_SIZE", "100"))  # Usuarios por batch

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
    """Limpia señales antiguas de la base de datos."""
    try:
        # Señales base: mantener 7 días
        cutoff_date = datetime.utcnow() - timedelta(days=7)
        
        result_base = signals_collection().delete_many({
            "created_at": {"$lt": cutoff_date}
        })
        
        # Señales de usuario: mantener 3 días
        cutoff_user = datetime.utcnow() - timedelta(days=3)
        result_user = user_signals_collection().delete_many({
            "created_at": {"$lt": cutoff_user}
        })
        
        if result_base.deleted_count > 0 or result_user.deleted_count > 0:
            logger.info(f"🧹 Limpieza: {result_base.deleted_count} señales base, "
                       f"{result_user.deleted_count} señales usuario")
            
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
            
            # Tarea 2: Cada hora: limpiar señales antiguas (5 min * 12 = 60 min)
            if iteration % 12 == 0:
                await cleanup_old_signals()
            
            # Tarea 3: Cada 6 horas: verificar salud de base de datos (5 min * 72 = 6 horas)
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
