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
from app.observability import heartbeat, log_event, record_audit_event
from app.payment_service import expire_stale_payment_orders

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

    expired_users = users_col.find(
        {
            "plan_end": {"$lt": now, "$ne": None},
            "plan": {"$ne": PLAN_FREE}
        }
    ).limit(BATCH_SIZE)

    processed_count = 0

    for user in expired_users:
        try:
            user_id = user["user_id"]
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
            heartbeat(
                "scheduler",
                status="ok",
                details={
                    "cleanup_base": result_base.deleted_count,
                    "cleanup_user": result_user.deleted_count,
                    "cleanup_results": result_results.deleted_count,
                    "cleanup_history": result_history.deleted_count,
                },
            )

    except Exception as e:
        logger.error(f"❌ Error en cleanup_old_signals: {e}")
        heartbeat("scheduler", status="degraded", details={"cleanup_error": str(e)})


async def check_database_health():
    """Verifica la salud de la base de datos."""
    try:
        from app.database import get_client

        client = get_client()
        client.admin.command('ping')

        db = client.get_default_database()
        collections = db.list_collection_names()

        required_collections = ['users', 'signals', 'user_signals', 'payment_orders']
        missing = [col for col in required_collections if col not in collections]

        if missing:
            logger.warning(f"⚠️ Colecciones faltantes: {missing}")
            heartbeat("database", status="degraded", details={"missing_collections": missing})
            return False

        heartbeat("database", status="ok", details={"collections": len(collections)})
        logger.debug("✅ Base de datos saludable")
        return True

    except Exception as e:
        logger.error(f"❌ Error en check_database_health: {e}")
        heartbeat("database", status="error", details={"error": str(e)})
        return False

# ======================================================
# LOOP PRINCIPAL DEL SCHEDULER (SIN FUNCIONES CON BOT)
# ======================================================

async def scheduler_loop():
    """Loop principal del scheduler - SIN USAR BOT para evitar errores de event loop."""
    logger.info("⏰ Scheduler iniciado correctamente (modo seguro)")
    heartbeat("scheduler", status="ok", details={"stage": "started"})
    record_audit_event(
        event_type="scheduler_started",
        status="info",
        module="scheduler",
        message="scheduler_started",
    )

    iteration = 0
    errors_in_row = 0
    max_errors_in_row = 5

    while True:
        try:
            expired_orders = expire_stale_payment_orders()
            if expired_orders > 0:
                logger.info("💳 Órdenes de pago expiradas | count=%s", expired_orders)

            processed = await check_expired_plans()
            if processed > 0:
                logger.info(f"📋 Procesados {processed} planes expirados (actualizados a FREE)")
                record_audit_event(
                    event_type="expired_plans_processed",
                    status="info",
                    module="scheduler",
                    message=f"processed={processed}",
                    metadata={"processed": processed},
                )

            refresh_now = (iteration % STATS_REFRESH_EVERY_LOOPS == 0)
            if refresh_now:
                run_statistics_cycle(evaluation_limit=EVALUATION_LIMIT)
                heartbeat("statistics", status="ok", details={"mode": "refresh_cycle", "iteration": iteration})
            else:
                from app.signals import evaluate_expired_signals
                evaluated = evaluate_expired_signals(limit=EVALUATION_LIMIT)
                if evaluated:
                    logger.info("📊 Evaluación automática completada | evaluated=%s", evaluated)
                heartbeat("statistics", status="ok", details={"mode": "evaluate_only", "iteration": iteration, "evaluated": evaluated or 0})

            if iteration % HISTORY_BACKFILL_EVERY_LOOPS == 0:
                backfilled = backfill_signal_history(limit=EVALUATION_LIMIT)
                if backfilled:
                    logger.info("🧾 Backfill de histórico ejecutado | processed=%s", backfilled)
                heartbeat("history", status="ok", details={"iteration": iteration, "backfilled": backfilled or 0})

            if iteration % 12 == 0:
                await cleanup_old_signals()

            if iteration % 72 == 0:
                await check_database_health()

            iteration += 1
            errors_in_row = 0
            heartbeat("scheduler", status="ok", details={"iteration": iteration, "interval_seconds": CHECK_INTERVAL_SECONDS})

            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            logger.info("🛑 Scheduler cancelado")
            heartbeat("scheduler", status="stopped", details={"reason": "cancelled"})
            record_audit_event(
                event_type="scheduler_stopped",
                status="warning",
                module="scheduler",
                message="scheduler_stopped",
                metadata={"reason": "cancelled"},
            )
            break
        except Exception as e:
            errors_in_row += 1
            logger.error(f"❌ Error en scheduler loop (error #{errors_in_row}): {e}", exc_info=True)
            record_audit_event(
                event_type="scheduler_loop_error",
                status="error",
                module="scheduler",
                message=str(e),
                metadata={"errors_in_row": errors_in_row},
            )
            heartbeat("scheduler", status="error", details={"error": str(e), "errors_in_row": errors_in_row})

            if errors_in_row >= max_errors_in_row:
                logger.critical(f"🚨 Demasiados errores consecutivos ({errors_in_row}), reiniciando scheduler...")
                await asyncio.sleep(60)
                errors_in_row = 0
            else:
                await asyncio.sleep(30)


def run_scheduler_worker() -> None:
    from app.database import initialize_database

    initialize_database()
    heartbeat("database", status="ok", details={"stage": "initialized"})
    heartbeat("scheduler", status="starting", details={"mode": "dedicated_process"})
    try:
        record_audit_event(
            event_type="scheduler_worker_started",
            status="info",
            module="scheduler",
            message="scheduler_worker_started",
        )
        logger.info("⏰ Iniciando scheduler dedicado...")
        asyncio.run(scheduler_loop())
    except Exception as exc:
        heartbeat("scheduler", status="error", details={"error": str(exc), "mode": "dedicated_process"})
        record_audit_event(
            event_type="scheduler_worker_crashed",
            status="error",
            module="scheduler",
            message=str(exc),
        )
        logger.error("❌ Scheduler worker falló: %s", exc, exc_info=True)
        raise
