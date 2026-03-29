from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from bson import ObjectId
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from telegram import Bot

from app.database import (
    signal_deliveries_collection,
    signal_jobs_collection,
    signals_collection,
    user_signals_collection,
)
from app.models import new_signal_delivery, new_signal_job
from app.notifier import _eligible_users_for_alert, send_signal_alerts
from app.observability import heartbeat, record_audit_event
from app.signals import build_user_signal_document

logger = logging.getLogger(__name__)

PIPELINE_RETRY_LIMIT = int(os.getenv("SIGNAL_PIPELINE_RETRY_LIMIT", "3"))
PIPELINE_RETRY_BASE_SECONDS = int(os.getenv("SIGNAL_PIPELINE_RETRY_BASE_SECONDS", "2"))
PIPELINE_RECOVERY_LIMIT = int(os.getenv("SIGNAL_PIPELINE_RECOVERY_LIMIT", "200"))

_dispatch_queue: "queue.Queue[str]" = queue.Queue()
_started = False
_bot: Optional[Bot] = None
_worker_thread: Optional[threading.Thread] = None


def initialize_signal_pipeline(bot: Bot) -> None:
    global _started, _bot, _worker_thread
    if _started:
        return

    _bot = bot
    _recover_pending_jobs()
    _worker_thread = threading.Thread(
        target=_run_pipeline_loop,
        daemon=True,
        name="SignalDispatchThread",
    )
    _worker_thread.start()
    _started = True
    heartbeat("signal_pipeline", status="ok", details={"stage": "started"})
    logger.info("✅ Signal pipeline en tiempo real iniciada")


def enqueue_signal_dispatch(base_signal: Dict) -> Optional[str]:
    signal_id = str(base_signal.get("_id") or "")
    visibility = str(base_signal.get("visibility") or "")
    if not signal_id or not visibility:
        logger.error("❌ No pude encolar señal: falta _id o visibility")
        return None

    now = datetime.utcnow()
    job = new_signal_job(signal_id=signal_id, visibility=visibility)
    job_id = signal_jobs_collection().insert_one(job).inserted_id

    signals_collection().update_one(
        {"_id": ObjectId(signal_id)},
        {
            "$set": {
                "dispatch_status": "queued",
                "dispatch_job_id": str(job_id),
                "dispatch_enqueued_at": now,
                "updated_at": now,
            }
        },
    )

    _dispatch_queue.put(str(job_id))
    logger.info("⚡ Señal %s encolada para despacho inmediato (job=%s)", signal_id, job_id)
    return str(job_id)


def _recover_pending_jobs() -> None:
    now = datetime.utcnow()
    pending = signal_jobs_collection().find(
        {
            "$or": [
                {"status": "queued"},
                {"status": "retry", "next_retry_at": {"$lte": now}},
                {"status": "processing"},
            ]
        },
        sort=[("enqueued_at", 1)],
        limit=PIPELINE_RECOVERY_LIMIT,
    )
    recovered = 0
    for job in pending:
        _dispatch_queue.put(str(job["_id"]))
        recovered += 1
    if recovered:
        heartbeat("signal_pipeline", status="degraded", details={"recovered_jobs": recovered})
        logger.warning("♻️ Signal pipeline recuperó %s jobs pendientes", recovered)


def _run_pipeline_loop() -> None:
    while True:
        job_id = _dispatch_queue.get()
        try:
            _process_job(job_id)
        except Exception as exc:
            heartbeat("signal_pipeline", status="error", details={"job_id": job_id, "error": str(exc)})
            record_audit_event(event_type="signal_pipeline_job_error", status="error", module="signal_pipeline", signal_id=None, order_id=None, message=str(exc), metadata={"job_id": job_id})
            logger.error("❌ Error no controlado en pipeline job=%s: %s", job_id, exc, exc_info=True)
        finally:
            _dispatch_queue.task_done()


def _claim_job(job_id: str) -> Optional[Dict]:
    now = datetime.utcnow()
    return signal_jobs_collection().find_one_and_update(
        {
            "_id": ObjectId(job_id),
            "status": {"$in": ["queued", "retry", "processing"]},
        },
        {
            "$set": {
                "status": "processing",
                "processing_started_at": now,
                "updated_at": now,
            }
        },
        return_document=True,
    )


def _mark_signal_dispatch(signal_id: str, **fields) -> None:
    payload = {**fields, "updated_at": datetime.utcnow()}
    signals_collection().update_one({"_id": ObjectId(signal_id)}, {"$set": payload})


def _build_user_signal_ops(base_signal: Dict, user_ids: List[int]) -> List[UpdateOne]:
    ops: List[UpdateOne] = []
    signal_id = str(base_signal["_id"])
    for user_id in user_ids:
        doc = build_user_signal_document(base_signal, int(user_id))
        ops.append(
            UpdateOne(
                {"user_id": int(user_id), "signal_id": signal_id},
                {"$setOnInsert": doc},
                upsert=True,
            )
        )
    return ops


def _upsert_user_signals(base_signal: Dict, user_ids: List[int]) -> None:
    if not user_ids:
        return
    ops = _build_user_signal_ops(base_signal, user_ids)
    try:
        user_signals_collection().bulk_write(ops, ordered=False)
    except BulkWriteError as exc:
        logger.warning("⚠️ Bulk write de user_signals con conflictos tolerados: %s", exc.details)


def _ensure_delivery_records(signal_id: str, visibility: str, user_ids: List[int]) -> None:
    if not user_ids:
        return
    ops: List[UpdateOne] = []
    for user_id in user_ids:
        delivery_doc = new_signal_delivery(signal_id=signal_id, user_id=int(user_id), visibility=visibility)
        ops.append(
            UpdateOne(
                {"signal_id": signal_id, "user_id": int(user_id)},
                {
                    "$setOnInsert": delivery_doc,
                    "$set": {"updated_at": datetime.utcnow()},
                },
                upsert=True,
            )
        )
    try:
        signal_deliveries_collection().bulk_write(ops, ordered=False)
    except BulkWriteError as exc:
        logger.warning("⚠️ Bulk write de deliveries con conflictos tolerados: %s", exc.details)


async def _dispatch_pushes(base_signal: Dict, user_ids: List[int]) -> Dict[str, object]:
    if _bot is None:
        raise RuntimeError("Signal pipeline no tiene bot configurado")
    return await send_signal_alerts(_bot, str(base_signal["visibility"]), user_ids=user_ids)


def _schedule_retry(job: Dict, error: str) -> None:
    attempt_count = int(job.get("attempt_count", 0)) + 1
    if attempt_count >= PIPELINE_RETRY_LIMIT:
        signal_jobs_collection().update_one(
            {"_id": job["_id"]},
            {
                "$set": {
                    "status": "failed",
                    "last_error": error,
                    "processing_finished_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                },
                "$inc": {"attempt_count": 1},
            },
        )
        _mark_signal_dispatch(job["signal_id"], dispatch_status="failed", dispatch_error=error)
        heartbeat("signal_pipeline", status="error", details={"signal_id": job["signal_id"], "error": error})
        record_audit_event(event_type="signal_dispatch_failed", status="error", module="signal_pipeline", signal_id=job["signal_id"], message=error, metadata={"job_id": str(job["_id"]), "attempts": attempt_count})
        return

    delay_seconds = PIPELINE_RETRY_BASE_SECONDS * attempt_count
    next_retry_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
    signal_jobs_collection().update_one(
        {"_id": job["_id"]},
        {
            "$set": {
                "status": "retry",
                "last_error": error,
                "next_retry_at": next_retry_at,
                "processing_finished_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            },
            "$inc": {"attempt_count": 1},
        },
    )
    _mark_signal_dispatch(job["signal_id"], dispatch_status="retry", dispatch_error=error)

    def _requeue() -> None:
        _dispatch_queue.put(str(job["_id"]))

    timer = threading.Timer(delay_seconds, _requeue)
    timer.daemon = True
    timer.start()
    heartbeat("signal_pipeline", status="degraded", details={"job_id": str(job["_id"]), "retry_in_seconds": delay_seconds, "attempt": attempt_count})
    logger.warning(
        "🔁 Job %s programado para retry en %ss (attempt=%s)",
        job["_id"],
        delay_seconds,
        attempt_count,
    )


def _update_delivery_results(signal_id: str, push_results: Dict[str, object]) -> None:
    now = datetime.utcnow()
    for result in push_results.get("results", []):
        status = result.get("status")
        user_id = int(result["user_id"])
        update = {
            "status": status,
            "updated_at": now,
            "error": result.get("error"),
        }
        if status == "sent":
            update["sent_at"] = result.get("sent_at") or now
        signal_deliveries_collection().update_one(
            {"signal_id": signal_id, "user_id": user_id},
            {"$set": update},
        )


def _process_job(job_id: str) -> None:
    job = _claim_job(job_id)
    if not job:
        return

    signal_id = str(job["signal_id"])
    now = datetime.utcnow()
    _mark_signal_dispatch(signal_id, dispatch_status="processing", fanout_started_at=now)

    signal_doc = signals_collection().find_one({"_id": ObjectId(signal_id)})
    if not signal_doc:
        _schedule_retry(job, "base_signal_not_found")
        return

    user_ids = _eligible_users_for_alert(str(signal_doc["visibility"]))
    signal_jobs_collection().update_one(
        {"_id": job["_id"]},
        {
            "$set": {
                "eligible_users": len(user_ids),
                "updated_at": datetime.utcnow(),
            }
        },
    )

    try:
        _upsert_user_signals(signal_doc, user_ids)
        _ensure_delivery_records(signal_id, str(signal_doc["visibility"]), user_ids)

        push_results = asyncio.run(_dispatch_pushes(signal_doc, user_ids))
        _update_delivery_results(signal_id, push_results)

        finished_at = datetime.utcnow()
        signal_jobs_collection().update_one(
            {"_id": job["_id"]},
            {
                "$set": {
                    "status": "completed",
                    "processing_finished_at": finished_at,
                    "updated_at": finished_at,
                    "requested_users": int(push_results.get("requested", 0)),
                    "sent_users": int(push_results.get("sent", 0)),
                    "failed_users": int(push_results.get("failed", 0)),
                    "first_push_at": push_results.get("first_push_at"),
                    "last_push_at": push_results.get("last_push_at"),
                }
            },
        )
        _mark_signal_dispatch(
            signal_id,
            dispatch_status="completed",
            dispatch_completed_at=finished_at,
            dispatch_requested_users=int(push_results.get("requested", 0)),
            dispatch_sent_users=int(push_results.get("sent", 0)),
            dispatch_failed_users=int(push_results.get("failed", 0)),
            first_push_at=push_results.get("first_push_at"),
            last_push_at=push_results.get("last_push_at"),
        )
        logger.info(
            "⚡ Señal %s despachada | users=%s sent=%s failed=%s",
            signal_id,
            push_results.get("requested", 0),
            push_results.get("sent", 0),
            push_results.get("failed", 0),
        )
    except Exception as exc:
        _schedule_retry(job, str(exc))
        logger.error("❌ Falló dispatch de señal %s: %s", signal_id, exc, exc_info=True)
