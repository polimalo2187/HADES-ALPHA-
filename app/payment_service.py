from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional
from uuid import uuid4

from pymongo import ReturnDocument

from app.bep20_verifier import VerificationConfigError, verify_payment
from app.config import (
    get_payment_configuration_status,
    get_payment_network,
    get_payment_order_ttl_minutes,
    get_payment_receiver_address,
    get_payment_token_contract,
    get_payment_token_symbol,
)
from app.database import payment_orders_collection, payment_verification_logs_collection, subscription_events_collection
from app.models import new_payment_order, new_payment_verification_log, update_timestamp, utcnow
from app.observability import heartbeat, record_audit_event
from app.plans import activate_plan_purchase, get_plan_price, normalize_plan, validate_plan_duration

logger = logging.getLogger(__name__)

OPEN_ORDER_STATUSES = {"awaiting_payment", "verification_in_progress", "paid_unconfirmed"}
VERIFYABLE_ORDER_STATUSES = {"awaiting_payment", "paid_unconfirmed"}
VERIFICATION_LOCK_STALE_AFTER = timedelta(minutes=3)
_DECIMAL_QUANT = Decimal("0.001")


def _quantize_amount(value: Decimal) -> Decimal:
    return value.quantize(_DECIMAL_QUANT, rounding=ROUND_DOWN)


def format_payment_amount(value: Decimal | float | str) -> str:
    return f"{_quantize_amount(Decimal(str(value))):.3f}"


def build_unique_amount_candidates(base_price: float, user_id: int, *, limit: int = 999) -> list[Decimal]:
    try:
        base = Decimal(str(base_price))
    except Exception as exc:
        raise ValueError("Precio base inválido") from exc
    if base <= 0:
        raise ValueError("Precio base inválido")
    user_id = int(user_id)
    if user_id <= 0:
        raise ValueError("user_id inválido")
    max_candidates = max(1, min(int(limit), 999))
    start_suffix = user_id % 999
    if start_suffix <= 0:
        start_suffix = 1
    candidates: list[Decimal] = []
    for offset in range(0, max_candidates):
        suffix_int = ((start_suffix + offset - 1) % 999) + 1
        candidates.append(_quantize_amount(base + (Decimal(suffix_int) / Decimal("1000"))))
    return candidates


def _next_unique_amount(base_price: float, user_id: int) -> Decimal:
    orders = payment_orders_collection()
    for amount in build_unique_amount_candidates(base_price, user_id):
        exists = orders.find_one({
            "amount_usdt": float(amount),
            "status": {"$in": list(OPEN_ORDER_STATUSES)},
        })
        if not exists:
            return amount
    raise RuntimeError("No se pudo generar un monto único de pago")


def _is_order_expired(order: Optional[Dict[str, Any]], *, now: Optional[datetime] = None) -> bool:
    if not order:
        return False
    expires_at = order.get("expires_at")
    if not isinstance(expires_at, datetime):
        return False
    return expires_at < (now or utcnow())


def _mark_order_status(order_id: str, status: str, *, reason: Optional[str] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    payload: Dict[str, Any] = {"status": status, "updated_at": utcnow(), "verification_lock_token": None, "verification_started_at": None}
    if reason is not None:
        payload["last_verification_reason"] = reason
    if extra:
        payload.update(extra)
    payment_orders_collection().update_one({"order_id": str(order_id)}, {"$set": payload})


def _load_effective_active_order(user_id: int) -> Optional[Dict[str, Any]]:
    order = get_active_payment_order_for_user(user_id)
    if order and _is_order_expired(order):
        _mark_order_status(str(order.get("order_id")), "expired", reason="order_expired")
        return None
    return order


def _payment_purchase_already_applied(order_id: str, user_id: int) -> bool:
    try:
        existing = subscription_events_collection().find_one({
            "user_id": int(user_id),
            "event_type": "purchase",
            "source": "payment_bep20",
            "metadata.order_id": str(order_id),
        })
        return bool(existing)
    except Exception:
        return False


def _finalize_completed_order_if_needed(order_id: str, *, tx_hash: Optional[str] = None, verification: Optional[Dict[str, Any]] = None, reason: str = "payment_confirmed") -> Optional[Dict[str, Any]]:
    verification = verification or {}
    now = utcnow()
    update_doc = {
        "status": "completed",
        "last_verification_reason": reason,
        "matched_tx_hash": tx_hash or verification.get("tx_hash"),
        "matched_from": verification.get("from_address"),
        "matched_to": verification.get("to_address"),
        "matched_amount": verification.get("amount_usdt"),
        "confirmations": int(verification.get("confirmations") or 0),
        "confirmed_at": now,
        "verification_lock_token": None,
        "verification_started_at": None,
        "updated_at": now,
    }
    return payment_orders_collection().find_one_and_update(
        {"order_id": str(order_id), "status": {"$ne": "completed"}},
        {"$set": update_doc},
        return_document=ReturnDocument.AFTER,
    ) or get_payment_order(str(order_id))


def _acquire_verification_lock(order_id: str, user_id: int) -> tuple[Optional[Dict[str, Any]], str]:
    now = utcnow()
    lock_token = uuid4().hex[:16]
    stale_before = now - VERIFICATION_LOCK_STALE_AFTER
    order = payment_orders_collection().find_one_and_update(
        {
            "order_id": str(order_id),
            "user_id": int(user_id),
            "$or": [
                {"status": {"$in": list(VERIFYABLE_ORDER_STATUSES)}},
                {"status": "verification_in_progress", "verification_started_at": {"$lt": stale_before}},
                {"status": "verification_in_progress", "verification_started_at": {"$exists": False}},
            ],
        },
        {
            "$set": {
                "status": "verification_in_progress",
                "verification_started_at": now,
                "verification_lock_token": lock_token,
                "updated_at": now,
            },
            "$inc": {"verification_attempts": 1},
        },
        return_document=ReturnDocument.AFTER,
    )
    return order, lock_token


def _update_locked_order(order_id: str, lock_token: str, values: Dict[str, Any]) -> None:
    payload = dict(values)
    payload.setdefault("updated_at", utcnow())
    payload.setdefault("verification_lock_token", None)
    payload.setdefault("verification_started_at", None)
    payment_orders_collection().update_one(
        {"order_id": str(order_id), "verification_lock_token": lock_token},
        {"$set": payload},
    )


def create_payment_order(user_id: int, plan: str, days: int) -> Dict[str, Any]:
    user_id = int(user_id)
    if user_id <= 0:
        raise ValueError("user_id inválido")

    payment_config = get_payment_configuration_status()
    if not payment_config.get("ready"):
        missing = ", ".join(str(item) for item in (payment_config.get("missing_keys") or []) if item)
        raise RuntimeError(f"Configuración de pagos incompleta: {missing or 'payment_config_missing'}")

    plan, days = validate_plan_duration(plan, days)
    base_price = get_plan_price(plan, days)
    if base_price <= 0:
        raise ValueError("Precio inválido para el plan seleccionado")

    existing_open = _load_effective_active_order(user_id)
    if existing_open:
        existing_plan = normalize_plan(existing_open.get("plan"))
        existing_days = int(existing_open.get("days") or 0)
        if existing_plan == plan and existing_days == int(days):
            record_audit_event(
                event_type="payment_order_reused",
                status="info",
                module="payments",
                user_id=user_id,
                order_id=existing_open.get("order_id"),
                message="payment_order_reused",
                metadata={"plan": plan, "days": days, "status": existing_open.get("status")},
            )
            return existing_open
        cancel_open_orders_for_user(user_id, reason="superseded_by_new_order")

    amount = _next_unique_amount(base_price, user_id)
    now = utcnow()
    order = new_payment_order(
        order_id=uuid4().hex[:12],
        user_id=user_id,
        plan=plan,
        days=days,
        base_price_usdt=float(base_price),
        amount_usdt=float(amount),
        network=get_payment_network(),
        token_symbol=get_payment_token_symbol(),
        token_contract=get_payment_token_contract(),
        deposit_address=get_payment_receiver_address(),
        expires_at=now + timedelta(minutes=get_payment_order_ttl_minutes()),
    )
    payment_orders_collection().insert_one(order)
    heartbeat("payments", status="ok", details={"stage": "order_created", "user_id": user_id, "order_id": order["order_id"]})
    record_audit_event(event_type="payment_order_created", status="info", module="payments", user_id=user_id, order_id=order["order_id"], message="payment_order_created", metadata={"plan": plan, "days": days, "amount_usdt": float(amount)})
    logger.info("💳 Orden de pago creada | user=%s plan=%s days=%s amount=%s", user_id, plan, days, amount)
    return order


def get_payment_order(order_id: str, *, user_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    query: Dict[str, Any] = {"order_id": str(order_id)}
    if user_id is not None:
        query["user_id"] = int(user_id)
    return payment_orders_collection().find_one(query)


def get_active_payment_order_for_user(user_id: int) -> Optional[Dict[str, Any]]:
    return payment_orders_collection().find_one(
        {"user_id": int(user_id), "status": {"$in": list(OPEN_ORDER_STATUSES)}},
        sort=[("created_at", -1)],
    )


def cancel_open_orders_for_user(user_id: int, *, reason: str = "cancelled_by_system") -> int:
    now = utcnow()
    result = payment_orders_collection().update_many(
        {"user_id": int(user_id), "status": {"$in": list(OPEN_ORDER_STATUSES)}},
        {"$set": {"status": "cancelled", "last_verification_reason": reason, "updated_at": now}},
    )
    return int(result.modified_count or 0)


def cancel_payment_order(order_id: str, user_id: int) -> bool:
    result = payment_orders_collection().update_one(
        {"order_id": str(order_id), "user_id": int(user_id), "status": {"$in": list(OPEN_ORDER_STATUSES)}},
        {"$set": {"status": "cancelled", "last_verification_reason": "cancelled_by_user", "updated_at": utcnow()}},
    )
    cancelled = bool(result.modified_count)
    if cancelled:
        record_audit_event(event_type="payment_order_cancelled", status="info", module="payments", user_id=user_id, order_id=order_id, message="payment_order_cancelled")
    return cancelled


def _write_verification_log(order: Dict[str, Any], verification: Dict[str, Any]) -> None:
    log_doc = new_payment_verification_log(
        order_id=order["order_id"],
        user_id=order["user_id"],
        status=verification.get("status") or "unknown",
        reason=verification.get("reason") or "unknown",
        tx_hash=verification.get("tx_hash"),
        from_address=verification.get("from_address"),
        to_address=verification.get("to_address"),
        amount_usdt=verification.get("amount_usdt"),
        confirmations=verification.get("confirmations"),
        raw=verification,
    )
    payment_verification_logs_collection().insert_one(log_doc)


def expire_stale_payment_orders() -> int:
    now = utcnow()
    result = payment_orders_collection().update_many(
        {"status": {"$in": list(OPEN_ORDER_STATUSES)}, "expires_at": {"$lt": now}},
        {"$set": {"status": "expired", "last_verification_reason": "order_expired", "updated_at": now}},
    )
    expired_count = int(result.modified_count or 0)
    if expired_count:
        heartbeat("payments", status="degraded", details={"expired_orders": expired_count})
    return expired_count


def confirm_payment_order(order_id: str, user_id: int) -> Dict[str, Any]:
    order = get_payment_order(order_id, user_id=user_id)
    if not order:
        return {"ok": False, "reason": "order_not_found"}

    if order.get("status") == "completed":
        return {"ok": True, "reason": "already_completed", "order": order}

    if order.get("status") == "cancelled":
        return {"ok": False, "reason": "order_cancelled", "order": order}

    if _is_order_expired(order):
        _mark_order_status(order_id, "expired", reason="order_expired")
        return {"ok": False, "reason": "order_expired", "order": get_payment_order(order_id, user_id=user_id)}

    if _payment_purchase_already_applied(order_id, user_id):
        finalized = _finalize_completed_order_if_needed(order_id, reason="activation_already_applied")
        return {"ok": True, "reason": "already_completed", "order": finalized or get_payment_order(order_id, user_id=user_id)}

    order, lock_token = _acquire_verification_lock(order_id, user_id)
    if not order:
        current = get_payment_order(order_id, user_id=user_id)
        if current and current.get("status") == "completed":
            return {"ok": True, "reason": "already_completed", "order": current}
        if current and current.get("status") == "verification_in_progress":
            return {"ok": False, "reason": "verification_in_progress", "order": current}
        if current and current.get("status") == "cancelled":
            return {"ok": False, "reason": "order_cancelled", "order": current}
        if current and _is_order_expired(current):
            _mark_order_status(order_id, "expired", reason="order_expired")
            return {"ok": False, "reason": "order_expired", "order": get_payment_order(order_id, user_id=user_id)}
        return {"ok": False, "reason": "order_not_available", "order": current}

    record_audit_event(event_type="payment_verification_started", status="info", module="payments", user_id=user_id, order_id=order_id, message="payment_verification_started")

    try:
        verification = verify_payment(order)
    except VerificationConfigError as exc:
        heartbeat("payments", status="error", details={"error": str(exc), "order_id": order_id})
        record_audit_event(event_type="payment_verification_failed", status="error", module="payments", user_id=user_id, order_id=order_id, message=str(exc), metadata={"reason": "payment_config_missing"})
        logger.error("Configuración de pagos incompleta: %s", exc)
        _update_locked_order(order_id, lock_token, {"status": "awaiting_payment", "last_verification_reason": "payment_config_missing"})
        return {"ok": False, "reason": "payment_config_missing", "message": str(exc)}
    except Exception as exc:
        heartbeat("payments", status="error", details={"error": str(exc), "order_id": order_id})
        record_audit_event(event_type="payment_verification_failed", status="error", module="payments", user_id=user_id, order_id=order_id, message=str(exc), metadata={"reason": "verification_error"})
        logger.error("Error verificando pago %s: %s", order_id, exc, exc_info=True)
        _update_locked_order(order_id, lock_token, {"status": "awaiting_payment", "last_verification_reason": "verification_error"})
        return {"ok": False, "reason": "verification_error"}

    _write_verification_log(order, verification)
    record_audit_event(event_type="payment_verification_result", status="info", module="payments", user_id=user_id, order_id=order_id, message=verification.get("reason"), metadata={"status": verification.get("status"), "confirmations": verification.get("confirmations"), "tx_hash": verification.get("tx_hash")})

    status = verification.get("status")
    if status == "not_found":
        _update_locked_order(order_id, lock_token, {"status": "awaiting_payment", "last_verification_reason": verification.get("reason")})
        heartbeat("payments", status="degraded", details={"order_id": order_id, "reason": verification.get("reason")})
        return {"ok": False, "reason": verification.get("reason"), "order": get_payment_order(order_id, user_id=user_id)}

    if status == "unconfirmed":
        _update_locked_order(order_id, lock_token, {
            "status": "paid_unconfirmed",
            "last_verification_reason": verification.get("reason"),
            "matched_tx_hash": verification.get("tx_hash"),
            "matched_from": verification.get("from_address"),
            "matched_to": verification.get("to_address"),
            "matched_amount": verification.get("amount_usdt"),
            "confirmations": int(verification.get("confirmations") or 0),
        })
        heartbeat("payments", status="degraded", details={"order_id": order_id, "reason": verification.get("reason")})
        return {"ok": False, "reason": verification.get("reason"), "order": get_payment_order(order_id, user_id=user_id)}

    tx_hash = verification.get("tx_hash")
    duplicate = payment_orders_collection().find_one({
        "matched_tx_hash": tx_hash,
        "order_id": {"$ne": order_id},
        "status": {"$in": ["verification_in_progress", "paid_unconfirmed", "completed"]},
    })
    if duplicate:
        _update_locked_order(order_id, lock_token, {"status": "awaiting_payment", "last_verification_reason": "tx_already_used"})
        record_audit_event(event_type="payment_duplicate_tx", status="warning", module="payments", user_id=user_id, order_id=order_id, message="tx_already_used", metadata={"tx_hash": tx_hash})
        return {"ok": False, "reason": "tx_already_used", "order": get_payment_order(order_id, user_id=user_id)}

    if _payment_purchase_already_applied(order_id, user_id):
        finalized = _finalize_completed_order_if_needed(order_id, tx_hash=tx_hash, verification=verification, reason="activation_already_applied")
        return {"ok": True, "reason": "already_completed", "order": finalized, "verification": verification}

    success = activate_plan_purchase(
        user_id=user_id,
        plan=order["plan"],
        days=int(order["days"]),
        source="payment_bep20",
        metadata={
            "order_id": order_id,
            "tx_hash": tx_hash,
            "amount_usdt": float(order["amount_usdt"]),
            "base_price_usdt": float(order["base_price_usdt"]),
            "network": order.get("network"),
            "token_symbol": order.get("token_symbol"),
        },
        trigger_referral=True,
    )
    if not success:
        _update_locked_order(order_id, lock_token, {"status": "awaiting_payment", "last_verification_reason": "activation_failed"})
        heartbeat("payments", status="error", details={"order_id": order_id, "reason": "activation_failed"})
        record_audit_event(event_type="payment_activation_failed", status="error", module="payments", user_id=user_id, order_id=order_id, message="activation_failed", metadata={"tx_hash": tx_hash})
        return {"ok": False, "reason": "activation_failed"}

    updated = _finalize_completed_order_if_needed(order_id, tx_hash=tx_hash, verification=verification, reason=verification.get("reason") or "payment_confirmed")
    heartbeat("payments", status="ok", details={"order_id": order_id, "tx_hash": tx_hash, "user_id": user_id})
    record_audit_event(event_type="payment_confirmed", status="success", module="payments", user_id=user_id, order_id=order_id, message="payment_confirmed", metadata={"tx_hash": tx_hash, "amount_usdt": float(order["amount_usdt"]), "plan": order["plan"], "days": int(order["days"])})
    logger.info("✅ Pago confirmado y plan activado | user=%s order=%s tx=%s", user_id, order_id, tx_hash)
    return {"ok": True, "reason": "payment_confirmed", "order": updated, "verification": verification}
