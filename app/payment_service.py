from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional
from uuid import uuid4

from pymongo import ReturnDocument

from app.bep20_verifier import VerificationConfigError, verify_payment
from app.config import (
    get_payment_network,
    get_payment_order_ttl_minutes,
    get_payment_receiver_address,
    get_payment_token_contract,
    get_payment_token_symbol,
)
from app.database import payment_orders_collection, payment_verification_logs_collection
from app.models import new_payment_order, new_payment_verification_log, update_timestamp, utcnow
from app.plans import activate_plan_purchase, get_plan_price, normalize_plan

logger = logging.getLogger(__name__)

OPEN_ORDER_STATUSES = {"awaiting_payment", "verification_in_progress", "paid_unconfirmed"}
_DECIMAL_QUANT = Decimal("0.001")


def _quantize_amount(value: Decimal) -> Decimal:
    return value.quantize(_DECIMAL_QUANT, rounding=ROUND_DOWN)


def _next_unique_amount(base_price: float) -> Decimal:
    orders = payment_orders_collection()
    base = Decimal(str(base_price))
    for suffix_int in range(1, 1000):
        amount = _quantize_amount(base + (Decimal(suffix_int) / Decimal("1000")))
        exists = orders.find_one({"amount_usdt": float(amount), "status": {"$in": list(OPEN_ORDER_STATUSES)}})
        if not exists:
            return amount
    raise RuntimeError("No se pudo generar un monto único de pago")


def create_payment_order(user_id: int, plan: str, days: int) -> Dict[str, Any]:
    user_id = int(user_id)
    plan = normalize_plan(plan)
    days = int(days)
    base_price = get_plan_price(plan, days)
    if base_price <= 0:
        raise ValueError("Precio inválido para el plan seleccionado")

    cancel_open_orders_for_user(user_id, reason="superseded_by_new_order")

    amount = _next_unique_amount(base_price)
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
    return bool(result.modified_count)


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
    return int(result.modified_count or 0)


def confirm_payment_order(order_id: str, user_id: int) -> Dict[str, Any]:
    order = get_payment_order(order_id, user_id=user_id)
    if not order:
        return {"ok": False, "reason": "order_not_found"}

    if order.get("status") == "completed":
        return {"ok": True, "reason": "already_completed", "order": order}

    if order.get("status") == "cancelled":
        return {"ok": False, "reason": "order_cancelled", "order": order}

    if order.get("expires_at") and order["expires_at"] < utcnow():
        payment_orders_collection().update_one(
            {"order_id": order_id},
            {"$set": {"status": "expired", "last_verification_reason": "order_expired", "updated_at": utcnow()}},
        )
        return {"ok": False, "reason": "order_expired"}

    payment_orders_collection().update_one(
        {"order_id": order_id},
        {"$set": {"status": "verification_in_progress", "updated_at": utcnow()}, "$inc": {"verification_attempts": 1}},
    )
    order = get_payment_order(order_id, user_id=user_id) or order

    try:
        verification = verify_payment(order)
    except VerificationConfigError as exc:
        logger.error("Configuración de pagos incompleta: %s", exc)
        payment_orders_collection().update_one(
            {"order_id": order_id},
            {"$set": {"status": "awaiting_payment", "last_verification_reason": "payment_config_missing", "updated_at": utcnow()}},
        )
        return {"ok": False, "reason": "payment_config_missing", "message": str(exc)}
    except Exception as exc:
        logger.error("Error verificando pago %s: %s", order_id, exc, exc_info=True)
        payment_orders_collection().update_one(
            {"order_id": order_id},
            {"$set": {"status": "awaiting_payment", "last_verification_reason": "verification_error", "updated_at": utcnow()}},
        )
        return {"ok": False, "reason": "verification_error"}

    _write_verification_log(order, verification)

    status = verification.get("status")
    if status == "not_found":
        payment_orders_collection().update_one(
            {"order_id": order_id},
            {"$set": {"status": "awaiting_payment", "last_verification_reason": verification.get("reason"), "updated_at": utcnow()}},
        )
        return {"ok": False, "reason": verification.get("reason"), "order": get_payment_order(order_id, user_id=user_id)}

    if status == "unconfirmed":
        payment_orders_collection().update_one(
            {"order_id": order_id},
            {
                "$set": {
                    "status": "paid_unconfirmed",
                    "last_verification_reason": verification.get("reason"),
                    "matched_tx_hash": verification.get("tx_hash"),
                    "matched_from": verification.get("from_address"),
                    "matched_to": verification.get("to_address"),
                    "matched_amount": verification.get("amount_usdt"),
                    "confirmations": int(verification.get("confirmations") or 0),
                    "updated_at": utcnow(),
                }
            },
        )
        return {"ok": False, "reason": verification.get("reason"), "order": get_payment_order(order_id, user_id=user_id)}

    tx_hash = verification.get("tx_hash")
    duplicate = payment_orders_collection().find_one({
        "matched_tx_hash": tx_hash,
        "order_id": {"$ne": order_id},
        "status": "completed",
    })
    if duplicate:
        payment_orders_collection().update_one(
            {"order_id": order_id},
            {"$set": {"status": "awaiting_payment", "last_verification_reason": "tx_already_used", "updated_at": utcnow()}},
        )
        return {"ok": False, "reason": "tx_already_used"}

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
        payment_orders_collection().update_one(
            {"order_id": order_id},
            {"$set": {"status": "awaiting_payment", "last_verification_reason": "activation_failed", "updated_at": utcnow()}},
        )
        return {"ok": False, "reason": "activation_failed"}

    updated = payment_orders_collection().find_one_and_update(
        {"order_id": order_id},
        {
            "$set": {
                "status": "completed",
                "last_verification_reason": verification.get("reason"),
                "matched_tx_hash": tx_hash,
                "matched_from": verification.get("from_address"),
                "matched_to": verification.get("to_address"),
                "matched_amount": verification.get("amount_usdt"),
                "confirmations": int(verification.get("confirmations") or 0),
                "confirmed_at": utcnow(),
                "updated_at": utcnow(),
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    logger.info("✅ Pago confirmado y plan activado | user=%s order=%s tx=%s", user_id, order_id, tx_hash)
    return {"ok": True, "reason": "payment_confirmed", "order": updated, "verification": verification}
