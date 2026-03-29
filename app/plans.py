from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import logging

from app.database import subscription_events_collection, users_collection
from app.models import activate_plan, is_plan_active, is_trial_active, new_subscription_event, update_timestamp, utcnow

logger = logging.getLogger(__name__)

# =========================
# CONSTANTES DE PLANES
# =========================

PLAN_FREE = "free"
PLAN_PLUS = "plus"
PLAN_PREMIUM = "premium"

PLAN_DURATION_DAYS = 30
PLAN_DURATION_OPTIONS = (7, 15, 21, 30)

SUBSCRIPTION_STATUS_FREE = "free"
SUBSCRIPTION_STATUS_TRIAL = "trial"
SUBSCRIPTION_STATUS_ACTIVE = "active"
SUBSCRIPTION_STATUS_EXPIRED = "expired"
SUBSCRIPTION_STATUS_BANNED = "banned"

PLAN_BASE_PRICES = {
    PLAN_PLUS: 15.0,
    PLAN_PREMIUM: 20.0,
}

# Precios por defecto: proporcionales al plan mensual base. Se pueden ajustar
# más adelante con variables de entorno o una fase de checkout dedicada.
PLAN_PRICE_TABLE = {
    PLAN_PLUS: {
        7: 3.5,
        15: 7.5,
        21: 10.5,
        30: 15.0,
    },
    PLAN_PREMIUM: {
        7: 4.67,
        15: 10.0,
        21: 14.0,
        30: 20.0,
    },
}

REFERRAL_REWARD_BY_DURATION = {
    7: 3,
    15: 7,
    21: 10,
    30: 15,
}


# =========================
# HELPERS DE USUARIO
# =========================

def get_user(user_id: int) -> Optional[dict]:
    return users_collection().find_one({"user_id": int(user_id)})



def save_user(user: dict):
    users_collection().update_one(
        {"user_id": user["user_id"]},
        {"$set": user},
        upsert=False,
    )



def plan_rank(plan: Optional[str]) -> int:
    plan_value = str(plan or PLAN_FREE).lower().strip()
    if plan_value == PLAN_PREMIUM:
        return 3
    if plan_value == PLAN_PLUS:
        return 2
    return 1



def normalize_plan(plan: Optional[str]) -> str:
    value = str(plan or PLAN_FREE).lower().strip()
    if value in {PLAN_PLUS, PLAN_PREMIUM}:
        return value
    return PLAN_FREE



def get_plan_name(plan: str) -> str:
    mapping = {
        PLAN_FREE: "FREE",
        PLAN_PLUS: "PLUS",
        PLAN_PREMIUM: "PREMIUM",
    }
    return mapping.get(normalize_plan(plan), "FREE")



def is_paid_plan(plan: Optional[str]) -> bool:
    return normalize_plan(plan) in {PLAN_PLUS, PLAN_PREMIUM}



def get_plan_duration_options(plan: Optional[str] = None) -> List[int]:
    return list(PLAN_DURATION_OPTIONS)



def get_plan_price(plan: str, days: int = PLAN_DURATION_DAYS) -> float:
    plan_value = normalize_plan(plan)
    day_value = int(days)
    return float(PLAN_PRICE_TABLE.get(plan_value, {}).get(day_value, 0.0))



def get_plan_catalog() -> Dict[str, List[Dict[str, Any]]]:
    catalog: Dict[str, List[Dict[str, Any]]] = {}
    for plan in (PLAN_PLUS, PLAN_PREMIUM):
        catalog[plan] = []
        for days in PLAN_DURATION_OPTIONS:
            catalog[plan].append(
                {
                    "plan": plan,
                    "days": days,
                    "price_usdt": get_plan_price(plan, days),
                }
            )
    return catalog



def get_referral_reward_days(purchased_days: int) -> int:
    return int(REFERRAL_REWARD_BY_DURATION.get(int(purchased_days), 0))



def can_count_as_valid_referral_purchase(plan: str, days: int) -> bool:
    return normalize_plan(plan) in {PLAN_PLUS, PLAN_PREMIUM} and int(days) in PLAN_DURATION_OPTIONS


# =========================
# VERIFICACIONES DE ESTADO
# =========================

def has_access(user: dict) -> bool:
    return is_plan_active(user) or is_trial_active(user)



def get_subscription_status(user: dict) -> str:
    if user.get("banned"):
        return SUBSCRIPTION_STATUS_BANNED
    if is_plan_active(user):
        return SUBSCRIPTION_STATUS_ACTIVE
    if is_trial_active(user):
        return SUBSCRIPTION_STATUS_TRIAL
    if normalize_plan(user.get("plan")) != PLAN_FREE:
        return SUBSCRIPTION_STATUS_EXPIRED
    return SUBSCRIPTION_STATUS_FREE



def plan_status(user: dict) -> dict:
    now = utcnow()

    if is_plan_active(user):
        return {
            "plan": normalize_plan(user.get("plan")),
            "status": SUBSCRIPTION_STATUS_ACTIVE,
            "expires": user.get("plan_end"),
            "days_left": max(((user.get("plan_end") or now) - now).days, 0),
        }

    if is_trial_active(user):
        return {
            "plan": PLAN_FREE,
            "status": SUBSCRIPTION_STATUS_TRIAL,
            "expires": user.get("trial_end"),
            "days_left": max(((user.get("trial_end") or now) - now).days, 0),
        }

    return {
        "plan": PLAN_FREE,
        "status": SUBSCRIPTION_STATUS_EXPIRED if normalize_plan(user.get("plan")) != PLAN_FREE else SUBSCRIPTION_STATUS_FREE,
        "expires": None,
        "days_left": 0,
    }



def can_access_feature(plan: Optional[str], feature_key: str, *, has_trial: bool = False, is_admin_user: bool = False) -> bool:
    if is_admin_user:
        return True
    if feature_key in {"signals_free", "risk_basic", "market_basic"}:
        return True
    if has_trial:
        return True

    rank = plan_rank(plan)
    if feature_key in {"signals_plus", "history", "watchlist_pro", "market_full", "radar", "movers", "risk_full"}:
        return rank >= plan_rank(PLAN_PLUS)
    if feature_key in {"signals_premium", "alerts_premium", "analysis_advanced", "tracking_advanced"}:
        return rank >= plan_rank(PLAN_PREMIUM)
    return rank >= plan_rank(PLAN_FREE)


# =========================
# AUDITORÍA DE MONETIZACIÓN
# =========================

def _record_subscription_event(
    *,
    user_id: int,
    event_type: str,
    plan: str,
    days: int,
    source: str,
    before_plan: Optional[str],
    after_plan: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        event = new_subscription_event(
            user_id=int(user_id),
            event_type=event_type,
            plan=normalize_plan(plan),
            days=int(days),
            source=str(source or "system"),
            before_plan=normalize_plan(before_plan),
            after_plan=normalize_plan(after_plan),
            metadata=metadata or {},
        )
        subscription_events_collection().insert_one(event)
    except Exception as exc:
        logger.error("❌ Error registrando subscription_event para %s: %s", user_id, exc, exc_info=True)


# =========================
# APLICACIÓN DE ENTITLEMENTS
# =========================

def _apply_entitlement_to_user(
    user: Dict[str, Any],
    *,
    target_plan: str,
    days: int,
    source: str,
    purchase: bool,
) -> Dict[str, Any]:
    now = utcnow()
    plan_value = normalize_plan(target_plan)
    current_plan = normalize_plan(user.get("plan"))
    current_active = is_plan_active(user)
    current_rank = plan_rank(current_plan if current_active else PLAN_FREE)
    target_rank = plan_rank(plan_value)

    base_end = user.get("plan_end") if current_active and user.get("plan_end") and user["plan_end"] > now else now

    # No degradamos planes por una activación/recompensa de tier inferior.
    if current_active and target_rank < current_rank:
        effective_plan = current_plan
    else:
        effective_plan = plan_value

    # Para upgrades conservamos el tiempo restante y sumamos los nuevos días.
    user["plan"] = effective_plan
    user["plan_end"] = base_end + timedelta(days=int(days))
    user["trial_end"] = None
    user["subscription_status"] = SUBSCRIPTION_STATUS_ACTIVE
    user["plan_started_at"] = user.get("plan_started_at") or now
    user["last_plan_change_at"] = now
    user["last_entitlement_source"] = source

    if purchase:
        user["last_purchase_at"] = now
        user["last_purchase_plan"] = plan_value
        user["last_purchase_days"] = int(days)

    return update_timestamp(user)



def grant_plan_entitlement(
    user_id: int,
    *,
    target_plan: str,
    days: int,
    source: str = "system",
    reason: str = "manual_activation",
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    try:
        user = get_user(user_id)
        if not user:
            logger.warning("Usuario %s no encontrado para grant_plan_entitlement", user_id)
            return False

        if int(days) <= 0:
            logger.warning("Días inválidos para entitlement de %s: %s", user_id, days)
            return False

        before_plan = normalize_plan(user.get("plan"))
        updated_user = _apply_entitlement_to_user(
            user,
            target_plan=target_plan,
            days=int(days),
            source=source,
            purchase=reason == "purchase",
        )
        save_user(updated_user)

        _record_subscription_event(
            user_id=user_id,
            event_type=reason,
            plan=target_plan,
            days=int(days),
            source=source,
            before_plan=before_plan,
            after_plan=updated_user.get("plan"),
            metadata=metadata,
        )
        return True
    except Exception as exc:
        logger.error("❌ Error otorgando entitlement a %s: %s", user_id, exc, exc_info=True)
        return False



def activate_plan_purchase(
    user_id: int,
    plan: str,
    *,
    days: int = PLAN_DURATION_DAYS,
    source: str = "manual_admin",
    metadata: Optional[Dict[str, Any]] = None,
    trigger_referral: bool = True,
) -> bool:
    plan_value = normalize_plan(plan)
    day_value = int(days)
    if plan_value not in {PLAN_PLUS, PLAN_PREMIUM}:
        logger.warning("Intento de compra con plan inválido para %s: %s", user_id, plan)
        return False
    if day_value not in PLAN_DURATION_OPTIONS:
        logger.warning("Intento de compra con duración inválida para %s: %s", user_id, days)
        return False

    success = grant_plan_entitlement(
        user_id,
        target_plan=plan_value,
        days=day_value,
        source=source,
        reason="purchase",
        metadata={
            "price_usdt": get_plan_price(plan_value, day_value),
            **(metadata or {}),
        },
    )

    if success and trigger_referral:
        _register_referral_after_activation(user_id, plan_value, purchased_days=day_value)

    if success:
        logger.info("✅ Compra aplicada | user=%s plan=%s days=%s source=%s", user_id, plan_value, day_value, source)
    return success


# =========================
# ACTIVACIONES (ADMIN / SISTEMA)
# =========================

def activate_plus(user_id: int, days: int = PLAN_DURATION_DAYS) -> bool:
    return activate_plan_purchase(user_id, PLAN_PLUS, days=days, source="admin_manual")



def activate_premium(user_id: int, days: int = PLAN_DURATION_DAYS) -> bool:
    return activate_plan_purchase(user_id, PLAN_PREMIUM, days=days, source="admin_manual")



def _register_referral_after_activation(user_id: int, plan: str, *, purchased_days: int) -> None:
    try:
        from app.referrals import register_valid_referral

        success = register_valid_referral(user_id, plan, purchased_days=purchased_days)
        if success:
            logger.info("✅ Referido registrado | user=%s plan=%s purchased_days=%s", user_id, plan, purchased_days)
        else:
            logger.debug("ℹ️ No se registró referido para %s", user_id)
    except ImportError as exc:
        logger.error("❌ No se pudo importar register_valid_referral: %s", exc)
    except Exception as exc:
        logger.error("❌ Error registrando referido para %s: %s", user_id, exc, exc_info=True)


# =========================
# EXPIRACIONES AUTOMÁTICAS
# =========================

def expire_plans() -> int:
    now = utcnow()
    users_col = users_collection()
    processed = 0

    expired_users = users_col.find({
        "plan_end": {"$lt": now, "$ne": None},
        "plan": {"$ne": PLAN_FREE},
    })

    for user in expired_users:
        result = users_col.update_one(
            {"user_id": user["user_id"]},
            {
                "$set": {
                    "plan": PLAN_FREE,
                    "plan_end": None,
                    "subscription_status": SUBSCRIPTION_STATUS_EXPIRED,
                    "last_plan_change_at": now,
                    "updated_at": now,
                }
            },
        )
        if result.modified_count > 0:
            processed += 1
            _record_subscription_event(
                user_id=user["user_id"],
                event_type="expired",
                plan=user.get("plan") or PLAN_FREE,
                days=0,
                source="scheduler",
                before_plan=user.get("plan"),
                after_plan=PLAN_FREE,
                metadata={},
            )
    return processed


# =========================
# UTILIDADES DE EXTENSIÓN
# =========================

def extend_current_plan(user_id: int, days: int = PLAN_DURATION_DAYS) -> bool:
    try:
        user = get_user(user_id)
        if not user or not is_plan_active(user):
            return False

        user["plan_end"] = user["plan_end"] + timedelta(days=int(days))
        user["subscription_status"] = SUBSCRIPTION_STATUS_ACTIVE
        user = update_timestamp(user)
        save_user(user)

        _record_subscription_event(
            user_id=user_id,
            event_type="extend",
            plan=user.get("plan"),
            days=int(days),
            source="system_extend",
            before_plan=user.get("plan"),
            after_plan=user.get("plan"),
            metadata={},
        )

        logger.info("✅ Plan extendido %s días para usuario %s", days, user_id)
        return True
    except Exception as exc:
        logger.error("❌ Error extendiendo plan para %s: %s", user_id, exc, exc_info=True)
        return False


# =========================
# FUNCIONES ADICIONALES PARA REFERIDOS
# =========================

def can_user_upgrade(user_id: int, target_plan: str) -> bool:
    user = get_user(user_id)
    if not user:
        return False

    current_plan = normalize_plan(user.get("plan"))
    return plan_rank(target_plan) > plan_rank(current_plan)
