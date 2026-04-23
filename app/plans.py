from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import logging

from app.database import subscription_events_collection, users_collection
from app.models import activate_plan, get_effective_trial_end, is_plan_active, is_trial_active, new_subscription_event, update_timestamp, utcnow
from app.services.admin_service import is_effectively_banned

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

# =========================
# VALIDACIONES DE PLANES
# =========================

def is_valid_plan_duration(plan: Optional[str], days: int) -> bool:
    plan_value = normalize_plan(plan)
    try:
        day_value = int(days)
    except Exception:
        return False
    return plan_value in {PLAN_PLUS, PLAN_PREMIUM} and day_value in PLAN_DURATION_OPTIONS


def validate_plan_duration(plan: Optional[str], days: int) -> tuple[str, int]:
    plan_value = normalize_plan(plan)
    try:
        day_value = int(days)
    except Exception as exc:
        raise ValueError("Duración inválida para el plan seleccionado") from exc
    if not is_valid_plan_duration(plan_value, day_value):
        raise ValueError("Duración inválida para el plan seleccionado")
    return plan_value, day_value


def validate_entitlement_days(days: int) -> int:
    try:
        day_value = int(days)
    except Exception as exc:
        raise ValueError("Días inválidos") from exc
    if day_value <= 0:
        raise ValueError("La cantidad de días debe ser mayor que 0")
    if day_value > 3650:
        raise ValueError("La cantidad de días es demasiado alta")
    return day_value


# Precios comerciales confirmados para los subplanes actuales.
PLAN_PRICE_TABLE = {
    PLAN_PLUS: {
        7: 3.5,
        15: 7.5,
        21: 10.5,
        30: 15.0,
    },
    PLAN_PREMIUM: {
        7: 5.0,
        15: 10.0,
        21: 15.0,
        30: 20.0,
    },
}

REFERRAL_REWARD_BY_DURATION = {
    7: 3,
    15: 7,
    21: 10,
    30: 15,
}

SECONDS_PER_DAY = 24 * 60 * 60


# =========================
# HELPERS DE USUARIO
# =========================

def get_user(user_id: int) -> Optional[dict]:
    user = users_collection().find_one({"user_id": int(user_id)})
    if not user:
        return None
    synced_user, dirty = _sync_deferred_plan_state(user)
    if dirty:
        save_user(synced_user)
    return synced_user



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
    try:
        day_value = int(days)
    except Exception:
        return 0.0
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
    return plan_status(user).get("status") in {SUBSCRIPTION_STATUS_ACTIVE, SUBSCRIPTION_STATUS_TRIAL}



def get_subscription_status(user: dict) -> str:
    if is_effectively_banned(user):
        return SUBSCRIPTION_STATUS_BANNED
    return str(plan_status(user).get("status") or SUBSCRIPTION_STATUS_FREE)



def plan_status(user: dict) -> dict:
    now = utcnow()
    user_copy = dict(user or {})
    current_plan = normalize_plan(user_copy.get("plan"))
    plan_end = user_copy.get("plan_end")
    if plan_end and plan_end > now and current_plan in {PLAN_PLUS, PLAN_PREMIUM}:
        return {
            "plan": current_plan,
            "status": SUBSCRIPTION_STATUS_ACTIVE,
            "expires": plan_end,
            "days_left": max((plan_end - now).days, 0),
        }

    queued_plus_seconds = _queued_plus_seconds(user_copy)
    if queued_plus_seconds > 0:
        expires = now + timedelta(seconds=queued_plus_seconds)
        return {
            "plan": PLAN_PLUS,
            "status": SUBSCRIPTION_STATUS_ACTIVE,
            "expires": expires,
            "days_left": max((expires - now).days, 0),
        }

    effective_trial_end = get_effective_trial_end(user_copy)
    if effective_trial_end and effective_trial_end > now:
        return {
            "plan": PLAN_FREE,
            "status": SUBSCRIPTION_STATUS_TRIAL,
            "expires": effective_trial_end,
            "days_left": max((effective_trial_end - now).days, 0),
        }

    return {
        "plan": PLAN_FREE,
        "status": SUBSCRIPTION_STATUS_EXPIRED if _is_paid_plan_value(current_plan) else SUBSCRIPTION_STATUS_FREE,
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
# HELPERS DE ENTITLEMENTS
# =========================

def _seconds_from_days(days: int) -> int:
    return validate_entitlement_days(days) * SECONDS_PER_DAY


def _queued_plus_seconds(user: Dict[str, Any]) -> int:
    return max(int(user.get("queued_plus_seconds") or 0), 0)


def _remaining_active_seconds(user: Dict[str, Any], now: Optional[datetime] = None) -> int:
    now = now or utcnow()
    plan_end = user.get("plan_end")
    if not plan_end:
        return 0
    return max(int((plan_end - now).total_seconds()), 0)


def _is_paid_plan_value(value: Optional[str]) -> bool:
    return normalize_plan(value) in {PLAN_PLUS, PLAN_PREMIUM}


def _sync_deferred_plan_state(user: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
    now = utcnow()
    dirty = False
    current_plan = normalize_plan(user.get("plan"))
    queued_plus_seconds = _queued_plus_seconds(user)
    if user.get("queued_plus_seconds") != queued_plus_seconds:
        user["queued_plus_seconds"] = queued_plus_seconds
        dirty = True

    plan_end = user.get("plan_end")
    if plan_end and plan_end > now and current_plan in {PLAN_PLUS, PLAN_PREMIUM}:
        if user.get("subscription_status") != SUBSCRIPTION_STATUS_ACTIVE:
            user["subscription_status"] = SUBSCRIPTION_STATUS_ACTIVE
            dirty = True
        return user, dirty

    if queued_plus_seconds > 0:
        new_end = now + timedelta(seconds=queued_plus_seconds)
        if current_plan != PLAN_PLUS or user.get("plan_end") != new_end:
            user["plan"] = PLAN_PLUS
            user["plan_end"] = new_end
            user["queued_plus_seconds"] = 0
            user["subscription_status"] = SUBSCRIPTION_STATUS_ACTIVE
            user["trial_end"] = None
            user["last_plan_change_at"] = now
            user["last_entitlement_source"] = user.get("last_entitlement_source") or "queued_plus_restore"
            dirty = True
        return user, dirty

    if current_plan != PLAN_FREE or user.get("plan_end") is not None:
        if user.get("plan") != PLAN_FREE:
            user["plan"] = PLAN_FREE
            dirty = True
        if user.get("plan_end") is not None:
            user["plan_end"] = None
            dirty = True
        desired_status = SUBSCRIPTION_STATUS_TRIAL if is_trial_active(user) else (SUBSCRIPTION_STATUS_EXPIRED if _is_paid_plan_value(current_plan) else SUBSCRIPTION_STATUS_FREE)
        if user.get("subscription_status") != desired_status:
            user["subscription_status"] = desired_status
            dirty = True
    return user, dirty


def _activate_plus_now(user: Dict[str, Any], seconds: int, *, now: datetime, source: str) -> None:
    user["plan"] = PLAN_PLUS
    user["plan_end"] = now + timedelta(seconds=max(int(seconds), 0))
    user["trial_end"] = None
    user["subscription_status"] = SUBSCRIPTION_STATUS_ACTIVE
    user["last_plan_change_at"] = now
    user["last_entitlement_source"] = source
    user["plan_started_at"] = now


def _activate_premium_now(user: Dict[str, Any], seconds: int, *, now: datetime, source: str) -> None:
    user["plan"] = PLAN_PREMIUM
    user["plan_end"] = now + timedelta(seconds=max(int(seconds), 0))
    user["trial_end"] = None
    user["subscription_status"] = SUBSCRIPTION_STATUS_ACTIVE
    user["last_plan_change_at"] = now
    user["last_entitlement_source"] = source
    user["plan_started_at"] = now


def _set_free_or_trial_state(user: Dict[str, Any], *, now: datetime, source: str) -> None:
    user["plan"] = PLAN_FREE
    user["plan_end"] = None
    user["last_plan_change_at"] = now
    user["last_entitlement_source"] = source
    user["subscription_status"] = SUBSCRIPTION_STATUS_TRIAL if is_trial_active(user) else SUBSCRIPTION_STATUS_EXPIRED


def get_effective_paid_plan(user: Dict[str, Any]) -> str:
    snapshot = plan_status(user)
    return normalize_plan(snapshot.get("plan"))


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
            days=days,
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
    user, _ = _sync_deferred_plan_state(user)
    plan_value = normalize_plan(target_plan)
    grant_seconds = _seconds_from_days(days)
    current_plan = normalize_plan(user.get("plan"))
    current_active = bool(user.get("plan_end") and user.get("plan_end") > now and current_plan in {PLAN_PLUS, PLAN_PREMIUM})
    queued_plus_seconds = _queued_plus_seconds(user)

    if plan_value == PLAN_PLUS:
        if current_active and current_plan == PLAN_PREMIUM:
            user["queued_plus_seconds"] = queued_plus_seconds + grant_seconds
            user["queued_plus_origin"] = source
            user["last_entitlement_source"] = source
        elif current_active and current_plan == PLAN_PLUS:
            base_end = user.get("plan_end") if user.get("plan_end") and user["plan_end"] > now else now
            user["plan_end"] = base_end + timedelta(seconds=grant_seconds)
            user["trial_end"] = None
            user["subscription_status"] = SUBSCRIPTION_STATUS_ACTIVE
            user["last_entitlement_source"] = source
        else:
            activation_seconds = grant_seconds + queued_plus_seconds
            user["queued_plus_seconds"] = 0
            user["queued_plus_origin"] = None
            _activate_plus_now(user, activation_seconds, now=now, source=source)
    elif plan_value == PLAN_PREMIUM:
        if current_active and current_plan == PLAN_PREMIUM:
            base_end = user.get("plan_end") if user.get("plan_end") and user["plan_end"] > now else now
            user["plan_end"] = base_end + timedelta(seconds=grant_seconds)
            user["trial_end"] = None
            user["subscription_status"] = SUBSCRIPTION_STATUS_ACTIVE
            user["last_entitlement_source"] = source
        else:
            if current_active and current_plan == PLAN_PLUS:
                user["queued_plus_seconds"] = queued_plus_seconds + _remaining_active_seconds(user, now)
                user["queued_plus_origin"] = current_plan
            _activate_premium_now(user, grant_seconds, now=now, source=source)
    else:
        raise ValueError(f"Plan de entitlement no soportado: {target_plan}")

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

        try:
            days = validate_entitlement_days(days)
        except ValueError:
            logger.warning("Días inválidos para entitlement de %s: %s", user_id, days)
            return False

        before_plan = normalize_plan(user.get("plan"))
        updated_user = _apply_entitlement_to_user(
            user,
            target_plan=target_plan,
            days=days,
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
    try:
        plan_value, day_value = validate_plan_duration(plan, days)
    except ValueError:
        logger.warning("Intento de compra inválida para %s: plan=%s days=%s", user_id, plan, days)
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
        _register_referral_after_activation(
            user_id,
            plan_value,
            purchased_days=day_value,
            purchase_key=(metadata or {}).get("order_id"),
            activation_source=source,
            activation_metadata=metadata or {},
        )

    if success:
        logger.info("✅ Compra aplicada | user=%s plan=%s days=%s source=%s", user_id, plan_value, day_value, source)
    return success


# =========================
# ACTIVACIONES (ADMIN / SISTEMA)
# =========================

def activate_plus(user_id: int, days: int = PLAN_DURATION_DAYS) -> bool:
    return activate_plan_purchase(user_id, PLAN_PLUS, days=days, source="admin_manual", trigger_referral=False)



def activate_premium(user_id: int, days: int = PLAN_DURATION_DAYS) -> bool:
    return activate_plan_purchase(user_id, PLAN_PREMIUM, days=days, source="admin_manual", trigger_referral=False)



def _register_referral_after_activation(
    user_id: int,
    plan: str,
    *,
    purchased_days: int,
    purchase_key: Optional[str] = None,
    activation_source: str = "unknown",
    activation_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        from app.referrals import register_valid_referral

        success = register_valid_referral(
            user_id,
            plan,
            purchased_days=purchased_days,
            purchase_key=purchase_key,
            activation_source=activation_source,
            activation_metadata=activation_metadata or {},
        )
        if success:
            logger.info("✅ Referido registrado | user=%s plan=%s purchased_days=%s purchase_key=%s", user_id, plan, purchased_days, purchase_key)
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

    candidates = users_col.find({
        "$or": [
            {"plan_end": {"$lt": now, "$ne": None}},
            {"queued_plus_seconds": {"$gt": 0}},
        ]
    })

    for user in candidates:
        before_plan = normalize_plan(user.get("plan"))
        synced_user, dirty = _sync_deferred_plan_state(user)
        if not dirty:
            continue
        save_user(synced_user)
        processed += 1
        _record_subscription_event(
            user_id=user["user_id"],
            event_type="expired" if synced_user.get("plan") == PLAN_FREE else "restore_queued_plus",
            plan=before_plan,
            days=0,
            source="scheduler",
            before_plan=before_plan,
            after_plan=synced_user.get("plan"),
            metadata={"queued_plus_seconds": int(synced_user.get("queued_plus_seconds") or 0)},
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
