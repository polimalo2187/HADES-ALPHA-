# app/plans.py - VERSIÓN CORREGIDA

from datetime import datetime, timedelta
from typing import Optional
import logging

from app.database import users_collection
from app.models import activate_plan, is_plan_active, is_trial_active, update_timestamp

logger = logging.getLogger(__name__)

# =========================
# CONSTANTES DE PLANES
# =========================

PLAN_FREE = "free"
PLAN_PLUS = "plus"
PLAN_PREMIUM = "premium"

PLAN_DURATION_DAYS = 30


# =========================
# HELPERS DE USUARIO
# =========================

def get_user(user_id: int) -> Optional[dict]:
    return users_collection().find_one({"user_id": user_id})


def save_user(user: dict):
    users_collection().update_one(
        {"user_id": user["user_id"]},
        {"$set": user},
        upsert=False,
    )


# =========================
# VERIFICACIONES DE ESTADO
# =========================

def has_access(user: dict) -> bool:
    """
    Retorna True si el usuario puede ver señales
    (plan activo o trial activo).
    """
    return is_plan_active(user) or is_trial_active(user)


def plan_status(user: dict) -> dict:
    """
    Retorna estado legible del plan.
    """
    now = datetime.utcnow()

    if is_plan_active(user):
        return {
            "plan": user["plan"],
            "status": "active",
            "expires": user["plan_end"],
        }

    if is_trial_active(user):
        return {
            "plan": PLAN_FREE,
            "status": "trial",
            "expires": user["trial_end"],
        }

    return {
        "plan": PLAN_FREE,
        "status": "expired",
        "expires": None,
    }


# =========================
# ACTIVACIONES (ADMIN / SISTEMA) - CORREGIDAS
# =========================

def activate_plus(user_id: int, days: int = PLAN_DURATION_DAYS) -> bool:
    """
    Activa o extiende PLAN PLUS.
    """
    try:
        user = get_user(user_id)
        if not user:
            logger.warning(f"Usuario {user_id} no encontrado al activar PLUS")
            return False

        user = activate_plan(user, PLAN_PLUS, days)
        save_user(user)
        
        # ✅ REGISTRAR REFERIDO (IMPORTANTE)
        _register_referral_after_activation(user_id, PLAN_PLUS)
        
        logger.info(f"✅ Plan PLUS activado para usuario {user_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error activando PLUS para {user_id}: {e}", exc_info=True)
        return False


def activate_premium(user_id: int, days: int = PLAN_DURATION_DAYS) -> bool:
    """
    Activa o extiende PLAN PREMIUM.
    """
    try:
        user = get_user(user_id)
        if not user:
            logger.warning(f"Usuario {user_id} no encontrado al activar PREMIUM")
            return False

        user = activate_plan(user, PLAN_PREMIUM, days)
        save_user(user)
        
        # ✅ REGISTRAR REFERIDO (IMPORTANTE)
        _register_referral_after_activation(user_id, PLAN_PREMIUM)
        
        logger.info(f"✅ Plan PREMIUM activado para usuario {user_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error activando PREMIUM para {user_id}: {e}", exc_info=True)
        return False


def _register_referral_after_activation(user_id: int, plan: str):
    """
    Llama al sistema de referidos después de activar un plan.
    Importa aquí para evitar dependencias circulares.
    """
    try:
        # Importación condicional para evitar problemas de importación circular
        from app.referrals import register_valid_referral
        success = register_valid_referral(user_id, plan)
        if success:
            logger.info(f"✅ Referido registrado para {user_id} con plan {plan}")
        else:
            logger.debug(f"ℹ️ No se registró referido para {user_id} (no tiene referidor)")
    except ImportError as e:
        logger.error(f"❌ No se pudo importar register_valid_referral: {e}")
    except Exception as e:
        logger.error(f"❌ Error registrando referido para {user_id}: {e}")


# =========================
# EXPIRACIONES AUTOMÁTICAS
# =========================

def expire_plans():
    """
    Revisa y expira planes vencidos.
    Debe ejecutarse periódicamente (scheduler).
    """
    now = datetime.utcnow()
    users_col = users_collection()

    expired_users = users_col.find({
        "plan_end": {"$lt": now}
    })

    for user in expired_users:
        user["plan"] = PLAN_FREE
        user["plan_end"] = None
        user = update_timestamp(user)
        users_col.update_one(
            {"user_id": user["user_id"]},
            {"$set": user},
        )


# =========================
# UTILIDADES DE EXTENSIÓN
# =========================

def extend_current_plan(user_id: int, days: int = PLAN_DURATION_DAYS) -> bool:
    """
    Extiende el plan actual del usuario.
    """
    try:
        user = get_user(user_id)
        if not user or not is_plan_active(user):
            return False

        # Calcular nueva fecha de expiración
        if user.get("plan_end"):
            new_end = user["plan_end"] + timedelta(days=days)
        else:
            new_end = datetime.utcnow() + timedelta(days=days)
        
        user["plan_end"] = new_end
        user = update_timestamp(user)
        save_user(user)
        
        logger.info(f"✅ Plan extendido {days} días para usuario {user_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error extendiendo plan para {user_id}: {e}", exc_info=True)
        return False


# =========================
# FUNCIONES ADICIONALES PARA REFERIDOS
# =========================

def get_plan_name(plan: str) -> str:
    """Devuelve el nombre legible del plan"""
    mapping = {
        PLAN_FREE: "FREE",
        PLAN_PLUS: "PLUS",
        PLAN_PREMIUM: "PREMIUM"
    }
    return mapping.get(plan, "FREE")


def can_user_upgrade(user_id: int, target_plan: str) -> bool:
    """Verifica si un usuario puede subir a un plan superior"""
    user = get_user(user_id)
    if not user:
        return False
    
    current_plan = user.get("plan", PLAN_FREE)
    
    # Solo puede subir, no bajar
    if current_plan == PLAN_FREE and target_plan in [PLAN_PLUS, PLAN_PREMIUM]:
        return True
    if current_plan == PLAN_PLUS and target_plan == PLAN_PREMIUM:
        return True
    
    return False


def get_plan_price(plan: str) -> float:
    """Devuelve el precio del plan en USDT (ejemplo)"""
    prices = {
        PLAN_PLUS: 20.0,
        PLAN_PREMIUM: 40.0,
    }
    return prices.get(plan, 0.0)
