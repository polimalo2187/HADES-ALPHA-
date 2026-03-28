# app/referrals.py

import logging
from typing import Optional, Dict, List
from datetime import datetime

from app.database import users_collection, referrals_collection
from app.plans import (
    activate_premium,
    extend_current_plan,
    PLAN_FREE,
    PLAN_PLUS,
    PLAN_PREMIUM,
)
from app.models import new_referral, update_timestamp, is_plan_active
from app.config import get_bot_username

logger = logging.getLogger(__name__)



# =========================
# REGISTRO DE REFERIDO VÁLIDO
# =========================

def register_valid_referral(
    referred_user_id: int,
    activated_plan: str,
) -> bool:
    """
    Registra un referido válido cuando un usuario activa un plan.
    Nueva lógica:
    - Cada referido válido suma +7 días al plan actual del referidor.
    - Si el referidor está en FREE, se le activa PREMIUM y se le suman 7 días.
    - Se mantiene el histórico de PLUS/PREMIUM para estadísticas.
    """
    try:
        users_col = users_collection()
        refs_col = referrals_collection()

        referred_user = users_col.find_one({"user_id": referred_user_id})
        if not referred_user:
            logger.warning(f"Usuario referido {referred_user_id} no encontrado")
            return False

        referrer_id = referred_user.get("referred_by")
        if not referrer_id:
            logger.debug(f"Usuario {referred_user_id} no fue referido por nadie")
            return False

        if referrer_id == referred_user_id:
            logger.warning(f"Auto-referido detectado: {referred_user_id}")
            return False

        referrer = users_col.find_one({"user_id": referrer_id})
        if not referrer:
            logger.warning(f"Referidor {referrer_id} no encontrado")
            return False

        existing = refs_col.find_one({
            "referrer_id": referrer_id,
            "referred_id": referred_user_id
        })
        if existing:
            logger.debug(f"Referido {referred_user_id} ya registrado para {referrer_id}")
            return False

        ref_doc = new_referral(
            referrer_id=referrer_id,
            referred_id=referred_user_id,
            activated_plan=activated_plan,
            reward_days_applied=7,
        )
        refs_col.insert_one(ref_doc)

        inc_fields = {
            "valid_referrals_total": 1,
            "reward_days_total": 7,
        }

        if activated_plan == PLAN_PLUS:
            inc_fields["ref_plus_valid"] = 1
            inc_fields["ref_plus_total"] = 1
        elif activated_plan == PLAN_PREMIUM:
            inc_fields["ref_premium_valid"] = 1
            inc_fields["ref_premium_total"] = 1

        users_col.update_one(
            {"user_id": referrer_id},
            {
                "$inc": inc_fields,
                "$set": update_timestamp(referrer),
            }
        )

        # Recompensa automática:
        # si el referidor está en FREE, activamos PREMIUM.
        # si ya tiene plan activo, extendemos +7 días.
        current_plan = referrer.get("plan", PLAN_FREE)

        reward_applied = False

        if current_plan == PLAN_FREE or not is_plan_active(referrer):
            reward_applied = activate_premium(referrer_id)
            if reward_applied:
                # tras activar premium, extendemos +7 días
                extend_current_plan(referrer_id, days=7)
        else:
            reward_applied = extend_current_plan(referrer_id, days=7)

        if reward_applied:
            logger.info(
                f"🎁 Recompensa aplicada a {referrer_id}: +7 días por referido válido {referred_user_id}"
            )
        else:
            logger.warning(
                f"⚠️ No se pudo aplicar recompensa a {referrer_id} tras referido válido {referred_user_id}"
            )

        logger.info(f"✅ Referido registrado: {referrer_id} → {referred_user_id} ({activated_plan})")
        return True

    except Exception as e:
        logger.error(f"❌ Error en register_valid_referral: {e}", exc_info=True)
        return False


# =========================
# ESTADÍSTICAS DE REFERIDOS
# =========================

def get_user_referral_stats(user_id: int) -> Optional[Dict]:
    """
    Devuelve estadísticas de referidos compatibles con handlers.py
    y con la nueva lógica de +7 días por cada referido válido.
    """
    try:
        users_col = users_collection()
        refs_col = referrals_collection()

        user = users_col.find_one({"user_id": user_id})
        if not user:
            return _get_empty_stats(user_id)

        ref_code = user.get("ref_code", f"ref_{user_id}")

        total_referred = refs_col.count_documents({"referrer_id": user_id})
        plus_referred = refs_col.count_documents({
            "referrer_id": user_id,
            "activated_plan": PLAN_PLUS
        })
        premium_referred = refs_col.count_documents({
            "referrer_id": user_id,
            "activated_plan": PLAN_PREMIUM
        })

        current_plus = user.get("ref_plus_valid", 0)
        current_premium = user.get("ref_premium_valid", 0)
        valid_total = user.get("valid_referrals_total", current_plus + current_premium)
        reward_days_total = user.get("reward_days_total", valid_total * 7)

        pending_rewards = _calculate_pending_rewards(user)

        return {
            "ref_code": ref_code,
            "total_referred": total_referred,
            "plus_referred": plus_referred,
            "premium_referred": premium_referred,
            "current_plus": current_plus,
            "current_premium": current_premium,
            "valid_referrals_total": valid_total,
            "reward_days_total": reward_days_total,
            "pending_rewards": pending_rewards,
        }

    except Exception as e:
        logger.error(f"❌ Error en get_user_referral_stats para user_id {user_id}: {e}", exc_info=True)
        return _get_empty_stats(user_id)


def _get_empty_stats(user_id: int) -> Dict:
    return {
        "ref_code": f"ref_{user_id}",
        "total_referred": 0,
        "plus_referred": 0,
        "premium_referred": 0,
        "current_plus": 0,
        "current_premium": 0,
        "valid_referrals_total": 0,
        "reward_days_total": 0,
        "pending_rewards": [],
    }


def _calculate_pending_rewards(user: Dict) -> List[str]:
    """
    En la nueva lógica no hay recompensas por bloques.
    Solo informamos el sistema vigente.
    """
    valid_total = user.get("valid_referrals_total")
    if valid_total is None:
        valid_total = user.get("ref_plus_valid", 0) + user.get("ref_premium_valid", 0)

    reward_days_total = user.get("reward_days_total", valid_total * 7)

    return [
        f"Cada referido válido suma +7 días a tu plan actual",
        f"Días acumulados por referidos: {reward_days_total}",
    ]


# =========================
# COMPATIBILIDAD
# =========================

def check_ref_rewards(referrer_id: int) -> bool:
    """
    Se mantiene por compatibilidad con imports existentes.
    La recompensa ahora se aplica directamente en register_valid_referral().
    """
    return False


# =========================
# FUNCIONES AUXILIARES
# =========================

def get_referral_link(user_id: int) -> str:
    users_col = users_collection()
    user = users_col.find_one({"user_id": user_id})

    if not user:
        return f"https://t.me/{get_bot_username()}?start=ref_{user_id}"

    ref_code = user.get("ref_code", f"ref_{user_id}")
    return f"https://t.me/{get_bot_username()}?start={ref_code}"


def get_referral_summary(user_id: int) -> Dict:
    stats = get_user_referral_stats(user_id)
    if not stats:
        return {"total": 0, "plus": 0, "premium": 0}

    return {
        "total": stats["total_referred"],
        "plus": stats["plus_referred"],
        "premium": stats["premium_referred"],
        "current_plus": stats["current_plus"],
        "current_premium": stats["current_premium"],
        "valid_referrals_total": stats.get("valid_referrals_total", 0),
        "reward_days_total": stats.get("reward_days_total", 0),
    }


def reset_referral_counters(user_id: int) -> bool:
    try:
        users_col = users_collection()
        users_col.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "ref_plus_valid": 0,
                    "ref_premium_valid": 0,
                    "ref_plus_total": 0,
                    "ref_premium_total": 0,
                    "valid_referrals_total": 0,
                    "reward_days_total": 0,
                }
            }
        )
        logger.info(f"♻️ Contadores de referidos reseteados para {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error reseteando contadores: {e}")
        return False
