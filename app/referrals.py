# app/referrals.py

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.config import get_bot_username
from app.database import referrals_collection, users_collection
from app.models import new_referral
from app.plans import (
    PLAN_PLUS,
    PLAN_PREMIUM,
    can_count_as_valid_referral_purchase,
    get_plan_name,
    get_referral_reward_days,
    grant_plan_entitlement,
    normalize_plan,
)

logger = logging.getLogger(__name__)


# =========================
# REGISTRO DE REFERIDO VÁLIDO
# =========================

def _build_fallback_purchase_key(
    *,
    referred_user: Dict[str, Any],
    activated_plan: str,
    purchased_days: int,
    activation_source: str,
) -> str:
    last_purchase_at = referred_user.get("last_purchase_at")
    timestamp_marker = getattr(last_purchase_at, "isoformat", lambda: None)() or "na"
    return f"legacy:{int(referred_user.get('user_id') or 0)}:{activated_plan}:{int(purchased_days)}:{activation_source}:{timestamp_marker}"



def register_valid_referral(
    referred_user_id: int,
    activated_plan: str,
    *,
    purchased_days: int,
    purchase_key: Optional[str] = None,
    activation_source: str = "unknown",
    activation_metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Registra una compra válida de un referido.

    Reglas:
    - Solo cuentan compras confirmadas de PLUS/PREMIUM con duraciones válidas.
    - La recompensa depende de la duración comprada y mantiene el mismo tier comprado.
    - El mismo referido puede generar nuevas recompensas en compras futuras válidas.
    - Cada compra solo puede recompensar una vez (idempotencia por purchase_key/order_id).
    """
    try:
        purchased_days = int(purchased_days)
        activated_plan = normalize_plan(activated_plan)

        if not can_count_as_valid_referral_purchase(activated_plan, purchased_days):
            logger.warning(
                "Compra no válida para referido | referred=%s plan=%s days=%s",
                referred_user_id,
                activated_plan,
                purchased_days,
            )
            return False

        reward_days = get_referral_reward_days(purchased_days)
        if reward_days <= 0:
            logger.warning("No hay recompensa definida para purchased_days=%s", purchased_days)
            return False

        users_col = users_collection()
        refs_col = referrals_collection()

        referred_user = users_col.find_one({"user_id": int(referred_user_id)})
        if not referred_user:
            logger.warning("Usuario referido %s no encontrado", referred_user_id)
            return False

        referrer_id = referred_user.get("referred_by")
        if not referrer_id:
            logger.debug("Usuario %s no fue referido por nadie", referred_user_id)
            return False

        if int(referrer_id) == int(referred_user_id):
            logger.warning("Auto-referido detectado: %s", referred_user_id)
            return False

        referrer = users_col.find_one({"user_id": int(referrer_id)})
        if not referrer:
            logger.warning("Referidor %s no encontrado", referrer_id)
            return False

        purchase_key_value = str(
            purchase_key
            or _build_fallback_purchase_key(
                referred_user=referred_user,
                activated_plan=activated_plan,
                purchased_days=purchased_days,
                activation_source=activation_source,
            )
        )

        existing = refs_col.find_one({"purchase_key": purchase_key_value})
        if existing:
            logger.debug("Compra de referido ya registrada | purchase_key=%s", purchase_key_value)
            return False

        reward_plan = activated_plan
        ref_doc = new_referral(
            referrer_id=int(referrer_id),
            referred_id=int(referred_user_id),
            activated_plan=activated_plan,
            activated_days=purchased_days,
            reward_days_applied=reward_days,
            reward_plan_applied=reward_plan,
            purchase_key=purchase_key_value,
        )
        refs_col.insert_one(ref_doc)

        inc_fields = {
            "valid_referrals_total": 1,
            "reward_days_total": reward_days,
        }
        if activated_plan == PLAN_PLUS:
            inc_fields["ref_plus_valid"] = 1
            inc_fields["ref_plus_total"] = 1
        elif activated_plan == PLAN_PREMIUM:
            inc_fields["ref_premium_valid"] = 1
            inc_fields["ref_premium_total"] = 1

        users_col.update_one(
            {"user_id": int(referrer_id)},
            {"$inc": inc_fields, "$set": {"updated_at": ref_doc["updated_at"]}},
        )

        reward_applied = grant_plan_entitlement(
            int(referrer_id),
            target_plan=reward_plan,
            days=reward_days,
            source="referral_reward",
            reason="referral_reward",
            metadata={
                "referred_user_id": int(referred_user_id),
                "purchase_key": purchase_key_value,
                "purchased_plan": activated_plan,
                "purchased_days": purchased_days,
                "activation_source": activation_source,
                **(activation_metadata or {}),
            },
        )

        if reward_applied:
            logger.info(
                "🎁 Recompensa aplicada | referrer=%s referred=%s purchased=%s:%s reward=%s:%s purchase_key=%s",
                referrer_id,
                referred_user_id,
                activated_plan,
                purchased_days,
                reward_plan,
                reward_days,
                purchase_key_value,
            )
        else:
            logger.warning(
                "⚠️ No se pudo aplicar recompensa | referrer=%s referred=%s purchase_key=%s",
                referrer_id,
                referred_user_id,
                purchase_key_value,
            )

        logger.info(
            "✅ Referido registrado | %s → %s (%s %s días) purchase_key=%s",
            referrer_id,
            referred_user_id,
            activated_plan,
            purchased_days,
            purchase_key_value,
        )
        return True

    except Exception as exc:
        logger.error("❌ Error en register_valid_referral: %s", exc, exc_info=True)
        return False


# =========================
# ESTADÍSTICAS DE REFERIDOS
# =========================

def get_user_referral_stats(user_id: int) -> Optional[Dict]:
    try:
        users_col = users_collection()
        refs_col = referrals_collection()

        user = users_col.find_one({"user_id": int(user_id)})
        if not user:
            return _get_empty_stats(user_id)

        ref_code = user.get("ref_code", f"ref_{user_id}")

        total_referred = len(refs_col.distinct("referred_id", {"referrer_id": int(user_id)})) if hasattr(refs_col, 'distinct') else refs_col.count_documents({"referrer_id": int(user_id)})
        plus_referred = int(refs_col.count_documents({
            "referrer_id": int(user_id),
            "activated_plan": PLAN_PLUS,
        }))
        premium_referred = int(refs_col.count_documents({
            "referrer_id": int(user_id),
            "activated_plan": PLAN_PREMIUM,
        }))

        current_plus = int(user.get("ref_plus_valid", 0) or 0)
        current_premium = int(user.get("ref_premium_valid", 0) or 0)
        valid_total = int(user.get("valid_referrals_total", current_plus + current_premium) or 0)
        reward_days_total = int(user.get("reward_days_total", 0) or 0)

        recent_rewards = list(
            refs_col.find(
                {"referrer_id": int(user_id)},
                {
                    "activated_plan": 1,
                    "activated_days": 1,
                    "reward_days_applied": 1,
                    "reward_plan_applied": 1,
                    "purchase_key": 1,
                },
            )
            .sort("created_at", -1)
            .limit(5)
        )

        return {
            "ref_code": ref_code,
            "total_referred": total_referred,
            "plus_referred": plus_referred,
            "premium_referred": premium_referred,
            "current_plus": current_plus,
            "current_premium": current_premium,
            "valid_referrals_total": valid_total,
            "reward_days_total": reward_days_total,
            "pending_rewards": _calculate_pending_rewards(),
            "recent_rewards": recent_rewards,
        }
    except Exception as exc:
        logger.error("❌ Error en get_user_referral_stats para user_id %s: %s", user_id, exc, exc_info=True)
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
        "pending_rewards": _calculate_pending_rewards(),
        "recent_rewards": [],
    }



def _calculate_pending_rewards() -> List[str]:
    return [
        "7 días comprados → 3 días de recompensa",
        "15 días comprados → 7 días de recompensa",
        "21 días comprados → 10 días de recompensa",
        "30 días comprados → 15 días de recompensa",
    ]



def get_referral_reward_rules() -> List[str]:
    return list(_calculate_pending_rewards())


# =========================
# COMPATIBILIDAD
# =========================

def check_ref_rewards(referrer_id: int) -> bool:
    return False


# =========================
# UTILIDADES PÚBLICAS
# =========================

def get_referral_link(user_id: int) -> str:
    bot_username = get_bot_username() or "HADES_ALPHA_bot"
    return f"https://t.me/{bot_username}?start=ref_{int(user_id)}"



def get_referral_summary(user_id: int) -> Dict:
    stats = get_user_referral_stats(user_id)
    return {
        "user_id": int(user_id),
        "referral_link": get_referral_link(user_id),
        "total_referred": int((stats or {}).get("total_referred") or 0),
        "plus_referred": int((stats or {}).get("plus_referred") or 0),
        "premium_referred": int((stats or {}).get("premium_referred") or 0),
        "valid_referrals_total": int((stats or {}).get("valid_referrals_total") or 0),
        "reward_days_total": int((stats or {}).get("reward_days_total") or 0),
        "reward_rules": get_referral_reward_rules(),
    }



def reset_referral_counters(user_id: int) -> bool:
    try:
        result = users_collection().update_one(
            {"user_id": int(user_id)},
            {
                "$set": {
                    "ref_plus_valid": 0,
                    "ref_premium_valid": 0,
                    "ref_plus_total": 0,
                    "ref_premium_total": 0,
                    "valid_referrals_total": 0,
                    "reward_days_total": 0,
                }
            },
        )
        return bool(getattr(result, "modified_count", 0))
    except Exception as exc:
        logger.error("❌ Error reiniciando contadores de referidos para %s: %s", user_id, exc, exc_info=True)
        return False
