"""
Modelos de datos para MongoDB
Esquemas de usuario, señales, pagos, etc.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import bcrypt


# =========================
# CONSTANTES
# =========================
TRIAL_DAYS = 5
USER_SCHEMA_VERSION = 5  # Actualizado para auth por teléfono
PLAN_FREE = "free"
PLAN_PLUS = "plus"
PLAN_PREMIUM = "premium"


def utcnow() -> datetime:
    return datetime.utcnow()


# =========================
# MODELO DE USUARIO
# =========================
def new_user(
    phone: str,
    password_hash: str,
    username: Optional[str] = None,
    referred_by: Optional[int] = None,
    language: str = "es",
) -> Dict[str, Any]:
    """Crea un nuevo usuario con autenticación por teléfono/contraseña"""
    now = utcnow()
    
    # Generar ID único basado en timestamp + random
    user_id = int(now.timestamp() * 1000) % 1000000000
    
    return {
        "user_id": user_id,
        "phone": phone,
        "password_hash": password_hash,
        "username": username,
        "plan": PLAN_FREE,
        "trial_end": now + timedelta(days=TRIAL_DAYS),
        "plan_end": None,
        "subscription_status": "trial",
        "plan_started_at": None,
        "last_plan_change_at": now,
        "last_purchase_at": None,
        "last_purchase_plan": None,
        "last_purchase_days": 0,
        "last_entitlement_source": None,
        "queued_plus_seconds": 0,
        "queued_plus_origin": None,
        "ref_code": f"ref_{user_id}",
        "referred_by": referred_by,
        "ref_plus_valid": 0,
        "ref_premium_valid": 0,
        "ref_plus_total": 0,
        "ref_premium_total": 0,
        "valid_referrals_total": 0,
        "reward_days_total": 0,
        "daily_signal_count": 0,
        "daily_signal_date": now.date().isoformat(),
        "last_signal_id": None,
        "last_signal_at": None,
        "language": language,
        "push_settings": {
            "enabled": True,
            "tiers": {
                PLAN_FREE: True,
                PLAN_PLUS: False,
                PLAN_PREMIUM: False,
            },
        },
        "onboarding_seen": False,
        "onboarding_completed": False,
        "onboarding_version": 0,
        "banned": False,
        "schema_version": USER_SCHEMA_VERSION,
        "created_at": now,
        "updated_at": now,
        "last_activity": now,
        "auth_provider": "phone_password",  # Sin Telegram
    }


def hash_password(password: str, salt_rounds: int = 12) -> str:
    """Hashea una contraseña usando bcrypt"""
    salt = bcrypt.gensalt(rounds=salt_rounds)
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(password: str, password_hash: str) -> bool:
    """Verifica una contraseña contra su hash"""
    try:
        return bcrypt.checkpw(
            password.encode('utf-8'),
            password_hash.encode('utf-8')
        )
    except Exception:
        return False


def activate_plan(user: Dict[str, Any], plan: str, days: int = 30) -> Dict[str, Any]:
    """Activa un plan para el usuario"""
    now = utcnow()
    
    if user.get("plan_end") and user["plan_end"] > now:
        user["plan_end"] = user["plan_end"] + timedelta(days=days)
    else:
        user["plan_end"] = now + timedelta(days=days)
    
    user["plan"] = plan
    user["trial_end"] = None
    user["subscription_status"] = "active"
    user["plan_started_at"] = user.get("plan_started_at") or now
    user["last_plan_change_at"] = now
    user["last_purchase_at"] = now
    user["last_purchase_plan"] = plan
    user["last_purchase_days"] = int(days)
    user["schema_version"] = USER_SCHEMA_VERSION
    
    return update_timestamp(user)


def is_trial_active(user: Dict[str, Any]) -> bool:
    """Verifica si el trial está activo"""
    trial_end = user.get("trial_end")
    if not isinstance(trial_end, datetime):
        return False
    
    created_at = user.get("created_at")
    if isinstance(created_at, datetime):
        capped_trial_end = created_at + timedelta(days=TRIAL_DAYS)
        effective_trial_end = min(trial_end, capped_trial_end)
        return effective_trial_end >= utcnow()
    
    return trial_end >= utcnow()


def is_plan_active(user: Dict[str, Any]) -> bool:
    """Verifica si el plan está activo"""
    now = utcnow()
    plan_end = user.get("plan_end")
    
    if plan_end is not None and plan_end >= now:
        return True
    
    queued_plus_seconds = int(user.get("queued_plus_seconds") or 0)
    return queued_plus_seconds > 0


def update_timestamp(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Actualiza el timestamp de actualización"""
    updated_doc = doc.copy()
    updated_doc["updated_at"] = utcnow()
    return updated_doc


# =========================
# MODELO DE SEÑAL
# =========================
def new_signal(
    symbol: str,
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profits: List[float],
    timeframes: List[str],
    visibility: str,
    leverage: Optional[Dict[str, str]] = None,
    components: Optional[List[Any]] = None,
    score: Optional[float] = None,
) -> Dict[str, Any]:
    """Crea una nueva señal"""
    now = utcnow()
    
    return {
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "take_profits": take_profits,
        "timeframes": timeframes,
        "leverage": leverage or {
            "conservador": "5x-10x",
            "moderado": "10x-20x",
            "agresivo": "30x-40x",
        },
        "visibility": visibility,
        "components": components or [],
        "score": score,
        "evaluated": False,
        "schema_version": 1,
        "created_at": now,
        "updated_at": now,
    }


# =========================
# MODELO DE ORDEN DE PAGO
# =========================
def new_payment_order(
    order_id: str,
    user_id: int,
    plan: str,
    days: int,
    base_price_usdt: float,
    amount_usdt: float,
    network: str,
    token_symbol: str,
    token_contract: str,
    deposit_address: str,
    expires_at: datetime,
) -> Dict[str, Any]:
    """Crea una nueva orden de pago"""
    now = utcnow()
    
    return {
        "order_id": order_id,
        "user_id": int(user_id),
        "plan": plan,
        "days": int(days),
        "base_price_usdt": float(base_price_usdt),
        "amount_usdt": float(amount_usdt),
        "network": network,
        "token_symbol": token_symbol,
        "token_contract": token_contract.lower(),
        "deposit_address": deposit_address.lower(),
        "declared_sender_address": None,
        "status": "awaiting_payment",
        "verification_attempts": 0,
        "verification_started_at": None,
        "verification_lock_token": None,
        "last_verification_reason": None,
        "matched_from": None,
        "matched_to": None,
        "matched_amount": None,
        "confirmations": 0,
        "confirmed_at": None,
        "expires_at": expires_at,
        "schema_version": 1,
        "created_at": now,
        "updated_at": now,
    }


# =========================
# MODELO DE SESIÓN PUSH
# =========================
def new_push_session(
    user_id: int,
    session_id: str,
    connection_type: str = "websocket",
) -> Dict[str, Any]:
    """Crea una nueva sesión de push notification"""
    now = utcnow()
    
    return {
        "user_id": int(user_id),
        "session_id": session_id,
        "connection_type": connection_type,
        "connected_at": now,
        "last_heartbeat": now,
        "is_active": True,
        "schema_version": 1,
        "created_at": now,
        "updated_at": now,
    }


__all__ = [
    "new_user",
    "hash_password",
    "verify_password",
    "activate_plan",
    "is_trial_active",
    "is_plan_active",
    "update_timestamp",
    "new_signal",
    "new_payment_order",
    "new_push_session",
    "TRIAL_DAYS",
    "PLAN_FREE",
    "PLAN_PLUS",
    "PLAN_PREMIUM",
]
