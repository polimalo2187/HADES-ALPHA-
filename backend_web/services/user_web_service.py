from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from services.database_service import db_service


def new_user_web(
    phone_number: str,
    username: Optional[str] = None,
    referred_by: Optional[int] = None,
    language: str = "es",
) -> Dict[str, Any]:
    """Crea un nuevo usuario para la plataforma web."""
    now = datetime.utcnow()
    user_id = int(now.timestamp()) % 1000000000  # Generar ID único simple
    
    return {
        "user_id": user_id,
        "phone_number": phone_number,
        "username": username or f"user_{user_id}",
        "plan": "free",
        "trial_end": now + timedelta(days=5),
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
        "miniapp_settings": {
            "push_alerts": {
                "enabled": True,
                "tiers": {
                    "free": True,
                    "plus": False,
                    "premium": False,
                },
            },
        },
        "onboarding_seen": False,
        "onboarding_completed": False,
        "onboarding_version": 0,
        "banned": False,
        "schema_version": 4,
        "created_at": now,
        "updated_at": now,
        "last_activity": now,
    }


def get_or_create_user_by_phone(
    phone_number: str,
    username: Optional[str] = None,
    referral_code: Optional[str] = None,
) -> Dict[str, Any]:
    """Obtiene o crea un usuario por número de teléfono."""
    user = db_service.get_user_by_phone(phone_number)
    
    if user:
        # Actualizar última actividad
        db_service.update_user(user["user_id"], {"last_activity": datetime.utcnow()})
        return user
    
    # Crear nuevo usuario
    user_data = new_user_web(
        phone_number=phone_number,
        username=username,
    )
    
    created_user = db_service.create_user(user_data)
    return created_user


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    """Obtiene un usuario por ID."""
    return db_service.get_user_by_id(user_id)
