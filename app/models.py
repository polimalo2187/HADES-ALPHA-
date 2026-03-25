from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# =========================
# CONSTANTES CONFIGURABLES
# =========================
TRIAL_DAYS = 7

# =========================
# USER MODEL
# =========================
def new_user(
    user_id: int,
    username: Optional[str],
    referred_by: Optional[int] = None,
) -> Dict[str, Any]:
    now = datetime.utcnow()

    return {
        "user_id": user_id,
        "username": username,
        "plan": "free",
        "trial_end": now + timedelta(days=TRIAL_DAYS),
        "plan_end": None,
        "ref_code": f"ref_{user_id}",
        "referred_by": referred_by,
        "ref_plus_valid": 0,
        "ref_premium_valid": 0,
        "daily_signal_count": 0,
        "daily_signal_date": now.date().isoformat(),
        "last_signal_id": None,
        "last_signal_at": None,
        "created_at": now,
        "updated_at": now,
        "last_activity": now,
    }


def update_timestamp(doc: Dict[str, Any]) -> Dict[str, Any]:
    updated_doc = doc.copy()
    updated_doc["updated_at"] = datetime.utcnow()
    return updated_doc


def activate_plan(user: Dict[str, Any], plan: str, days: int = 30) -> Dict[str, Any]:
    now = datetime.utcnow()

    if user.get("plan_end") and user["plan_end"] > now:
        user["plan_end"] = user["plan_end"] + timedelta(days=days)
    else:
        user["plan_end"] = now + timedelta(days=days)

    user["plan"] = plan
    user["trial_end"] = None
    return update_timestamp(user)


def is_trial_active(user: Dict[str, Any]) -> bool:
    if user.get("trial_end") is None:
        return False
    return user["trial_end"] >= datetime.utcnow()


def is_plan_active(user: Dict[str, Any]) -> bool:
    if user.get("plan_end") is None:
        return False
    return user["plan_end"] >= datetime.utcnow()


# =========================
# REFERRAL MODEL
# =========================
def new_referral(referrer_id: int, referred_id: int, activated_plan: str) -> Dict[str, Any]:
    return {
        "referrer_id": referrer_id,
        "referred_id": referred_id,
        "activated_plan": activated_plan,
        "activated_at": datetime.utcnow(),
    }


# =========================
# SIGNAL MODEL
# =========================
def new_signal(
    symbol: str,
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profits: list,
    timeframes: list,
    visibility: str,
    leverage: Optional[Dict[str, str]] = None,
    components: Optional[list] = None,
    score: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Crea un diccionario base de señal listo para MongoDB.
    """
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
        "components": components,
        "score": score,
        "created_at": datetime.utcnow(),
  }
