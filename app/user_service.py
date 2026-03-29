from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from app.database import users_collection
from app.i18n import normalize_language
from app.models import USER_SCHEMA_VERSION, new_user, user_backfill_patch




def ensure_valid_user_id(user_id: int) -> int:
    try:
        value = int(user_id)
    except Exception as exc:
        raise ValueError("user_id inválido") from exc
    if value <= 0:
        raise ValueError("user_id inválido")
    return value


DEFAULT_EXISTING_USER_FIELDS = {
    "plan": "free",
    "trial_end": None,
    "plan_end": None,
    "subscription_status": "free",
    "plan_started_at": None,
    "last_plan_change_at": None,
    "last_purchase_at": None,
    "last_purchase_plan": None,
    "last_purchase_days": 0,
    "last_entitlement_source": None,
    "ref_plus_valid": 0,
    "ref_premium_valid": 0,
    "ref_plus_total": 0,
    "ref_premium_total": 0,
    "valid_referrals_total": 0,
    "reward_days_total": 0,
    "daily_signal_count": 0,
    "last_signal_id": None,
    "last_signal_at": None,
    "onboarding_seen": False,
    "onboarding_completed": False,
    "onboarding_version": 0,
    "banned": False,
    "schema_version": USER_SCHEMA_VERSION,
}



def build_user_patch(
    *,
    existing_user: Optional[Dict[str, Any]],
    user_id: int,
    username: Optional[str],
    telegram_language: Optional[str],
    referred_by: Optional[int],
) -> Dict[str, Any]:
    user_id = ensure_valid_user_id(user_id)
    now = datetime.utcnow()
    patch: Dict[str, Any] = {}
    normalized_language = normalize_language(telegram_language)

    if not existing_user:
        raise ValueError("existing_user is required for build_user_patch")

    patch.update(user_backfill_patch(existing_user, user_id=user_id))

    for key, value in DEFAULT_EXISTING_USER_FIELDS.items():
        if key not in existing_user:
            patch[key] = value

    if "ref_code" not in existing_user:
        patch["ref_code"] = f"ref_{user_id}"

    if "daily_signal_date" not in existing_user:
        patch["daily_signal_date"] = now.date().isoformat()

    if not existing_user.get("language"):
        patch["language"] = normalized_language

    if username and existing_user.get("username") != username:
        patch["username"] = username

    if referred_by and referred_by != user_id and not existing_user.get("referred_by"):
        patch["referred_by"] = referred_by

    if "created_at" not in existing_user:
        patch["created_at"] = now

    patch["updated_at"] = now
    patch["last_activity"] = now
    return patch



def get_or_create_user(
    *,
    user_id: int,
    username: Optional[str],
    telegram_language: Optional[str],
    referred_by: Optional[int] = None,
) -> Tuple[Dict[str, Any], bool]:
    user_id = ensure_valid_user_id(user_id)
    users_col = users_collection()
    existing_user = users_col.find_one({"user_id": user_id})
    normalized_language = normalize_language(telegram_language)

    if not existing_user:
        user_doc = new_user(
            user_id=user_id,
            username=username,
            referred_by=referred_by,
            language=normalized_language,
        )
        users_col.insert_one(user_doc)
        return user_doc, True

    patch = build_user_patch(
        existing_user=existing_user,
        user_id=user_id,
        username=username,
        telegram_language=telegram_language,
        referred_by=referred_by,
    )

    if patch:
        users_col.update_one({"user_id": user_id}, {"$set": patch})
        existing_user.update(patch)

    return existing_user, False
