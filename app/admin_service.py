from __future__ import annotations

from datetime import datetime

from app.config import is_admin
from app.database import users_collection


def get_user_by_id(user_id: int) -> dict | None:
    return users_collection().find_one({"user_id": int(user_id)})


def can_block_target(admin_user_id: int, target_user_id: int) -> tuple[bool, str | None]:
    if target_user_id == admin_user_id:
        return False, "self"
    if is_admin(target_user_id):
        return False, "admin"
    return True, None


def ban_user(target_user_id: int, banned_by: int) -> None:
    users_collection().update_one(
        {"user_id": int(target_user_id)},
        {"$set": {
            "banned": True,
            "banned_at": datetime.utcnow(),
            "banned_by": int(banned_by),
            "updated_at": datetime.utcnow(),
        }},
    )


def validate_custom_plan_days(days: int) -> tuple[bool, str | None]:
    if days <= 0:
        return False, "non_positive"
    if days > 3650:
        return False, "too_high"
    return True, None
