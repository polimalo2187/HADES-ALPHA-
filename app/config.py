# app/config.py

import os
from typing import List


DEFAULT_BOT_DISPLAY_NAME = "HADES ALPHA V2"
DEFAULT_BOT_USERNAME = "HADES_ALPHA_bot"


# ======================================================
# BOT / MARCA
# ======================================================

def get_bot_display_name() -> str:
    return os.getenv("BOT_DISPLAY_NAME", DEFAULT_BOT_DISPLAY_NAME).strip() or DEFAULT_BOT_DISPLAY_NAME



def get_bot_username() -> str:
    value = os.getenv("BOT_USERNAME", DEFAULT_BOT_USERNAME).strip()
    return value.lstrip("@") or DEFAULT_BOT_USERNAME


# ======================================================
# ADMINISTRADORES DEL SISTEMA (TELEGRAM USER_ID)
# ======================================================

def _parse_admin_user_ids() -> List[int]:
    admin_ids: List[int] = []

    raw_csv = os.getenv("ADMIN_USER_IDS", "").strip()
    if raw_csv:
        for chunk in raw_csv.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                admin_id = int(chunk)
            except ValueError:
                continue
            if admin_id > 0 and admin_id not in admin_ids:
                admin_ids.append(admin_id)

    for env_name in ("ADMIN_USER_ID_1", "ADMIN_USER_ID_2"):
        raw_value = os.getenv(env_name, "").strip()
        if not raw_value:
            continue
        try:
            admin_id = int(raw_value)
        except ValueError:
            continue
        if admin_id > 0 and admin_id not in admin_ids:
            admin_ids.append(admin_id)

    return admin_ids


ADMIN_USER_IDS: List[int] = _parse_admin_user_ids()



def is_admin(user_id: int) -> bool:
    """
    Verifica si un user_id es administrador.
    """
    return user_id in ADMIN_USER_IDS


# ======================================================
# CONTACTOS DE WHATSAPP PARA ACTIVAR PLANES
# ======================================================

def _parse_admin_whatsapps() -> List[str]:
    values: List[str] = []

    raw_csv = os.getenv("ADMIN_WHATSAPPS", "").strip()
    if raw_csv:
        for item in raw_csv.split(","):
            item = item.strip()
            if item and item not in values:
                values.append(item)

    for env_name in ("ADMIN_WHATSAPP_1", "ADMIN_WHATSAPP_2"):
        value = os.getenv(env_name, "").strip()
        if value and value not in values:
            values.append(value)

    return values


ADMIN_WHATSAPPS: List[str] = _parse_admin_whatsapps()



def get_admin_whatsapps() -> List[str]:
    """
    Retorna la lista de WhatsApps válidos (no vacíos).
    """
    return [w for w in ADMIN_WHATSAPPS if w]
