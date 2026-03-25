# app/config.py

import os
from typing import List


# ======================================================
# ADMINISTRADORES DEL SISTEMA (TELEGRAM USER_ID)
# ======================================================
# Coloca aquí los USER_ID de Telegram de los admins usando variables de entorno.
# ADMIN_USER_ID_1 = tu ID
# ADMIN_USER_ID_2 = ID de tu hermano

ADMIN_USER_IDS: List[int] = [
    int(os.getenv("ADMIN_USER_ID_1", "0")),
    int(os.getenv("ADMIN_USER_ID_2", "0")),
]


def is_admin(user_id: int) -> bool:
    """
    Verifica si un user_id es administrador.
    """
    return user_id in ADMIN_USER_IDS


# ======================================================
# CONTACTOS DE WHATSAPP PARA ACTIVAR PLANES
# ======================================================
# ADMIN_WHATSAPP_1 = tu WhatsApp
# ADMIN_WHATSAPP_2 = WhatsApp de tu hermano

ADMIN_WHATSAPPS: List[str] = [
    os.getenv("ADMIN_WHATSAPP_1", "").strip(),
    os.getenv("ADMIN_WHATSAPP_2", "").strip(),
]


def get_admin_whatsapps() -> List[str]:
    """
    Retorna la lista de WhatsApps válidos (no vacíos).
    """
    return [w for w in ADMIN_WHATSAPPS if w]
