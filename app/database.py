# app/database.py

import os
from pymongo import MongoClient

# =========================
# CONEXIÓN MONGODB
# =========================

MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

if not MONGODB_URI or not DATABASE_NAME:
    raise RuntimeError("MONGODB_URI o DATABASE_NAME no están definidos")

# MongoClient global seguro para threads
_client = None
_db = None

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGODB_URI)
    return _client

def get_db():
    global _db
    if _db is None:
        _db = get_client()[DATABASE_NAME]
    return _db


# =========================
# COLECCIONES PRINCIPALES
# =========================

def users_collection():
    """
    Usuarios del bot
    """
    return get_db()["users"]


def referrals_collection():
    """
    Historial de referidos válidos
    """
    return get_db()["referrals"]


def signals_collection():
    """
    Señales BASE generadas por el scanner
    """
    return get_db()["signals"]


def user_signals_collection():
    """
    Señales PERSONALIZADAS entregadas a cada usuario
    """
    return get_db()["user_signals"]


def signal_results_collection():
    """
    Resultados de señales (para estadísticas):
    - ganada
    - perdida
    - fecha
    - plan
    """
    return get_db()["signal_results"]
