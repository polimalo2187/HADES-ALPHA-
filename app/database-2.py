from __future__ import annotations

import logging
import os
from typing import Iterable, List, Sequence, Tuple

from pymongo import ASCENDING, DESCENDING, IndexModel, MongoClient
from pymongo.errors import OperationFailure, PyMongoError

logger = logging.getLogger(__name__)

# =========================
# CONEXIÓN MONGODB
# =========================


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        value = value.strip()
        if value:
            return value
    return None


MONGODB_URI = _first_env('MONGODB_URI', 'MONGO_URI')
DATABASE_NAME = _first_env('DATABASE_NAME', 'MONGO_DB_NAME')

if not MONGODB_URI or not DATABASE_NAME:
    present = {
        'MONGODB_URI': bool(_first_env('MONGODB_URI')),
        'MONGO_URI': bool(_first_env('MONGO_URI')),
        'DATABASE_NAME': bool(_first_env('DATABASE_NAME')),
        'MONGO_DB_NAME': bool(_first_env('MONGO_DB_NAME')),
    }
    logger.error('Mongo env vars ausentes o vacías: %s', present)
    raise RuntimeError(
        'Faltan variables de MongoDB. Usa MONGODB_URI o MONGO_URI, y DATABASE_NAME o MONGO_DB_NAME.'
    )

# MongoClient global seguro para threads
_client = None
_db = None
_indexes_initialized = False


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
    """Usuarios del bot"""
    return get_db()['users']


def referrals_collection():
    """Historial de referidos válidos"""
    return get_db()['referrals']


def signals_collection():
    """Señales BASE generadas por el scanner"""
    return get_db()['signals']


def user_signals_collection():
    """Señales PERSONALIZADAS entregadas a cada usuario"""
    return get_db()['user_signals']


def signal_results_collection():
    """Resultados de señales para estadísticas"""
    return get_db()['signal_results']


def watchlists_collection():
    """Watchlists por usuario"""
    return get_db()['watchlists']


def signal_jobs_collection():
    """Jobs persistidos de despacho rápido de señales"""
    return get_db()['signal_jobs']


def signal_deliveries_collection():
    """Tracking de entrega de push por usuario y señal"""
    return get_db()['signal_deliveries']


def stats_snapshots_collection():
    """Snapshots materializados de estadísticas"""
    return get_db()['stats_snapshots']


def signal_history_collection():
    """Histórico persistente y verificable de señales cerradas"""
    return get_db()['signal_history']


def subscription_events_collection():
    """Auditoría comercial de activaciones, upgrades, rewards y expiraciones"""
    return get_db()['subscription_events']


def payment_orders_collection():
    """Órdenes de pago automáticas BEP-20"""
    return get_db()['payment_orders']


def payment_verification_logs_collection():
    """Auditoría de verificaciones on-chain de pagos"""
    return get_db()['payment_verification_logs']


def audit_logs_collection():
    """Auditoría operacional y de errores críticos"""
    return get_db()['audit_logs']


def system_health_collection():
    """Heartbeat y salud de componentes del sistema"""
    return get_db()['system_health']


