"""
Conexión y gestión de MongoDB
"""

from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional
import logging

from backend.config import settings

logger = logging.getLogger(__name__)

# Cliente global de MongoDB
client: Optional[AsyncIOMotorClient] = None
db = None


async def initialize_database():
    """Inicializa la conexión a MongoDB"""
    global client, db
    
    try:
        client = AsyncIOMotorClient(
            settings.MONGODB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
        )
        
        # Forzar conexión para verificar que funciona
        await client.admin.command('ping')
        
        db = client[settings.DATABASE_NAME]
        
        # Crear índices necesarios
        await _create_indexes()
        
        logger.info(f"✅ Conectado a MongoDB: {settings.DATABASE_NAME}")
        
    except Exception as e:
        logger.error(f"❌ Error conectando a MongoDB: {e}")
        raise


async def close_database():
    """Cierra la conexión a MongoDB"""
    global client
    
    if client:
        client.close()
        client = None
        logger.info("🔌 Conexión a MongoDB cerrada")


async def check_database_connection() -> bool:
    """Verifica si la conexión a MongoDB está activa"""
    global client
    
    if not client:
        return False
    
    try:
        await client.admin.command('ping')
        return True
    except Exception:
        return False


async def _create_indexes():
    """Crea los índices necesarios en las colecciones"""
    if db is None:
        return
    
    try:
        # Índices para usuarios
        await db.users.create_index("user_id", unique=True)
        await db.users.create_index("phone", unique=True)
        await db.users.create_index("auth_provider")
        await db.users.create_index("plan")
        await db.users.create_index("subscription_status")
        await db.users.create_index("last_activity")
        
        # Índices para señales
        await db.signals.create_index("symbol")
        await db.signals.create_index("direction")
        await db.signals.create_index("visibility")
        await db.signals.create_index("created_at")
        await db.signals.create_index([("symbol", 1), ("created_at", -1)])
        
        # Índices para señales de usuario
        await db.user_signals.create_index("user_id")
        await db.user_signals.create_index("signal_id")
        await db.user_signals.create_index("visibility")
        await db.user_signals.create_index("valid_until")
        
        # Índices para pagos
        await db.payment_orders.create_index("order_id", unique=True)
        await db.payment_orders.create_index("user_id")
        await db.payment_orders.create_index("status")
        await db.payment_orders.create_index("expires_at")
        
        # Índices para sesiones push
        await db.push_sessions.create_index("session_id", unique=True)
        await db.push_sessions.create_index("user_id")
        await db.push_sessions.create_index("is_active")
        await db.push_sessions.create_index("last_heartbeat")
        
        # Índices para historial
        await db.signal_history.create_index("user_id")
        await db.signal_history.create_index("signal_id")
        await db.signal_history.create_index("evaluated_at")
        
        # Índices para eventos de auditoría
        await db.audit_logs.create_index("event_type")
        await db.audit_logs.create_index("user_id")
        await db.audit_logs.create_index("created_at")
        
        logger.info("✅ Índices de base de datos creados/verificados")
        
    except Exception as e:
        logger.warning(f"⚠️ Error creando índices: {e}")


def get_database():
    """Obtiene la instancia de la base de datos"""
    if db is None:
        raise RuntimeError("Base de datos no inicializada. Llama a initialize_database() primero")
    return db


def get_users_collection():
    """Obtiene la colección de usuarios"""
    return get_database().users


def get_signals_collection():
    """Obtiene la colección de señales"""
    return get_database().signals


def get_user_signals_collection():
    """Obtiene la colección de señales de usuario"""
    return get_database().user_signals


def get_payment_orders_collection():
    """Obtiene la colección de órdenes de pago"""
    return get_database().payment_orders


def get_push_sessions_collection():
    """Obtiene la colección de sesiones push"""
    return get_database().push_sessions


def get_signal_history_collection():
    """Obtiene la colección de historial de señales"""
    return get_database().signal_history


def get_audit_logs_collection():
    """Obtiene la colección de logs de auditoría"""
    return get_database().audit_logs


__all__ = [
    "initialize_database",
    "close_database",
    "check_database_connection",
    "get_database",
    "get_users_collection",
    "get_signals_collection",
    "get_user_signals_collection",
    "get_payment_orders_collection",
    "get_push_sessions_collection",
    "get_signal_history_collection",
    "get_audit_logs_collection",
]
