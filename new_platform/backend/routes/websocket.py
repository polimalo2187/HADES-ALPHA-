"""
Rutas de WebSocket para Push Notifications internas
Sistema de notificaciones en tiempo real sin servicios de terceros
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from typing import Dict, List, Optional
import asyncio
import logging
import json
from datetime import datetime

from backend.config import settings
from backend.database import get_push_sessions_collection, get_users_collection
from backend.models import new_push_session

router = APIRouter()
logger = logging.getLogger(__name__)


# =========================
# GESTIÓN DE CONEXIONES ACTIVAS
# =========================
class ConnectionManager:
    """Gestiona las conexiones WebSocket activas"""
    
    def __init__(self):
        # {user_id: [WebSocket, ...]}
        self.active_connections: Dict[int, List[WebSocket]] = {}
        # {session_id: user_id}
        self.session_map: Dict[str, int] = {}
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket, user_id: int, session_id: str) -> bool:
        """Acepta una nueva conexión WebSocket"""
        await websocket.accept()
        
        async with self._lock:
            # Verificar límite de conexiones por usuario
            if user_id not in self.active_connections:
                self.active_connections[user_id] = []
            
            if len(self.active_connections[user_id]) >= settings.PUSH_MAX_CONNECTIONS_PER_USER:
                # Cerrar la conexión más antigua
                old_ws = self.active_connections[user_id].pop(0)
                try:
                    await old_ws.close()
                except Exception:
                    pass
            
            self.active_connections[user_id].append(websocket)
            self.session_map[session_id] = user_id
        
        # Registrar sesión en base de datos
        try:
            sessions_col = get_push_sessions_collection()
            session_doc = new_push_session(user_id, session_id)
            await sessions_col.insert_one(session_doc)
        except Exception as e:
            logger.warning(f"Error registrando sesión push: {e}")
        
        logger.info(f"🔌 WebSocket conectado: user_id={user_id}, session_id={session_id}")
        return True
    
    async def disconnect(self, websocket: WebSocket, session_id: str):
        """Cierra una conexión WebSocket"""
        async with self._lock:
            user_id = self.session_map.get(session_id)
            
            if user_id is not None:
                if user_id in self.active_connections:
                    try:
                        self.active_connections[user_id].remove(websocket)
                    except ValueError:
                        pass
                    
                    if not self.active_connections[user_id]:
                        del self.active_connections[user_id]
                
                del self.session_map[session_id]
        
        # Marcar sesión como inactiva en base de datos
        try:
            sessions_col = get_push_sessions_collection()
            await sessions_col.update_one(
                {"session_id": session_id},
                {"$set": {"is_active": False, "updated_at": datetime.utcnow()}}
            )
        except Exception as e:
            logger.warning(f"Error actualizando sesión push: {e}")
        
        logger.info(f"🔌 WebSocket desconectado: session_id={session_id}")
    
    async def send_to_user(self, user_id: int, message: dict):
        """Envía un mensaje a todas las conexiones de un usuario"""
        async with self._lock:
            connections = self.active_connections.get(user_id, []).copy()
        
        if not connections:
            return
        
        message_json = json.dumps(message)
        
        for websocket in connections:
            try:
                await websocket.send_text(message_json)
            except Exception as e:
                logger.warning(f"Error enviando push a user_id={user_id}: {e}")
    
    async def broadcast(self, message: dict, target_users: Optional[List[int]] = None):
        """Envía un mensaje a múltiples usuarios"""
        if target_users:
            tasks = [self.send_to_user(user_id, message) for user_id in target_users]
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            # Broadcast a todos los usuarios conectados
            async with self._lock:
                all_user_ids = list(self.active_connections.keys())
            
            tasks = [self.send_to_user(user_id, message) for user_id in all_user_ids]
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def update_heartbeat(self, session_id: str):
        """Actualiza el último heartbeat de una sesión"""
        try:
            sessions_col = get_push_sessions_collection()
            await sessions_col.update_one(
                {"session_id": session_id},
                {"$set": {"last_heartbeat": datetime.utcnow(), "updated_at": datetime.utcnow()}}
            )
        except Exception as e:
            logger.warning(f"Error actualizando heartbeat: {e}")


# Instancia global del manager
manager = ConnectionManager()


# =========================
# ENDPOINT WEBSOCKET
# =========================
@router.websocket("/push")
async def websocket_push(
    websocket: WebSocket,
    token: str = Query(..., description="Token JWT del usuario"),
    session_id: str = Query(..., description="ID único de sesión"),
):
    """
    Endpoint WebSocket para notificaciones push en tiempo real.
    
    El cliente debe conectarse con:
    - token: JWT obtenido del login/registro
    - session_id: ID único generado por el cliente para esta sesión
    
    Mensajes soportados:
    - {"type": "ping"} -> {"type": "pong", "timestamp": ...}
    - {"type": "subscribe", "tiers": ["free", "plus"]} -> Suscribirse a tiers específicos
    - {"type": "unsubscribe"} -> Cancelar suscripción
    """
    import jwt
    
    # Validar token
    try:
        payload = jwt.decode(token, settings.AUTH_SESSION_SECRET, algorithms=["HS256"])
        user_id = payload.get("user_id")
        
        if not user_id:
            await websocket.close(code=4001, reason="Token inválido")
            return
        
    except jwt.ExpiredSignatureError:
        await websocket.close(code=4002, reason="Token expirado")
        return
    except jwt.InvalidTokenError:
        await websocket.close(code=4003, reason="Token inválido")
        return
    
    # Conectar
    connected = await manager.connect(websocket, user_id, session_id)
    if not connected:
        await websocket.close(code=4004, reason="Límite de conexiones alcanzado")
        return
    
    # Enviar mensaje de bienvenida
    await websocket.send_json({
        "type": "connected",
        "user_id": user_id,
        "session_id": session_id,
        "timestamp": datetime.utcnow().isoformat(),
    })
    
    # Loop principal
    try:
        while True:
            try:
                # Esperar mensaje del cliente con timeout para heartbeat
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=settings.PUSH_HEARTBEAT_INTERVAL_SECONDS
                )
                
                message = json.loads(data)
                msg_type = message.get("type")
                
                if msg_type == "ping":
                    await websocket.send_json({
                        "type": "pong",
                        "timestamp": datetime.utcnow().isoformat(),
                    })
                    await manager.update_heartbeat(session_id)
                
                elif msg_type == "subscribe":
                    tiers = message.get("tiers", [])
                    logger.info(f"Usuario {user_id} se suscribe a tiers: {tiers}")
                    # Aquí se podría guardar la preferencia en DB
                
                elif msg_type == "unsubscribe":
                    logger.info(f"Usuario {user_id} cancela suscripción")
                
            except asyncio.TimeoutError:
                # Enviar heartbeat automático
                await websocket.send_json({
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat(),
                })
                await manager.update_heartbeat(session_id)
                
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"Error en WebSocket: {e}")
    finally:
        await manager.disconnect(websocket, session_id)


# =========================
# FUNCIONES PARA ENVIAR NOTIFICACIONES
# =========================
async def send_signal_notification(
    signal_id: str,
    visibility: str,
    symbol: str,
    direction: str,
    user_ids: Optional[List[int]] = None
):
    """Envía notificación de nueva señal a los usuarios"""
    message = {
        "type": "new_signal",
        "signal_id": signal_id,
        "visibility": visibility,
        "symbol": symbol,
        "direction": direction,
        "title": f"📢 Nueva Señal: {symbol}",
        "body": f"{direction.upper()} - Toca para ver detalles",
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    await manager.broadcast(message, target_users=user_ids)
    logger.info(f"📨 Notificación de señal enviada: {symbol} ({direction})")


async def send_payment_notification(
    user_id: int,
    order_id: str,
    status: str,
    plan: str,
):
    """Envía notificación de estado de pago"""
    message = {
        "type": "payment_update",
        "order_id": order_id,
        "status": status,
        "plan": plan,
        "title": "💰 Actualización de Pago",
        "body": f"Tu pago ha sido {status}",
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    await manager.send_to_user(user_id, message)


async def send_system_notification(
    user_ids: List[int],
    title: str,
    body: str,
    data: Optional[dict] = None,
):
    """Envía notificación del sistema"""
    message = {
        "type": "system",
        "title": title,
        "body": body,
        "data": data or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    await manager.broadcast(message, target_users=user_ids)


__all__ = ["router", "manager", "send_signal_notification", "send_payment_notification"]
