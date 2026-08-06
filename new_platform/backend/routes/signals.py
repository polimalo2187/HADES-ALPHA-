"""
Rutas de gestión de señales de trading
CRUD completo, evaluación automática y distribución vía push notifications
"""

from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
import asyncio

from backend.routes.auth import get_current_user
from backend.database import get_signals_collection, get_users_collection
from backend.models import new_signal, update_timestamp, is_plan_active, is_trial_active, PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
from backend.config import settings

router = APIRouter()


# =========================
# MODELOS
# =========================
class SignalCreateRequest(BaseModel):
    symbol: str = Field(..., min_length=3, max_length=20, description="Par de trading (ej: BTCUSDT)")
    direction: str = Field(..., pattern="^(LONG|SHORT)$", description="Dirección de la operación")
    entry_price: float = Field(..., gt=0, description="Precio de entrada")
    stop_loss: float = Field(..., gt=0, description="Precio de stop loss")
    take_profits: List[float] = Field(..., min_items=1, max_items=5, description="Lista de take profits")
    timeframes: List[str] = Field(..., min_items=1, description="Timeframes aplicables")
    visibility: str = Field(default="premium", pattern="^(free|plus|premium)$", description="Visibilidad por plan")
    leverage: Optional[Dict[str, str]] = Field(None, description="Recomendaciones de apalancamiento")
    components: Optional[List[Any]] = Field(default=[], description="Componentes técnicos de la señal")
    score: Optional[float] = Field(None, ge=0, le=100, description="Score de confianza (0-100)")
    notes: Optional[str] = Field(None, max_length=1000, description="Notas adicionales")


class SignalUpdateRequest(BaseModel):
    entry_price: Optional[float] = Field(None, gt=0)
    stop_loss: Optional[float] = Field(None, gt=0)
    take_profits: Optional[List[float]] = Field(None, min_items=1, max_items=5)
    status: Optional[str] = Field(None, pattern="^(active|closed|cancelled)$")
    notes: Optional[str] = Field(None, max_length=1000)


class SignalResponse(BaseModel):
    signal_id: str
    symbol: str
    direction: str
    entry_price: float
    stop_loss: float
    take_profits: List[float]
    timeframes: List[str]
    leverage: Dict[str, str]
    visibility: str
    components: List[Any]
    score: Optional[float]
    status: str
    evaluated: bool
    hits: int
    result: Optional[str]
    notes: Optional[str]
    created_at: str
    updated_at: str
    closed_at: Optional[str]


class SignalListResponse(BaseModel):
    signals: List[SignalResponse]
    total: int
    page: int
    page_size: int
    active_count: int
    closed_count: int


# =========================
# FUNCIONES AUXILIARES
# =========================
async def evaluate_signal(signal_id: str):
    """Evalúa automáticamente una señal contra precios de mercado"""
    signals_col = get_signals_collection()
    
    signal = await signals_col.find_one({"signal_id": signal_id})
    if not signal or signal.get("status") != "active":
        return
    
    # Obtener precio actual (en producción esto vendría de una API de exchange)
    # Aquí simulamos con un precio base + variación aleatoria
    import random
    current_price = signal["entry_price"] * (1 + random.uniform(-0.05, 0.05))
    
    hit_tp = False
    hit_sl = False
    hit_tp_levels = []
    
    # Verificar take profits
    for i, tp in enumerate(signal["take_profits"]):
        if signal["direction"] == "LONG" and current_price >= tp:
            hit_tp = True
            hit_tp_levels.append(i + 1)
        elif signal["direction"] == "SHORT" and current_price <= tp:
            hit_tp = True
            hit_tp_levels.append(i + 1)
    
    # Verificar stop loss
    if signal["direction"] == "LONG" and current_price <= signal["stop_loss"]:
        hit_sl = True
    elif signal["direction"] == "SHORT" and current_price >= signal["stop_loss"]:
        hit_sl = True
    
    # Actualizar señal
    update_data = {
        "evaluated": True,
        "last_evaluation_at": datetime.utcnow(),
        "current_price": current_price,
    }
    
    if hit_tp:
        update_data["hits"] = signal.get("hits", 0) + len(hit_tp_levels)
        update_data["hit_tp_levels"] = hit_tp_levels
    
    if hit_sl:
        update_data["hit_sl"] = True
    
    # Determinar resultado si la señal está cerrada
    if hit_sl or (hit_tp and len(hit_tp_levels) == len(signal["take_profits"])):
        if hit_sl:
            update_data["result"] = "loss"
            update_data["status"] = "closed"
            update_data["closed_at"] = datetime.utcnow()
        elif len(hit_tp_levels) == len(signal["take_profits"]):
            update_data["result"] = "win"
            update_data["status"] = "closed"
            update_data["closed_at"] = datetime.utcnow()
    
    await signals_col.update_one(
        {"signal_id": signal_id},
        {"$set": update_data}
    )
    
    # Notificar a usuarios si hay actualización importante
    if hit_tp or hit_sl:
        await notify_signal_update(signal_id, signal, update_data)


async def notify_signal_update(signal_id: str, signal: dict, update_data: dict):
    """Envía notificaciones push sobre actualización de señal"""
    from backend.routes.websocket import manager
    
    # Determinar qué usuarios deben recibir la notificación
    users_col = get_users_collection()
    visibility = signal.get("visibility", "premium")
    
    query = {"deleted": {"$ne": True}, "banned": {"$ne": True}}
    
    if visibility == "free":
        pass  # Todos reciben
    elif visibility == "plus":
        query["$or"] = [
            {"plan": PLAN_PLUS},
            {"plan": PLAN_PREMIUM}
        ]
    else:  # premium
        query["plan"] = PLAN_PREMIUM
    
    # Agregar filtro de trial/plan activo
    query["$or"] = [
        {"subscription_status": "active"},
        {"trial_end": {"$gte": datetime.utcnow()}},
    ]
    
    users_cursor = users_col.find(query)
    user_ids = [user["user_id"] async for user in users_cursor]
    
    # Preparar mensaje
    message = {
        "type": "signal_update",
        "signal_id": signal_id,
        "symbol": signal["symbol"],
        "direction": signal["direction"],
        "updates": {
            "hit_tp": update_data.get("hit_tp_levels"),
            "hit_sl": update_data.get("hit_sl"),
            "result": update_data.get("result"),
            "status": update_data.get("status"),
        },
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    # Enviar a todos los usuarios conectados
    await manager.broadcast_to_users(user_ids, message)


def can_user_access_signal(user: dict, visibility: str) -> bool:
    """Verifica si un usuario puede acceder a una señal según su plan"""
    plan = user.get("plan", PLAN_FREE)
    is_active = is_plan_active(user) or is_trial_active(user)
    
    if not is_active:
        return False
    
    if visibility == PLAN_FREE:
        return True
    elif visibility == PLAN_PLUS:
        return plan in [PLAN_PLUS, PLAN_PREMIUM]
    else:  # premium
        return plan == PLAN_PREMIUM


# =========================
# ENDPOINTS PÚBLICOS
# =========================
@router.get("/", response_model=SignalListResponse)
async def list_signals(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None, pattern="^(active|closed|cancelled)$"),
    symbol: Optional[str] = Query(None),
    direction: Optional[str] = Query(None, pattern="^(LONG|SHORT)$"),
    visibility: Optional[str] = Query(None, pattern="^(free|plus|premium)$"),
    current_user: dict = Depends(get_current_user)
):
    """Lista señales disponibles para el usuario según su plan"""
    signals_col = get_signals_collection()
    
    # Construir filtro base
    query_filter = {"status": {"$ne": "cancelled"}}
    
    # Filtrar por visibilidad según plan del usuario
    user_plan = current_user.get("plan", PLAN_FREE)
    allowed_visibilities = [PLAN_FREE]
    
    if user_plan == PLAN_PLUS:
        allowed_visibilities.append(PLAN_PLUS)
    elif user_plan == PLAN_PREMIUM:
        allowed_visibilities.extend([PLAN_PLUS, PLAN_PREMIUM])
    
    query_filter["visibility"] = {"$in": allowed_visibilities}
    
    # Aplicar filtros adicionales
    if status:
        query_filter["status"] = status
    
    if symbol:
        query_filter["symbol"] = {"$regex": symbol, "$options": "i"}
    
    if direction:
        query_filter["direction"] = direction
    
    # Calcular offset
    skip = (page - 1) * page_size
    
    # Obtener señales
    signals_cursor = signals_col.find(query_filter).skip(skip).limit(page_size).sort("created_at", -1)
    signals_list = await signals_cursor.to_list(length=page_size)
    
    # Contar totales
    total = await signals_col.count_documents(query_filter)
    active_count = await signals_col.count_documents({**query_filter, "status": "active"})
    closed_count = await signals_col.count_documents({**query_filter, "status": "closed"})
    
    # Formatear respuesta
    signals = []
    for sig in signals_list:
        signals.append(SignalResponse(
            signal_id=sig["signal_id"],
            symbol=sig["symbol"],
            direction=sig["direction"],
            entry_price=sig["entry_price"],
            stop_loss=sig["stop_loss"],
            take_profits=sig["take_profits"],
            timeframes=sig["timeframes"],
            leverage=sig.get("leverage", {}),
            visibility=sig["visibility"],
            components=sig.get("components", []),
            score=sig.get("score"),
            status=sig.get("status", "active"),
            evaluated=sig.get("evaluated", False),
            hits=sig.get("hits", 0),
            result=sig.get("result"),
            notes=sig.get("notes"),
            created_at=sig["created_at"].isoformat(),
            updated_at=sig["updated_at"].isoformat(),
            closed_at=sig.get("closed_at").isoformat() if sig.get("closed_at") else None,
        ))
    
    return SignalListResponse(
        signals=signals,
        total=total,
        page=page,
        page_size=page_size,
        active_count=active_count,
        closed_count=closed_count,
    )


@router.get("/{signal_id}", response_model=SignalResponse)
async def get_signal(
    signal_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Obtiene detalles de una señal específica"""
    signals_col = get_signals_collection()
    
    signal = await signals_col.find_one({"signal_id": signal_id})
    if not signal:
        raise HTTPException(status_code=404, detail="Señal no encontrada")
    
    # Verificar permisos
    if not can_user_access_signal(current_user, signal["visibility"]):
        raise HTTPException(status_code=403, detail="No tienes acceso a esta señal. Mejora tu plan.")
    
    return SignalResponse(
        signal_id=signal["signal_id"],
        symbol=signal["symbol"],
        direction=signal["direction"],
        entry_price=signal["entry_price"],
        stop_loss=signal["stop_loss"],
        take_profits=signal["take_profits"],
        timeframes=signal["timeframes"],
        leverage=signal.get("leverage", {}),
        visibility=signal["visibility"],
        components=signal.get("components", []),
        score=signal.get("score"),
        status=signal.get("status", "active"),
        evaluated=signal.get("evaluated", False),
        hits=signal.get("hits", 0),
        result=signal.get("result"),
        notes=signal.get("notes"),
        created_at=signal["created_at"].isoformat(),
        updated_at=signal["updated_at"].isoformat(),
        closed_at=signal.get("closed_at").isoformat() if signal.get("closed_at") else None,
    )


@router.post("/", response_model=SignalResponse, status_code=201)
async def create_signal(
    request: SignalCreateRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Crea una nueva señal (solo admin/premium)"""
    # Verificar permisos - solo premium o admin pueden crear señales
    if current_user.get("plan") != PLAN_PREMIUM:
        raise HTTPException(status_code=403, detail="Se requiere plan Premium para crear señales")
    
    signals_col = get_signals_collection()
    
    # Generar ID único
    import uuid
    signal_id = str(uuid.uuid4())
    
    # Crear señal
    signal_doc = new_signal(
        symbol=request.symbol.upper(),
        direction=request.direction,
        entry_price=request.entry_price,
        stop_loss=request.stop_loss,
        take_profits=request.take_profits,
        timeframes=request.timeframes,
        visibility=request.visibility,
        leverage=request.leverage,
        components=request.components,
        score=request.score,
    )
    
    signal_doc["signal_id"] = signal_id
    signal_doc["status"] = "active"
    signal_doc["created_by"] = current_user["user_id"]
    signal_doc["notes"] = request.notes
    
    # Insertar en BD
    result = await signals_col.insert_one(signal_doc)
    if not result.inserted_id:
        raise HTTPException(status_code=500, detail="Error creando señal")
    
    # Programar evaluación automática
    background_tasks.add_task(evaluate_signal, signal_id)
    
    # Notificar a usuarios sobre nueva señal
    from backend.routes.websocket import manager
    
    users_col = get_users_collection()
    query = {"deleted": {"$ne": True}, "banned": {"$ne": True}}
    
    if request.visibility == PLAN_FREE:
        pass
    elif request.visibility == PLAN_PLUS:
        query["$or"] = [{"plan": PLAN_PLUS}, {"plan": PLAN_PREMIUM}]
    else:
        query["plan"] = PLAN_PREMIUM
    
    query["$or"] = [
        {"subscription_status": "active"},
        {"trial_end": {"$gte": datetime.utcnow()}},
    ]
    
    users_cursor = users_col.find(query)
    user_ids = [user["user_id"] async for user in users_cursor]
    
    message = {
        "type": "new_signal",
        "signal_id": signal_id,
        "symbol": request.symbol.upper(),
        "direction": request.direction,
        "entry_price": request.entry_price,
        "visibility": request.visibility,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    await manager.broadcast_to_users(user_ids, message)
    
    return SignalResponse(
        signal_id=signal_id,
        symbol=signal_doc["symbol"],
        direction=signal_doc["direction"],
        entry_price=signal_doc["entry_price"],
        stop_loss=signal_doc["stop_loss"],
        take_profits=signal_doc["take_profits"],
        timeframes=signal_doc["timeframes"],
        leverage=signal_doc.get("leverage", {}),
        visibility=signal_doc["visibility"],
        components=signal_doc.get("components", []),
        score=signal_doc.get("score"),
        status=signal_doc["status"],
        evaluated=signal_doc["evaluated"],
        hits=signal_doc.get("hits", 0),
        result=signal_doc.get("result"),
        notes=signal_doc.get("notes"),
        created_at=signal_doc["created_at"].isoformat(),
        updated_at=signal_doc["updated_at"].isoformat(),
        closed_at=None,
    )


@router.put("/{signal_id}", response_model=SignalResponse)
async def update_signal(
    signal_id: str,
    request: SignalUpdateRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Actualiza una señal existente (solo admin/premium)"""
    if current_user.get("plan") != PLAN_PREMIUM:
        raise HTTPException(status_code=403, detail="Se requiere plan Premium")
    
    signals_col = get_signals_collection()
    
    signal = await signals_col.find_one({"signal_id": signal_id})
    if not signal:
        raise HTTPException(status_code=404, detail="Señal no encontrada")
    
    # Construir datos de actualización
    update_data = {}
    
    if request.entry_price is not None:
        update_data["entry_price"] = request.entry_price
    
    if request.stop_loss is not None:
        update_data["stop_loss"] = request.stop_loss
    
    if request.take_profits is not None:
        update_data["take_profits"] = request.take_profits
    
    if request.status is not None:
        update_data["status"] = request.status
        if request.status == "closed":
            update_data["closed_at"] = datetime.utcnow()
    
    if request.notes is not None:
        update_data["notes"] = request.notes
    
    if not update_data:
        # Retornar señal sin cambios
        return SignalResponse(
            signal_id=signal["signal_id"],
            symbol=signal["symbol"],
            direction=signal["direction"],
            entry_price=signal["entry_price"],
            stop_loss=signal["stop_loss"],
            take_profits=signal["take_profits"],
            timeframes=signal["timeframes"],
            leverage=signal.get("leverage", {}),
            visibility=signal["visibility"],
            components=signal.get("components", []),
            score=signal.get("score"),
            status=signal.get("status", "active"),
            evaluated=signal.get("evaluated", False),
            hits=signal.get("hits", 0),
            result=signal.get("result"),
            notes=signal.get("notes"),
            created_at=signal["created_at"].isoformat(),
            updated_at=signal["updated_at"].isoformat(),
            closed_at=signal.get("closed_at").isoformat() if signal.get("closed_at") else None,
        )
    
    update_data["updated_at"] = datetime.utcnow()
    
    # Actualizar en BD
    await signals_col.update_one(
        {"signal_id": signal_id},
        {"$set": update_data}
    )
    
    # Obtener señal actualizada
    updated_signal = await signals_col.find_one({"signal_id": signal_id})
    
    # Programar re-evaluación
    background_tasks.add_task(evaluate_signal, signal_id)
    
    # Notificar actualización
    await notify_signal_update(signal_id, updated_signal, update_data)
    
    return SignalResponse(
        signal_id=updated_signal["signal_id"],
        symbol=updated_signal["symbol"],
        direction=updated_signal["direction"],
        entry_price=updated_signal["entry_price"],
        stop_loss=updated_signal["stop_loss"],
        take_profits=updated_signal["take_profits"],
        timeframes=updated_signal["timeframes"],
        leverage=updated_signal.get("leverage", {}),
        visibility=updated_signal["visibility"],
        components=updated_signal.get("components", []),
        score=updated_signal.get("score"),
        status=updated_signal.get("status", "active"),
        evaluated=updated_signal.get("evaluated", False),
        hits=updated_signal.get("hits", 0),
        result=updated_signal.get("result"),
        notes=updated_signal.get("notes"),
        created_at=updated_signal["created_at"].isoformat(),
        updated_at=updated_signal["updated_at"].isoformat(),
        closed_at=updated_signal.get("closed_at").isoformat() if updated_signal.get("closed_at") else None,
    )


@router.delete("/{signal_id}")
async def delete_signal(
    signal_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Elimina una señal (solo admin/premium)"""
    if current_user.get("plan") != PLAN_PREMIUM:
        raise HTTPException(status_code=403, detail="Se requiere plan Premium")
    
    signals_col = get_signals_collection()
    
    result = await signals_col.delete_one({"signal_id": signal_id})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Señal no encontrada")
    
    # Notificar eliminación
    from backend.routes.websocket import manager
    
    message = {
        "type": "signal_deleted",
        "signal_id": signal_id,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    # Broadcast a todos los conectados
    await manager.broadcast(message)
    
    return {"message": "Señal eliminada exitosamente"}


@router.post("/{signal_id}/evaluate")
async def trigger_evaluation(
    signal_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Dispara evaluación manual de una señal"""
    if current_user.get("plan") != PLAN_PREMIUM:
        raise HTTPException(status_code=403, detail="Se requiere plan Premium")
    
    signals_col = get_signals_collection()
    
    signal = await signals_col.find_one({"signal_id": signal_id})
    if not signal:
        raise HTTPException(status_code=404, detail="Señal no encontrada")
    
    # Programar evaluación
    background_tasks.add_task(evaluate_signal, signal_id)
    
    return {"message": "Evaluación programada", "signal_id": signal_id}


__all__ = ["router"]
