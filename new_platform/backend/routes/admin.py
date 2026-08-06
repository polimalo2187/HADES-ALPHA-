"""
Rutas de Administración - Panel de Control Interno
Gestión de usuarios, señales, pagos y configuración del sistema
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from bson import ObjectId

from backend.database import get_database
from backend.models import activate_plan, is_plan_active, is_trial_active, update_timestamp
from backend.config import settings

router = APIRouter(prefix="/admin", tags=["admin"])


# =========================
# VERIFICACIÓN DE ADMIN
# =========================
async def verify_admin(db, user_id: int) -> bool:
    """Verifica si un usuario es administrador"""
    user = await db.users.find_one({"user_id": user_id})
    
    if not user:
        return False
    
    # Verificar si tiene rol de admin
    return user.get("is_admin", False) or user.get("role") == "admin"


async def get_current_admin(
    authorization: str,
    db=Depends(get_database),
):
    """Obtiene el admin actual desde el token"""
    # Implementación simplificada - en producción usar JWT
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token inválido")
    
    token = authorization.replace("Bearer ", "")
    
    # Buscar usuario por token (en producción decodificar JWT)
    # Aquí asumimos que el token contiene el user_id
    try:
        user_id = int(token.split("-")[0])
    except:
        raise HTTPException(status_code=401, detail="Token inválido")
    
    is_admin_user = await verify_admin(db, user_id)
    
    if not is_admin_user:
        raise HTTPException(status_code=403, detail="No tienes permisos de administrador")
    
    return user_id


# =========================
# ESTADÍSTICAS DEL SISTEMA
# =========================

@router.get("/stats/overview")
async def get_system_overview(
    db=Depends(get_database),
):
    """Obtiene estadísticas generales del sistema"""
    
    # Contar usuarios
    total_users = await db.users.count_documents({})
    active_users = await db.users.count_documents({"last_activity": {"$gte": datetime.utcnow() - timedelta(days=7)}})
    
    # Contar por plan
    free_users = await db.users.count_documents({"plan": "free"})
    plus_users = await db.users.count_documents({"plan": "plus"})
    premium_users = await db.users.count_documents({"plan": "premium"})
    
    # Usuarios en trial
    trial_users = await db.users.count_documents({"subscription_status": "trial"})
    
    # Pagos
    total_orders = await db.payment_orders.count_documents({})
    pending_orders = await db.payment_orders.count_documents({"status": "awaiting_payment"})
    completed_orders = await db.payment_orders.count_documents({"status": "completed"})
    total_revenue = await db.payment_orders.aggregate([
        {"$match": {"status": "completed"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount_usdt"}}}
    ]).to_list(length=1)
    total_revenue = total_revenue[0]["total"] if total_revenue else 0
    
    # Señales
    total_signals = await db.signals.count_documents({})
    signals_today = await db.signals.count_documents({
        "created_at": {"$gte": datetime.utcnow().replace(hour=0, minute=0, second=0)}
    })
    
    # Sesiones push activas
    active_push_sessions = await db.push_sessions.count_documents({"is_active": True})
    
    return {
        "users": {
            "total": total_users,
            "active_last_7_days": active_users,
            "by_plan": {
                "free": free_users,
                "plus": plus_users,
                "premium": premium_users,
            },
            "trial": trial_users,
        },
        "payments": {
            "total_orders": total_orders,
            "pending_orders": pending_orders,
            "completed_orders": completed_orders,
            "total_revenue_usdt": round(total_revenue, 2),
        },
        "signals": {
            "total": total_signals,
            "today": signals_today,
        },
        "push_notifications": {
            "active_sessions": active_push_sessions,
        },
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/stats/revenue")
async def get_revenue_stats(
    days: int = Query(default=30, ge=1, le=365),
    db=Depends(get_database),
):
    """Obtiene estadísticas de ingresos por período"""
    
    start_date = datetime.utcnow() - timedelta(days=days)
    
    # Ingresos por día
    pipeline = [
        {"$match": {
            "status": "completed",
            "confirmed_at": {"$gte": start_date}
        }},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$confirmed_at"}},
            "total": {"$sum": "$amount_usdt"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    
    daily_revenue = await db.payment_orders.aggregate(pipeline).to_list(length=None)
    
    # Ingresos por plan
    plan_pipeline = [
        {"$match": {
            "status": "completed",
            "confirmed_at": {"$gte": start_date}
        }},
        {"$group": {
            "_id": "$plan",
            "total": {"$sum": "$amount_usdt"},
            "count": {"$sum": 1}
        }}
    ]
    
    plan_revenue = await db.payment_orders.aggregate(plan_pipeline).to_list(length=None)
    
    return {
        "period_days": days,
        "start_date": start_date.isoformat(),
        "end_date": datetime.utcnow().isoformat(),
        "daily_revenue": [
            {"date": item["_id"], "total_usdt": round(item["total"], 2), "orders": item["count"]}
            for item in daily_revenue
        ],
        "by_plan": [
            {"plan": item["_id"], "total_usdt": round(item["total"], 2), "orders": item["count"]}
            for item in plan_revenue
        ],
        "total_usdt": round(sum(item["total"] for item in daily_revenue), 2),
    }


# =========================
# GESTIÓN DE USUARIOS
# =========================

@router.get("/users")
async def list_users(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
    plan: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    db=Depends(get_database),
):
    """Lista usuarios con paginación y filtros"""
    
    query = {}
    
    if plan:
        query["plan"] = plan
    
    if search:
        query["$or"] = [
            {"phone": {"$regex": search, "$options": "i"}},
            {"username": {"$regex": search, "$options": "i"}},
            {"user_id": int(search)} if search.isdigit() else None,
        ]
        query["$or"] = [q for q in query["$or"] if q is not None]
    
    skip = (page - 1) * limit
    
    users_cursor = db.users.find(query, skip=skip, limit=limit, sort=[("created_at", -1)])
    users = await users_cursor.to_list(length=limit)
    
    total = await db.users.count_documents(query)
    
    return {
        "users": [
            {
                "user_id": u["user_id"],
                "phone": u["phone"],
                "username": u.get("username"),
                "plan": u["plan"],
                "subscription_status": u["subscription_status"],
                "trial_end": u.get("trial_end", {}).isoformat() if u.get("trial_end") else None,
                "plan_end": u.get("plan_end", {}).isoformat() if u.get("plan_end") else None,
                "created_at": u["created_at"].isoformat(),
                "last_activity": u.get("last_activity", {}).isoformat() if u.get("last_activity") else None,
                "banned": u.get("banned", False),
            }
            for u in users
        ],
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": (total + limit - 1) // limit,
        },
    }


@router.get("/users/{user_id}")
async def get_user_detail(user_id: int, db=Depends(get_database)):
    """Obtiene detalles completos de un usuario"""
    
    user = await db.users.find_one({"user_id": user_id})
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # Obtener historial de pagos
    payments = await db.payment_orders.find(
        {"user_id": user_id},
        sort=[("created_at", -1)],
        limit=20
    ).to_list(length=20)
    
    # Obtener sesiones push activas
    push_sessions = await db.push_sessions.find(
        {"user_id": user_id, "is_active": True}
    ).to_list(length=10)
    
    return {
        "user": {
            "user_id": user["user_id"],
            "phone": user["phone"],
            "username": user.get("username"),
            "plan": user["plan"],
            "subscription_status": user["subscription_status"],
            "trial_end": user.get("trial_end", {}).isoformat() if user.get("trial_end") else None,
            "plan_end": user.get("plan_end", {}).isoformat() if user.get("plan_end") else None,
            "ref_code": user.get("ref_code"),
            "referred_by": user.get("referred_by"),
            "language": user.get("language", "es"),
            "push_settings": user.get("push_settings", {}),
            "banned": user.get("banned", False),
            "created_at": user["created_at"].isoformat(),
            "last_activity": user.get("last_activity", {}).isoformat() if user.get("last_activity") else None,
        },
        "recent_payments": [
            {
                "order_id": p["order_id"],
                "plan": p["plan"],
                "amount_usdt": p["amount_usdt"],
                "status": p["status"],
                "created_at": p["created_at"].isoformat(),
            }
            for p in payments
        ],
        "active_push_sessions": len(push_sessions),
    }


@router.post("/users/{user_id}/ban")
async def ban_user(
    user_id: int,
    reason: str = Query(default="Violación de términos"),
    db=Depends(get_database),
):
    """Banea un usuario"""
    
    user = await db.users.find_one({"user_id": user_id})
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "banned": True,
                "ban_reason": reason,
                "banned_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
        }
    )
    
    # Cerrar sesiones push
    await db.push_sessions.update_many(
        {"user_id": user_id},
        {"$set": {"is_active": False, "updated_at": datetime.utcnow()}}
    )
    
    return {"success": True, "message": f"Usuario {user_id} baneado", "reason": reason}


@router.post("/users/{user_id}/unban")
async def unban_user(user_id: int, db=Depends(get_database)):
    """Desbanea un usuario"""
    
    user = await db.users.find_one({"user_id": user_id})
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "banned": False,
                "unbanned_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            },
            "$unset": {"ban_reason": ""}
        }
    )
    
    return {"success": True, "message": f"Usuario {user_id} desbaneado"}


@router.post("/users/{user_id}/activate-plan")
async def manually_activate_plan(
    user_id: int,
    plan: str,
    days: int = Query(default=30, ge=1),
    db=Depends(get_database),
):
    """Activa manualmente un plan para un usuario (soporte)"""
    
    user = await db.users.find_one({"user_id": user_id})
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    if plan not in ["free", "plus", "premium"]:
        raise HTTPException(status_code=400, detail="Plan inválido")
    
    updated_user = activate_plan(user.copy(), plan, days)
    
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": updated_user}
    )
    
    return {
        "success": True,
        "message": f"Plan {plan} activado por {days} días",
        "new_plan_end": updated_user["plan_end"].isoformat() if updated_user["plan_end"] else None,
    }


# =========================
# GESTIÓN DE SEÑALES
# =========================

@router.get("/signals")
async def list_admin_signals(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
    visibility: Optional[str] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    db=Depends(get_database),
):
    """Lista todas las señales con filtros"""
    
    query = {}
    
    if visibility:
        query["visibility"] = visibility
    
    if symbol:
        query["symbol"] = {"$regex": symbol, "$options": "i"}
    
    skip = (page - 1) * limit
    
    signals_cursor = db.signals.find(query, skip=skip, limit=limit, sort=[("created_at", -1)])
    signals = await signals_cursor.to_list(length=limit)
    
    total = await db.signals.count_documents(query)
    
    return {
        "signals": [
            {
                "id": str(s.get("_id", "")),
                "symbol": s["symbol"],
                "direction": s["direction"],
                "entry_price": s["entry_price"],
                "stop_loss": s["stop_loss"],
                "take_profits": s["take_profits"],
                "visibility": s["visibility"],
                "created_at": s["created_at"].isoformat(),
            }
            for s in signals
        ],
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": (total + limit - 1) // limit,
        },
    }


@router.delete("/signals/{signal_id}")
async def delete_signal(signal_id: str, db=Depends(get_database)):
    """Elimina una señal"""
    
    result = await db.signals.delete_one({"_id": ObjectId(signal_id)})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Señal no encontrada")
    
    return {"success": True, "message": "Señal eliminada"}


# =========================
# CONFIGURACIÓN DEL SISTEMA
# =========================

@router.get("/config")
async def get_system_config(db=Depends(get_database)):
    """Obtiene configuración del sistema"""
    
    return {
        "system": {
            "name": "Hades Platform",
            "version": "2.0.0",
            "auth_provider": "phone_password",
            "push_provider": "internal_websocket",
        },
        "plans": {
            "free": {"price_usdt": 0, "trial_days": 5},
            "plus": {"price_usdt": 29.99, "features": ["señales_basicas", "push_notifications"]},
            "premium": {"price_usdt": 59.99, "features": ["todas_señales", "push_priority", "soporte_prioritario"]},
        },
        "payment": {
            "network": "BSC",
            "token": "USDT (BEP20)",
            "token_contract": settings.payment_token_contract,
            "receiver_address": settings.payment_receiver_address,
        },
        "exchanges": ["binance", "bybit", "okx", "kucoin"],
    }


@router.get("/audit-log")
async def get_audit_log(
    limit: int = Query(default=100, ge=1, le=1000),
    db=Depends(get_database),
):
    """Obtiene log de auditoría (si existe colección)"""
    
    # Intentar obtener de la colección audit_log
    logs = await db.audit_log.find(
        {},
        sort=[("timestamp", -1)],
        limit=limit
    ).to_list(length=limit)
    
    return {
        "logs": [
            {
                "action": log.get("action"),
                "user_id": log.get("user_id"),
                "details": log.get("details"),
                "timestamp": log.get("timestamp", {}).isoformat() if log.get("timestamp") else None,
            }
            for log in logs
        ],
        "count": len(logs),
    }


__all__ = ["router"]
