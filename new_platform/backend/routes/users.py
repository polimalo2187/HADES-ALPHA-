"""
Rutas de gestión de usuarios
CRUD completo para administración de usuarios
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
from pydantic import BaseModel, Field

from backend.routes.auth import get_current_user
from backend.database import get_users_collection
from backend.models import update_timestamp, is_plan_active, activate_plan

router = APIRouter()


# =========================
# MODELOS
# =========================
class UserUpdateRequest(BaseModel):
    username: Optional[str] = Field(None, min_length=3, max_length=50)
    language: Optional[str] = Field(None, pattern="^(es|en|pt|fr|de|ru|zh|ja|ko)$")
    push_enabled: Optional[bool] = None


class UserResponse(BaseModel):
    user_id: int
    phone: str
    username: Optional[str]
    plan: str
    subscription_status: str
    trial_end: Optional[str]
    plan_end: Optional[str]
    language: str
    push_enabled: bool
    created_at: str
    last_activity: str
    is_trial_active: bool
    is_plan_active: bool


class UserListResponse(BaseModel):
    users: List[UserResponse]
    total: int
    page: int
    page_size: int


# =========================
# ENDPOINTS PÚBLICOS
# =========================
@router.put("/me", response_model=UserResponse)
async def update_profile(
    request: UserUpdateRequest,
    current_user: dict = Depends(get_current_user)
):
    """Actualiza el perfil del usuario autenticado"""
    users_col = get_users_collection()
    
    update_data = {}
    
    if request.username is not None:
        # Verificar que el username no esté en uso
        existing = await users_col.find_one({
            "username": request.username,
            "user_id": {"$ne": current_user["user_id"]}
        })
        if existing:
            raise HTTPException(status_code=400, detail="Username ya está en uso")
        update_data["username"] = request.username
    
    if request.language is not None:
        update_data["language"] = request.language
    
    if request.push_enabled is not None:
        update_data["push_settings.enabled"] = request.push_enabled
    
    if not update_data:
        return UserResponse(
            user_id=current_user["user_id"],
            phone=current_user["phone"],
            username=current_user.get("username"),
            plan=current_user["plan"],
            subscription_status=current_user["subscription_status"],
            trial_end=current_user.get("trial_end").isoformat() if current_user.get("trial_end") else None,
            plan_end=current_user.get("plan_end").isoformat() if current_user.get("plan_end") else None,
            language=current_user.get("language", "es"),
            push_enabled=current_user.get("push_settings", {}).get("enabled", True),
            created_at=current_user["created_at"].isoformat(),
            last_activity=current_user["last_activity"].isoformat(),
            is_trial_active=is_plan_active(current_user),
            is_plan_active=is_plan_active(current_user),
        )
    
    # Actualizar usuario
    result = await users_col.update_one(
        {"user_id": current_user["user_id"]},
        {"$set": {**update_data, **{"updated_at": __import__('datetime').datetime.utcnow()}}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=500, detail="Error actualizando perfil")
    
    # Obtener usuario actualizado
    updated_user = await users_col.find_one({"user_id": current_user["user_id"]})
    
    return UserResponse(
        user_id=updated_user["user_id"],
        phone=updated_user["phone"],
        username=updated_user.get("username"),
        plan=updated_user["plan"],
        subscription_status=updated_user["subscription_status"],
        trial_end=updated_user.get("trial_end").isoformat() if updated_user.get("trial_end") else None,
        plan_end=updated_user.get("plan_end").isoformat() if updated_user.get("plan_end") else None,
        language=updated_user.get("language", "es"),
        push_enabled=updated_user.get("push_settings", {}).get("enabled", True),
        created_at=updated_user["created_at"].isoformat(),
        last_activity=updated_user["last_activity"].isoformat(),
        is_trial_active=is_plan_active(updated_user),
        is_plan_active=is_plan_active(updated_user),
    )


@router.delete("/me")
async def delete_account(current_user: dict = Depends(get_current_user)):
    """Elimina la cuenta del usuario (soft delete)"""
    users_col = get_users_collection()
    
    # Soft delete - marcar como eliminado
    await users_col.update_one(
        {"user_id": current_user["user_id"]},
        {
            "$set": {
                "deleted": True,
                "deleted_at": __import__('datetime').datetime.utcnow(),
                "phone": f"deleted_{current_user['user_id']}",  # Anonimizar teléfono
                "username": None,
                "banned": True,
                "updated_at": __import__('datetime').datetime.utcnow()
            }
        }
    )
    
    return {"message": "Cuenta eliminada exitosamente"}


# =========================
# ENDPOINTS DE ADMINISTRACIÓN
# =========================
@router.get("/", response_model=UserListResponse)
async def list_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    plan: Optional[str] = Query(None),
    subscription_status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Lista usuarios con paginación y filtros (solo admin)"""
    # Verificar si es admin
    if current_user.get("plan") != "premium":
        raise HTTPException(status_code=403, detail="Se requiere plan Premium")
    
    users_col = get_users_collection()
    
    # Construir filtro
    query_filter = {"deleted": {"$ne": True}}
    
    if plan:
        query_filter["plan"] = plan
    
    if subscription_status:
        query_filter["subscription_status"] = subscription_status
    
    if search:
        query_filter["$or"] = [
            {"phone": {"$regex": search, "$options": "i"}},
            {"username": {"$regex": search, "$options": "i"}}
        ]
    
    # Calcular offset
    skip = (page - 1) * page_size
    
    # Obtener usuarios
    users_cursor = users_col.find(query_filter).skip(skip).limit(page_size).sort("created_at", -1)
    users_list = await users_cursor.to_list(length=page_size)
    
    # Contar total
    total = await users_col.count_documents(query_filter)
    
    # Formatear respuesta
    users = []
    for user in users_list:
        users.append(UserResponse(
            user_id=user["user_id"],
            phone=user["phone"],
            username=user.get("username"),
            plan=user["plan"],
            subscription_status=user["subscription_status"],
            trial_end=user.get("trial_end").isoformat() if user.get("trial_end") else None,
            plan_end=user.get("plan_end").isoformat() if user.get("plan_end") else None,
            language=user.get("language", "es"),
            push_enabled=user.get("push_settings", {}).get("enabled", True),
            created_at=user["created_at"].isoformat(),
            last_activity=user["last_activity"].isoformat(),
            is_trial_active=is_plan_active(user),
            is_plan_active=is_plan_active(user),
        ))
    
    return UserListResponse(
        users=users,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{user_id}")
async def get_user(
    user_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Obtiene detalles de un usuario específico (solo admin)"""
    if current_user.get("plan") != "premium":
        raise HTTPException(status_code=403, detail="Se requiere plan Premium")
    
    users_col = get_users_collection()
    user = await users_col.find_one({"user_id": user_id, "deleted": {"$ne": True}})
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    return user


@router.post("/{user_id}/activate-plan")
async def activate_user_plan(
    user_id: int,
    plan: str = Query(..., pattern="^(free|plus|premium)$"),
    days: int = Query(30, ge=1, le=365),
    current_user: dict = Depends(get_current_user)
):
    """Activa un plan para un usuario (solo admin)"""
    if current_user.get("plan") != "premium":
        raise HTTPException(status_code=403, detail="Se requiere plan Premium")
    
    users_col = get_users_collection()
    user = await users_col.find_one({"user_id": user_id, "deleted": {"$ne": True}})
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # Activar plan
    updated_user = activate_plan(user, plan, days)
    
    # Guardar en BD
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": updated_user}
    )
    
    return {"message": f"Plan {plan} activado por {days} días", "user": updated_user}


@router.post("/{user_id}/ban")
async def ban_user(
    user_id: int,
    reason: str = Query(..., min_length=10),
    current_user: dict = Depends(get_current_user)
):
    """Banear un usuario (solo admin)"""
    if current_user.get("plan") != "premium":
        raise HTTPException(status_code=403, detail="Se requiere plan Premium")
    
    users_col = get_users_collection()
    user = await users_col.find_one({"user_id": user_id, "deleted": {"$ne": True}})
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    await users_col.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "banned": True,
                "ban_reason": reason,
                "banned_at": __import__('datetime').datetime.utcnow(),
                "banned_by": current_user["user_id"],
                "updated_at": __import__('datetime').datetime.utcnow()
            }
        }
    )
    
    return {"message": "Usuario baneado exitosamente"}


@router.post("/{user_id}/unban")
async def unban_user(
    user_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Desbanear un usuario (solo admin)"""
    if current_user.get("plan") != "premium":
        raise HTTPException(status_code=403, detail="Se requiere plan Premium")
    
    users_col = get_users_collection()
    user = await users_col.find_one({"user_id": user_id, "deleted": {"$ne": True}})
    
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    await users_col.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "banned": False,
                "ban_reason": None,
                "banned_at": None,
                "banned_by": None,
                "updated_at": __import__('datetime').datetime.utcnow()
            }
        }
    )
    
    return {"message": "Usuario desbaneado exitosamente"}


__all__ = ["router"]
