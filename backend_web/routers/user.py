from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, Any
from config import PushTokenRequest, SettingsUpdateRequest
from services.auth_service import verify_token
from services.database_service import db_service
from services.user_web_service import get_user_by_id


router = APIRouter(prefix="/api/user", tags=["User"])


async def get_current_user(authorization: str) -> Dict[str, Any]:
    """Dependencia para obtener el usuario actual."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
        )
    
    token = authorization.split(" ")[1]
    user_id = verify_token(token)
    
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado o inválido",
        )
    
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado",
        )
    
    return user


@router.get("/me")
async def get_me(user: Dict[str, Any] = Depends(get_current_user)):
    """Obtiene la información del usuario actual."""
    return {
        "ok": True,
        "user": {
            "user_id": user["user_id"],
            "phone_number": user["phone_number"],
            "username": user.get("username"),
            "plan": user.get("plan", "free"),
            "subscription_status": user.get("subscription_status", "trial"),
            "language": user.get("language", "es"),
            "trial_end": user.get("trial_end").isoformat() if user.get("trial_end") else None,
            "plan_end": user.get("plan_end").isoformat() if user.get("plan_end") else None,
        },
    }


@router.post("/push-token")
async def save_push_token(
    request: PushTokenRequest,
    user: Dict[str, Any] = Depends(get_current_user),
):
    """Guarda el token push del usuario."""
    success = db_service.save_push_token(
        user_id=user["user_id"],
        token=request.push_token,
        platform=request.platform,
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error guardando token push",
        )
    
    return {"ok": True, "message": "Token push guardado"}


@router.delete("/push-token/{token}")
async def delete_push_token(
    token: str,
    user: Dict[str, Any] = Depends(get_current_user),
):
    """Elimina un token push."""
    success = db_service.delete_push_token(token)
    
    return {"ok": success, "message": "Token eliminado" if success else "Token no encontrado"}


@router.put("/settings")
async def update_settings(
    request: SettingsUpdateRequest,
    user: Dict[str, Any] = Depends(get_current_user),
):
    """Actualiza la configuración del usuario."""
    update_data = {}
    
    if request.language is not None:
        update_data["language"] = request.language
    
    if request.push_alerts_enabled is not None:
        update_data["miniapp_settings.push_alerts.enabled"] = request.push_alerts_enabled
    
    if request.push_tiers is not None:
        for tier in ["free", "plus", "premium"]:
            if tier in request.push_tiers:
                update_data[f"miniapp_settings.push_alerts.tiers.{tier}"] = request.push_tiers[tier]
    
    if update_data:
        success = db_service.update_user(user["user_id"], update_data)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error actualizando configuración",
            )
    
    # Retornar usuario actualizado
    updated_user = get_user_by_id(user["user_id"])
    return {
        "ok": True,
        "user": {
            "user_id": updated_user["user_id"],
            "phone_number": updated_user["phone_number"],
            "username": updated_user.get("username"),
            "plan": updated_user.get("plan", "free"),
            "subscription_status": updated_user.get("subscription_status", "trial"),
            "language": updated_user.get("language", "es"),
            "miniapp_settings": updated_user.get("miniapp_settings", {}),
        },
    }
