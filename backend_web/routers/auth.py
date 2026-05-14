from fastapi import APIRouter, Depends, HTTPException, status
from typing import Optional, Dict, Any
from config import (
    PhoneAuthRequest,
    PhoneVerifyRequest,
    SessionTokenResponse,
    UserRegisterRequest,
    UserLoginRequest,
)
from services.auth_service import create_access_token, verify_token
from services.phone_auth_service import phone_auth_service
from services.database_service import db_service
from services.user_web_service import get_or_create_user_by_phone


router = APIRouter(prefix="/api/auth", tags=["Authentication"])


@router.post("/send-code")
async def send_verification_code(request: PhoneAuthRequest):
    """Envía un código de verificación por SMS al número de teléfono."""
    result = phone_auth_service.send_verification_code(request.phone_number)
    
    if not result.get("ok"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result.get("error", "Error enviando código"),
        )
    
    return result


@router.post("/verify-code")
async def verify_code(request: PhoneVerifyRequest):
    """Verifica el código SMS y retorna un token de sesión."""
    # Verificar el código
    verification_result = phone_auth_service.verify_code(
        request.phone_number,
        request.verification_code
    )
    
    if not verification_result.get("ok"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=verification_result.get("error", "Código inválido"),
        )
    
    # Obtener o crear usuario
    user = get_or_create_user_by_phone(request.phone_number)
    
    # Crear token JWT
    session_token = create_access_token(
        user_id=user["user_id"],
        username=user.get("username"),
    )
    
    return {
        "ok": True,
        "session_token": session_token,
        "user": {
            "user_id": user["user_id"],
            "phone_number": user["phone_number"],
            "username": user.get("username"),
            "plan": user.get("plan", "free"),
            "subscription_status": user.get("subscription_status", "trial"),
        },
    }


@router.post("/register")
async def register_user(request: UserRegisterRequest):
    """Registra un nuevo usuario con número de teléfono."""
    # Verificar si ya existe
    existing_user = db_service.get_user_by_phone(request.phone_number)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El número de teléfono ya está registrado",
        )
    
    # Crear usuario
    user = get_or_create_user_by_phone(
        request.phone_number,
        request.username,
        request.referral_code,
    )
    
    session_token = create_access_token(
        user_id=user["user_id"],
        username=user.get("username"),
    )
    
    return {
        "ok": True,
        "session_token": session_token,
        "user": {
            "user_id": user["user_id"],
            "phone_number": user["phone_number"],
            "username": user.get("username"),
            "plan": user.get("plan", "free"),
            "subscription_status": user.get("subscription_status", "trial"),
        },
    }


@router.post("/login")
async def login_user(request: UserLoginRequest):
    """Inicia sesión con número de teléfono (envía código de verificación)."""
    user = db_service.get_user_by_phone(request.phone_number)
    
    if not user:
        # Si no existe, creamos uno nuevo
        user = get_or_create_user_by_phone(request.phone_number)
    
    # Enviar código de verificación
    result = phone_auth_service.send_verification_code(request.phone_number)
    
    if not result.get("ok"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result.get("error", "Error enviando código"),
        )
    
    return {
        "ok": True,
        "message": "Código de verificación enviado",
        "requires_verification": True,
    }
