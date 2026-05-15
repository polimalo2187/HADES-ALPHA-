"""
Rutas de autenticación
Login y registro con teléfono + contraseña (sin verificación SMS)
"""

from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, validator
import re
import time
import jwt

from backend.config import settings
from backend.database import get_users_collection
from backend.models import new_user, hash_password, verify_password, is_plan_active, is_trial_active

router = APIRouter()
security = HTTPBearer(auto_error=False)


# =========================
# MODELOS DE REQUEST
# =========================
class RegisterRequest(BaseModel):
    phone: str = Field(..., description="Número de teléfono")
    password: str = Field(..., min_length=6, description="Contraseña (mínimo 6 caracteres)")
    username: str | None = Field(None, description="Nombre de usuario opcional")
    language: str = Field(default="es", description="Idioma preferido")
    
    @validator('phone')
    def validate_phone(cls, v):
        # Eliminar espacios y caracteres especiales
        cleaned = re.sub(r'[\s\-\(\)\+]', '', v)
        
        # Verificar que solo contiene dígitos
        if not cleaned.isdigit():
            raise ValueError("El teléfono solo debe contener dígitos")
        
        # Verificar longitud (mínimo 7, máximo 15 dígitos)
        if len(cleaned) < 7 or len(cleaned) > 15:
            raise ValueError("El teléfono debe tener entre 7 y 15 dígitos")
        
        return cleaned
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 6:
            raise ValueError("La contraseña debe tener al menos 6 caracteres")
        return v


class LoginRequest(BaseModel):
    phone: str = Field(..., description="Número de teléfono")
    password: str = Field(..., description="Contraseña")
    
    @validator('phone')
    def validate_phone(cls, v):
        cleaned = re.sub(r'[\s\-\(\)\+]', '', v)
        if not cleaned.isdigit():
            raise ValueError("El teléfono solo debe contener dígitos")
        return cleaned


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user_id: int
    phone: str
    username: str | None
    plan: str
    subscription_status: str


class MeResponse(BaseModel):
    user_id: int
    phone: str
    username: str | None
    plan: str
    subscription_status: str
    trial_end: str | None
    plan_end: str | None
    language: str
    push_enabled: bool
    created_at: str
    last_activity: str


# =========================
# FUNCIONES AUXILIARES
# =========================
def create_access_token(user_id: int, phone: str) -> str:
    """Crea un token JWT para el usuario"""
    now = int(time.time())
    payload = {
        "user_id": user_id,
        "phone": phone,
        "iat": now,
        "exp": now + settings.AUTH_TOKEN_TTL_SECONDS,
    }
    return jwt.encode(payload, settings.AUTH_SESSION_SECRET, algorithm="HS256")


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security)
) -> dict:
    """Obtiene el usuario actual desde el token JWT"""
    if not credentials:
        raise HTTPException(status_code=401, detail="Token no proporcionado")
    
    try:
        token = credentials.credentials
        payload = jwt.decode(token, settings.AUTH_SESSION_SECRET, algorithms=["HS256"])
        user_id = payload.get("user_id")
        
        if not user_id:
            raise HTTPException(status_code=401, detail="Token inválido")
        
        users_col = get_users_collection()
        user = await users_col.find_one({"user_id": user_id})
        
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        # Actualizar última actividad
        await users_col.update_one(
            {"user_id": user_id},
            {"$set": {"last_activity": __import__('datetime').datetime.utcnow()}}
        )
        
        return user
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")


# =========================
# ENDPOINTS
# =========================
@router.post("/register", response_model=TokenResponse)
async def register(request: RegisterRequest):
    """
    Registro de nuevo usuario con teléfono y contraseña.
    Sin verificación SMS - registro directo.
    """
    users_col = get_users_collection()
    
    # Verificar si el teléfono ya existe
    existing_user = await users_col.find_one({"phone": request.phone})
    if existing_user:
        raise HTTPException(status_code=400, detail="Este teléfono ya está registrado")
    
    # Hashear contraseña
    password_hash = hash_password(request.password, settings.AUTH_PASSWORD_SALT_ROUNDS)
    
    # Crear usuario
    user_doc = new_user(
        phone=request.phone,
        password_hash=password_hash,
        username=request.username,
        language=request.language,
    )
    
    # Insertar en base de datos
    result = await users_col.insert_one(user_doc)
    if not result.inserted_id:
        raise HTTPException(status_code=500, detail="Error creando usuario")
    
    # Crear token de acceso
    access_token = create_access_token(user_doc["user_id"], user_doc["phone"])
    
    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=settings.AUTH_TOKEN_TTL_SECONDS,
        user_id=user_doc["user_id"],
        phone=user_doc["phone"],
        username=user_doc.get("username"),
        plan=user_doc["plan"],
        subscription_status=user_doc["subscription_status"],
    )


@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest):
    """
    Login con teléfono y contraseña.
    Sin verificación SMS - login directo.
    """
    users_col = get_users_collection()
    
    # Buscar usuario por teléfono
    user = await users_col.find_one({"phone": request.phone})
    if not user:
        raise HTTPException(status_code=401, detail="Teléfono o contraseña incorrectos")
    
    # Verificar contraseña
    if not verify_password(request.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Teléfono o contraseña incorrectos")
    
    # Verificar si el usuario está baneado
    if user.get("banned"):
        raise HTTPException(status_code=403, detail="Usuario suspendido")
    
    # Crear token de acceso
    access_token = create_access_token(user["user_id"], user["phone"])
    
    # Actualizar última actividad
    await users_col.update_one(
        {"user_id": user["user_id"]},
        {"$set": {"last_activity": __import__('datetime').datetime.utcnow()}}
    )
    
    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=settings.AUTH_TOKEN_TTL_SECONDS,
        user_id=user["user_id"],
        phone=user["phone"],
        username=user.get("username"),
        plan=user["plan"],
        subscription_status=user["subscription_status"],
    )


@router.get("/me", response_model=MeResponse)
async def get_me(current_user: dict = Depends(get_current_user)):
    """Obtiene información del usuario autenticado"""
    return MeResponse(
        user_id=current_user["user_id"],
        phone=current_user["phone"],
        username=current_user.get("username"),
        plan=current_user["plan"],
        subscription_status=current_user["subscription_status"],
        trial_end=current_user.get("trial_end", {}).isoformat() if current_user.get("trial_end") else None,
        plan_end=current_user.get("plan_end", {}).isoformat() if current_user.get("plan_end") else None,
        language=current_user.get("language", "es"),
        push_enabled=current_user.get("push_settings", {}).get("enabled", True),
        created_at=current_user["created_at"].isoformat(),
        last_activity=current_user["last_activity"].isoformat(),
    )


@router.post("/refresh")
async def refresh_token(current_user: dict = Depends(get_current_user)):
    """Refresca el token de acceso"""
    access_token = create_access_token(current_user["user_id"], current_user["phone"])
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": settings.AUTH_TOKEN_TTL_SECONDS,
    }


__all__ = ["router"]
