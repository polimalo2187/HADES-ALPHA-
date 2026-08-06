from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import jwt
from config import JWT_SECRET, JWT_ALGORITHM, JWT_ACCESS_TOKEN_EXPIRE_MINUTES


def create_access_token(user_id: int, username: Optional[str] = None, expires_delta: Optional[timedelta] = None) -> str:
    """Crea un token JWT para el usuario."""
    now = datetime.utcnow()
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode = {
        "user_id": user_id,
        "username": username or "",
        "exp": expire,
        "iat": now,
    }
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[Dict[str, Any]]:
    """Decodifica y valida un token JWT."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def verify_token(token: str) -> Optional[int]:
    """Verifica el token y retorna el user_id si es válido."""
    payload = decode_access_token(token)
    if payload:
        return payload.get("user_id")
    return None
