import os
from dotenv import load_dotenv

load_dotenv()

# ===========================
# CONFIGURACIÓN PRINCIPAL
# ===========================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "hades_web")

# JWT Configuration
JWT_SECRET = os.getenv("JWT_SECRET", "your-super-secret-jwt-key-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

# Firebase Configuration (para Push Notifications)
FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH", "")
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "")

# Twilio Configuration (para SMS/Phone Auth)
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_SERVICE_SID = os.getenv("TWILIO_SERVICE_SID", "")

# CORS
CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5173,http://localhost:3000,https://hades-app.com"
).split(",")

# ===========================
# MODELOS PYDANTIC
# ===========================

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class TokenData(BaseModel):
    user_id: int
    exp: Optional[int] = None


class PhoneAuthRequest(BaseModel):
    phone_number: str = Field(..., description="Número de teléfono con código de país, ej: +5491123456789")


class PhoneVerifyRequest(BaseModel):
    phone_number: str
    verification_code: str


class SessionTokenResponse(BaseModel):
    ok: bool
    session_token: str
    user: Dict[str, Any]


class UserRegisterRequest(BaseModel):
    phone_number: str
    username: Optional[str] = None
    referral_code: Optional[str] = None


class UserLoginRequest(BaseModel):
    phone_number: str


class PushTokenRequest(BaseModel):
    push_token: str
    platform: str = Field(..., description="web, ios, android")


class SettingsUpdateRequest(BaseModel):
    language: Optional[str] = None
    push_alerts_enabled: Optional[bool] = None
    push_tiers: Optional[Dict[str, bool]] = None


class RiskProfileUpdateRequest(BaseModel):
    capital_usdt: Optional[float] = None
    risk_percent: Optional[float] = None
    exchange: Optional[str] = None
    entry_mode: Optional[str] = None
    fee_percent_per_side: Optional[float] = None
    slippage_percent: Optional[float] = None
    default_leverage: Optional[float] = None
    default_profile: Optional[str] = None


class WatchlistSymbolRequest(BaseModel):
    symbol: str


class WatchlistReplaceRequest(BaseModel):
    symbols: Optional[List[str]] = None
    raw: Optional[str] = None


class AdminManualPlanActivationRequest(BaseModel):
    user_id: int
    plan: str
    days: int


class AdminUserModerationRequest(BaseModel):
    user_id: int
    action: str
    duration_value: Optional[int] = None
    duration_unit: Optional[str] = None
    confirm: bool = False
