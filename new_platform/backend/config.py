"""
Configuración centralizada de la plataforma
Carga variables de entorno y valida configuración
"""

import os
from typing import List
from urllib.parse import urlparse
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Configuración principal de la aplicación"""
    
    # ==============================
    # BASE
    # ==============================
    ENVIRONMENT: str = Field(default="production", env="ENVIRONMENT")
    MONGODB_URI: str = Field(env="MONGODB_URI")
    DATABASE_NAME: str = Field(default="hades_db", env="DATABASE_NAME")
    
    # ==============================
    # SERVER
    # ==============================
    APP_RUNTIME_ROLE: str = Field(default="web", env="APP_RUNTIME_ROLE")
    PORT: int = Field(default=8000, env="PORT")
    HOST: str = Field(default="0.0.0.0", env="HOST")
    
    # ==============================
    # AUTENTICACIÓN
    # ==============================
    AUTH_SESSION_SECRET: str = Field(env="AUTH_SESSION_SECRET")
    AUTH_SESSION_TTL_SECONDS: int = Field(default=43200, env="AUTH_SESSION_TTL_SECONDS")
    AUTH_PASSWORD_SALT_ROUNDS: int = Field(default=12, env="AUTH_PASSWORD_SALT_ROUNDS")
    AUTH_TOKEN_TTL_SECONDS: int = Field(default=2592000, env="AUTH_TOKEN_TTL_SECONDS")
    
    # ==============================
    # CORS
    # ==============================
    CORS_ORIGINS_RAW: str = Field(default="", env="CORS_ORIGINS")
    
    # ==============================
    # PUSH NOTIFICATIONS
    # ==============================
    PUSH_ENABLED: bool = Field(default=True, env="PUSH_ENABLED")
    PUSH_MAX_CONNECTIONS_PER_USER: int = Field(default=5, env="PUSH_MAX_CONNECTIONS_PER_USER")
    PUSH_HEARTBEAT_INTERVAL_SECONDS: int = Field(default=30, env="PUSH_HEARTBEAT_INTERVAL_SECONDS")
    PUSH_RECONNECT_DELAY_MS: int = Field(default=3000, env="PUSH_RECONNECT_DELAY_MS")
    
    # ==============================
    # PAGOS BEP-20
    # ==============================
    PAYMENT_NETWORK: str = Field(default="bep20", env="PAYMENT_NETWORK")
    PAYMENT_TOKEN_SYMBOL: str = Field(default="USDT", env="PAYMENT_TOKEN_SYMBOL")
    PAYMENT_TOKEN_CONTRACT: str = Field(env="PAYMENT_TOKEN_CONTRACT")
    PAYMENT_RECEIVER_ADDRESS: str = Field(env="PAYMENT_RECEIVER_ADDRESS")
    BSC_RPC_HTTP_URL: str = Field(env="BSC_RPC_HTTP_URL")
    PAYMENT_MIN_CONFIRMATIONS: int = Field(default=3, env="PAYMENT_MIN_CONFIRMATIONS")
    PAYMENT_ORDER_TTL_MINUTES: int = Field(default=30, env="PAYMENT_ORDER_TTL_MINUTES")
    PAYMENT_UNIQUE_MAX_DELTA: float = Field(default=0.150, env="PAYMENT_UNIQUE_MAX_DELTA")
    PAYMENT_TOKEN_DECIMALS: int = Field(default=18, env="PAYMENT_TOKEN_DECIMALS")
    PAYMENT_LOOKBACK_BLOCKS: int = Field(default=2500, env="PAYMENT_LOOKBACK_BLOCKS")
    
    # ==============================
    # SCANNER
    # ==============================
    SCANNER_KLINE_LIMIT_5M: int = Field(default=260, env="SCANNER_KLINE_LIMIT_5M")
    SCANNER_KLINE_LIMIT_15M: int = Field(default=260, env="SCANNER_KLINE_LIMIT_15M")
    SCANNER_KLINE_LIMIT_1H: int = Field(default=260, env="SCANNER_KLINE_LIMIT_1H")
    SCANNER_SYMBOL_CONCURRENCY: int = Field(default=24, env="SCANNER_SYMBOL_CONCURRENCY")
    SCANNER_MAX_REQUESTS_PER_SECOND: int = Field(default=8, env="SCANNER_MAX_REQUESTS_PER_SECOND")
    SCANNER_MAX_BURST: int = Field(default=16, env="SCANNER_MAX_BURST")
    REQUEST_MAX_RETRIES: int = Field(default=4, env="REQUEST_MAX_RETRIES")
    REQUEST_RETRY_BASE_SLEEP: float = Field(default=0.6, env="REQUEST_RETRY_BASE_SLEEP")
    SCANNER_ENABLE_HTF_CACHE: bool = Field(default=True, env="SCANNER_ENABLE_HTF_CACHE")
    SCANNER_5M_CACHE_SECONDS: int = Field(default=0, env="SCANNER_5M_CACHE_SECONDS")
    SCANNER_HTF_STALE_GRACE_SECONDS: int = Field(default=900, env="SCANNER_HTF_STALE_GRACE_SECONDS")
    ACTIVE_SYMBOLS_CACHE_SECONDS: int = Field(default=300, env="ACTIVE_SYMBOLS_CACHE_SECONDS")
    SCANNER_BOOTSTRAP_BATCH_SIZE: int = Field(default=48, env="SCANNER_BOOTSTRAP_BATCH_SIZE")
    SCANNER_15M_REFRESH_BATCH_SIZE: int = Field(default=20, env="SCANNER_15M_REFRESH_BATCH_SIZE")
    SCANNER_1H_REFRESH_BATCH_SIZE: int = Field(default=8, env="SCANNER_1H_REFRESH_BATCH_SIZE")
    
    # ==============================
    # SCORE THRESHOLDS
    # ==============================
    PREMIUM_RAW_SCORE_MIN: int = Field(default=79, env="PREMIUM_RAW_SCORE_MIN")
    PLUS_RAW_SCORE_MIN: int = Field(default=74, env="PLUS_RAW_SCORE_MIN")
    FREE_RAW_SCORE_MIN: int = Field(default=66, env="FREE_RAW_SCORE_MIN")
    
    # ==============================
    # BREAKOUT FILTERS
    # ==============================
    FREE_ADX_MIN: float = Field(default=15.8, env="FREE_ADX_MIN")
    PLUS_ADX_MIN: float = Field(default=17.1, env="PLUS_ADX_MIN")
    PREMIUM_ADX_MIN: float = Field(default=17.8, env="PREMIUM_ADX_MIN")
    FREE_ATR_PCT_MIN: float = Field(default=0.0020, env="FREE_ATR_PCT_MIN")
    PLUS_ATR_PCT_MIN: float = Field(default=0.0023, env="PLUS_ATR_PCT_MIN")
    PREMIUM_ATR_PCT_MIN: float = Field(default=0.0025, env="PREMIUM_ATR_PCT_MIN")
    FREE_ATR_PCT_MAX: float = Field(default=0.0142, env="FREE_ATR_PCT_MAX")
    PLUS_ATR_PCT_MAX: float = Field(default=0.0128, env="PLUS_ATR_PCT_MAX")
    PREMIUM_ATR_PCT_MAX: float = Field(default=0.0122, env="PREMIUM_ATR_PCT_MAX")
    
    # ==============================
    # RETENCION
    # ==============================
    BASE_SIGNALS_RETENTION_DAYS: int = Field(default=180, env="BASE_SIGNALS_RETENTION_DAYS")
    USER_SIGNALS_RETENTION_DAYS: int = Field(default=45, env="USER_SIGNALS_RETENTION_DAYS")
    SIGNAL_RESULTS_RETENTION_DAYS: int = Field(default=365, env="SIGNAL_RESULTS_RETENTION_DAYS")
    SIGNAL_HISTORY_RETENTION_DAYS: int = Field(default=730, env="SIGNAL_HISTORY_RETENTION_DAYS")
    
    # ==============================
    # SCHEDULER
    # ==============================
    SCHEDULER_CHECK_INTERVAL: int = Field(default=300, env="SCHEDULER_CHECK_INTERVAL")
    SCHEDULER_BATCH_SIZE: int = Field(default=100, env="SCHEDULER_BATCH_SIZE")
    SCHEDULER_EVALUATION_LIMIT: int = Field(default=200, env="SCHEDULER_EVALUATION_LIMIT")
    
    # ==============================
    # ADMIN
    # ==============================
    ADMIN_USER_IDS_RAW: str = Field(default="", env="ADMIN_USER_IDS")
    ADMIN_WHATSAPPS_RAW: str = Field(default="", env="ADMIN_WHATSAPPS")
    
    # ==============================
    # BINANCE (OPCIONAL)
    # ==============================
    BINANCE_API_KEY: str = Field(default="", env="BINANCE_API_KEY")
    BINANCE_API_SECRET: str = Field(default="", env="BINANCE_API_SECRET")
    
    class Config:
        env_file = ".env"
        case_sensitive = True
    
    @property
    def CORS_ORIGINS(self) -> List[str]:
        """Procesa los orígenes CORS desde la variable de entorno"""
        if not self.CORS_ORIGINS_RAW:
            return []
        
        origins = []
        for item in self.CORS_ORIGINS_RAW.split(","):
            item = item.strip()
            if not item:
                continue
            parsed = urlparse(item)
            if parsed.scheme and parsed.netloc:
                origins.append(f"{parsed.scheme}://{parsed.netloc}")
            elif item == "*":
                return ["*"]
        return origins
    
    @property
    def ADMIN_USER_IDS(self) -> List[int]:
        """Procesa los IDs de administradores"""
        if not self.ADMIN_USER_IDS_RAW:
            return []
        
        ids = []
        for item in self.ADMIN_USER_IDS_RAW.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                admin_id = int(item)
                if admin_id > 0:
                    ids.append(admin_id)
            except ValueError:
                continue
        return ids
    
    @property
    def ADMIN_WHATSAPPS(self) -> List[str]:
        """Procesa los WhatsApps de administración"""
        if not self.ADMIN_WHATSAPPS_RAW:
            return []
        
        return [w.strip() for w in self.ADMIN_WHATSAPPS_RAW.split(",") if w.strip()]
    
    @property
    def is_production(self) -> bool:
        """Verifica si está en producción"""
        return self.ENVIRONMENT.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Verifica si está en desarrollo"""
        return self.ENVIRONMENT.lower() in {"dev", "development", "local", "test", "testing", "staging"}


# Instancia global de configuración
settings = Settings()


def validate_configuration() -> None:
    """Valida que la configuración sea correcta"""
    errors = []
    
    # Requeridos siempre
    if not settings.MONGODB_URI:
        errors.append("MONGODB_URI es requerida")
    if not settings.DATABASE_NAME:
        errors.append("DATABASE_NAME es requerida")
    
    # Requeridos para autenticación
    if not settings.AUTH_SESSION_SECRET:
        errors.append("AUTH_SESSION_SECRET es requerida")
    if len(settings.AUTH_SESSION_SECRET) < 32:
        errors.append("AUTH_SESSION_SECRET debe tener al menos 32 caracteres")
    
    # Requeridos para pagos
    if not settings.PAYMENT_TOKEN_CONTRACT:
        errors.append("PAYMENT_TOKEN_CONTRACT es requerida")
    if not settings.PAYMENT_RECEIVER_ADDRESS:
        errors.append("PAYMENT_RECEIVER_ADDRESS es requerida")
    if not settings.BSC_RPC_HTTP_URL:
        errors.append("BSC_RPC_HTTP_URL es requerida")
    
    if errors:
        raise RuntimeError("Errores de configuración:\n" + "\n".join(f"  - {e}" for e in errors))


__all__ = ["settings", "validate_configuration"]
