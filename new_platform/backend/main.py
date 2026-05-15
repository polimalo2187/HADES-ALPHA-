"""
Hades Platform - Backend Principal
Arquitectura Web + App sin dependencias de Telegram
Autenticación: Teléfono + Contraseña (sin verificación SMS)
Push Notifications: API interna vía WebSockets
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from backend.config import settings
from backend.database import initialize_database, close_database, get_database
from backend.routes import auth, users, signals, market, payments, admin, websocket
from backend.services import scanner_service, scheduler_service, register_default_tasks
from backend.observability import start_observability, stop_observability

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación"""
    # Startup
    logger.info("🚀 Iniciando Hades Platform...")
    
    await initialize_database()
    db = await get_database()
    logger.info("✅ Base de datos conectada")
    
    await start_observability()
    logger.info("✅ Observabilidad iniciada")
    
    # Registrar tareas por defecto en el scheduler
    from backend.routes.websocket import websocket_manager
    register_default_tasks(scheduler_service, db, websocket_manager)
    
    # Iniciar servicios
    await scheduler_service.start()
    logger.info("✅ Scheduler iniciado")
    
    scanner_service.set_database(db)
    await scanner_service.start()
    logger.info("✅ Scanner iniciado")
    
    yield
    
    # Shutdown
    logger.info("🛑 Deteniendo Hades Platform...")
    
    await scanner_service.stop()
    await scheduler_service.stop()
    await stop_observability()
    await close_database()
    
    logger.info("✅ Plataforma detenida correctamente")


app = FastAPI(
    title="HADES Platform API",
    description="API para Web y App - Trading Signals Platform",
    version="2.0.0",
    lifespan=lifespan
)

# Middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rutas
app.include_router(auth.router, prefix="/api/auth", tags=["Autenticación"])
app.include_router(users.router, prefix="/api/users", tags=["Usuarios"])
app.include_router(signals.router, prefix="/api/signals", tags=["Señales"])
app.include_router(market.router, prefix="/api/market", tags=["Mercado"])
app.include_router(payments.router, prefix="/api/payments", tags=["Pagos"])
app.include_router(admin.router, prefix="/api/admin", tags=["Administración"])
app.include_router(websocket.router, prefix="/api/ws", tags=["WebSockets"])


@app.get("/health")
async def health():
    """Endpoint de salud básico"""
    return {"ok": True, "service": "hades-platform", "version": "2.0.0"}


@app.get("/health/live")
async def live():
    """Live probe para Kubernetes/Docker"""
    return {"ok": True}


@app.get("/health/ready")
async def ready():
    """Ready probe - verifica dependencias"""
    from backend.database import check_database_connection
    db_ok = await check_database_connection()
    
    return {
        "ok": db_ok,
        "database": "connected" if db_ok else "disconnected"
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        workers=4
    )
