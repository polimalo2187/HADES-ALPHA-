from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import CORS_ORIGINS

# Importar routers
from routers import auth, user


def create_app() -> FastAPI:
    """Crea la aplicación FastAPI."""
    app = FastAPI(
        title="HADES Web API",
        description="API para la plataforma web y móvil HADES",
        version="2.0.0",
    )
    
    # Configurar CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Incluir routers
    app.include_router(auth.router)
    app.include_router(user.router)
    
    @app.get("/")
    async def root():
        return {
            "ok": True,
            "message": "HADES Web API v2.0.0",
            "endpoints": {
                "auth": "/api/auth",
                "user": "/api/user",
            },
        }
    
    @app.get("/health")
    async def health():
        return {
            "status": "healthy",
            "service": "hades-web-api",
            "version": "2.0.0",
        }
    
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
