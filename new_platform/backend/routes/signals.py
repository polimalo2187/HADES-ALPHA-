# Placeholder para rutas de señales
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_signals():
    return {"message": "Signals endpoint - implement soon"}

__all__ = ["router"]
