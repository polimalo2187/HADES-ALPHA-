# Placeholder para rutas de mercado
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_market():
    return {"message": "Market endpoint - implement soon"}

__all__ = ["router"]
