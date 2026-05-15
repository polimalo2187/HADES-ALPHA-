# Placeholder para rutas de pagos
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_payments():
    return {"message": "Payments endpoint - implement soon"}

__all__ = ["router"]
