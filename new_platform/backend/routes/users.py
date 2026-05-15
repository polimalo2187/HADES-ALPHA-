# Placeholder para rutas adicionales
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_users():
    return {"message": "Users endpoint - implement soon"}

__all__ = ["router"]
