# Placeholder para rutas de admin
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_admin():
    return {"message": "Admin endpoint - implement soon"}

__all__ = ["router"]
