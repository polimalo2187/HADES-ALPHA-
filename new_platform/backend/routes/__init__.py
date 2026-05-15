"""
Rutas del backend - inicialización
"""

from . import auth
from . import users
from . import signals
from . import market
from . import payments
from . import admin
from . import websocket

__all__ = [
    "auth",
    "users",
    "signals",
    "market",
    "payments",
    "admin",
    "websocket",
]
