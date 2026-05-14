from pymongo import MongoClient
from typing import Optional, Dict, Any
from datetime import datetime
from config import MONGODB_URI, MONGODB_DB


class DatabaseService:
    """Servicio de conexión a MongoDB."""
    
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DB]
        
        # Colecciones principales
        self.users = self.db["users"]
        self.user_signals = self.db["user_signals"]
        self.watchlists = self.db["watchlists"]
        self.payment_orders = self.db["payment_orders"]
        self.subscription_events = self.db["subscription_events"]
        self.referrals = self.db["referrals"]
        self.push_tokens = self.db["push_tokens"]
        
        # Índices
        self._create_indexes()
    
    def _create_indexes(self):
        """Crea índices para optimizar consultas."""
        try:
            self.users.create_index("user_id", unique=True)
            self.users.create_index("phone_number", unique=True)
            self.user_signals.create_index("user_id")
            self.user_signals.create_index("signal_id")
            self.watchlists.create_index("user_id", unique=True)
            self.payment_orders.create_index("user_id")
            self.payment_orders.create_index("order_id", unique=True)
            self.push_tokens.create_index("user_id")
            self.push_tokens.create_index("token", unique=True)
        except Exception as e:
            print(f"Error creando índices: {e}")
    
    def get_user_by_phone(self, phone_number: str) -> Optional[Dict[str, Any]]:
        """Obtiene un usuario por número de teléfono."""
        return self.users.find_one({"phone_number": phone_number})
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Obtiene un usuario por ID."""
        return self.users.find_one({"user_id": user_id})
    
    def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Crea un nuevo usuario."""
        user_data["created_at"] = datetime.utcnow()
        user_data["updated_at"] = datetime.utcnow()
        result = self.users.insert_one(user_data)
        user_data["_id"] = result.inserted_id
        return user_data
    
    def update_user(self, user_id: int, update_data: Dict[str, Any]) -> bool:
        """Actualiza un usuario."""
        update_data["updated_at"] = datetime.utcnow()
        result = self.users.update_one(
            {"user_id": user_id},
            {"$set": update_data}
        )
        return result.modified_count > 0
    
    def save_push_token(self, user_id: int, token: str, platform: str) -> bool:
        """Guarda o actualiza un token push."""
        result = self.push_tokens.update_one(
            {"user_id": user_id, "platform": platform},
            {
                "$set": {
                    "token": token,
                    "platform": platform,
                    "updated_at": datetime.utcnow(),
                }
            },
            upsert=True
        )
        return True
    
    def get_push_tokens_for_user(self, user_id: int) -> list:
        """Obtiene todos los tokens push de un usuario."""
        tokens = self.push_tokens.find({"user_id": user_id})
        return [t["token"] for t in tokens]
    
    def delete_push_token(self, token: str) -> bool:
        """Elimina un token push."""
        result = self.push_tokens.delete_one({"token": token})
        return result.deleted_count > 0


# Instancia global
db_service = DatabaseService()
