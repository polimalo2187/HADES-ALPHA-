import firebase_admin
from firebase_admin import credentials, messaging
from typing import Optional, Dict, Any
from config import FIREBASE_CREDENTIALS_PATH, FIREBASE_PROJECT_ID


class PushNotificationService:
    """Servicio de notificaciones push usando Firebase Cloud Messaging."""
    
    def __init__(self):
        self.initialized = False
        if FIREBASE_CREDENTIALS_PATH and not firebase_admin._apps:
            try:
                cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
                firebase_admin.initialize_app(cred, {
                    'projectId': FIREBASE_PROJECT_ID,
                })
                self.initialized = True
            except Exception as e:
                print(f"Error inicializando Firebase: {e}")
    
    def send_push_notification(
        self,
        token: str,
        title: str,
        body: str,
        data: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Envía una notificación push a un dispositivo."""
        if not self.initialized:
            return {
                "ok": False,
                "error": "Firebase no inicializado",
            }
        
        try:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body,
                ),
                data=data or {},
                token=token,
            )
            
            response = messaging.send(message)
            return {
                "ok": True,
                "message_id": response,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
            }
    
    def send_topic_notification(
        self,
        topic: str,
        title: str,
        body: str,
        data: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Envía una notificación push a un tópico."""
        if not self.initialized:
            return {
                "ok": False,
                "error": "Firebase no inicializado",
            }
        
        try:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body,
                ),
                data=data or {},
                topic=topic,
            )
            
            response = messaging.send(message)
            return {
                "ok": True,
                "message_id": response,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
            }
    
    def subscribe_to_topic(self, tokens: list, topic: str) -> Dict[str, Any]:
        """Suscribe dispositivos a un tópico."""
        if not self.initialized:
            return {
                "ok": False,
                "error": "Firebase no inicializado",
            }
        
        try:
            response = messaging.subscribe_to_topic(tokens, topic)
            return {
                "ok": True,
                "success_count": response.success_count,
                "failure_count": response.failure_count,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
            }
    
    def unsubscribe_from_topic(self, tokens: list, topic: str) -> Dict[str, Any]:
        """Desuscribe dispositivos de un tópico."""
        if not self.initialized:
            return {
                "ok": False,
                "error": "Firebase no inicializado",
            }
        
        try:
            response = messaging.unsubscribe_from_topic(tokens, topic)
            return {
                "ok": True,
                "success_count": response.success_count,
                "failure_count": response.failure_count,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
            }


# Instancia global
push_service = PushNotificationService()
