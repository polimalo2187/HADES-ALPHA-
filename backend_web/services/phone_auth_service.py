import os
from typing import Optional, Dict, Any
from twilio.rest import Client
from config import TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_SERVICE_SID


class PhoneAuthService:
    """Servicio de autenticación por teléfono usando Twilio Verify."""
    
    def __init__(self):
        if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
            self.client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
            self.service_sid = TWILIO_SERVICE_SID
        else:
            self.client = None
            self.service_sid = None
    
    def send_verification_code(self, phone_number: str) -> Dict[str, Any]:
        """Envía un código de verificación por SMS."""
        if not self.client or not self.service_sid:
            # Modo desarrollo: retorna código simulado
            return {
                "ok": True,
                "message": "Código de verificación enviado (modo desarrollo: 123456)",
                "dev_code": "123456",
            }
        
        try:
            verification = self.client.verify.v2.services(self.service_sid).verifications.create(
                to=phone_number,
                channel="sms"
            )
            return {
                "ok": True,
                "message": "Código de verificación enviado",
                "status": verification.status,
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
            }
    
    def verify_code(self, phone_number: str, code: str) -> Dict[str, Any]:
        """Verifica el código ingresado por el usuario."""
        if not self.client or not self.service_sid:
            # Modo desarrollo: acepta cualquier código
            if code == "123456":
                return {
                    "ok": True,
                    "message": "Verificación exitosa",
                    "verified": True,
                }
            return {
                "ok": False,
                "error": "Código inválido (modo desarrollo: usa 123456)",
            }
        
        try:
            verification_check = self.client.verify.v2.services(self.service_sid).verification_checks.create(
                to=phone_number,
                code=code
            )
            if verification_check.status == "approved":
                return {
                    "ok": True,
                    "message": "Verificación exitosa",
                    "verified": True,
                }
            return {
                "ok": False,
                "error": "Código inválido o expirado",
            }
        except Exception as e:
            return {
                "ok": False,
                "error": str(e),
            }


# Instancia global
phone_auth_service = PhoneAuthService()
