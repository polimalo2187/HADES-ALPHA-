"""
MTF Futures Scanner Bot
=======================

Bot de Telegram para escaneo de mercados de futuros con señales multi-timeframe.
"""

import logging
import sys
from datetime import datetime

# ======================================================
# CONFIGURACIÓN DE LOGGING
# ======================================================

def setup_logging():
    """
    Configura el logging para toda la aplicación.
    """
    log_format = (
        '%(asctime)s - %(name)s - %(levelname)s - '
        '[%(filename)s:%(lineno)d] - %(message)s'
    )
    
    log_level = logging.INFO
    if "--debug" in sys.argv or "-d" in sys.argv:
        log_level = logging.DEBUG
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    
    # Reducir verbosidad de algunas librerías
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info("✅ Logging configurado")
    
    return logger

# ======================================================
# METADATOS
# ======================================================

__version__ = "1.0.0"
__author__ = "MTF Futures Scanner Team"
__description__ = "Bot de escaneo de mercados para futuros con señales MTF"

# ======================================================
# FUNCIÓN DE INICIALIZACIÓN SIMPLIFICADA
# ======================================================

def initialize_app():
    """
    Inicializa la aplicación verificando configuraciones básicas.
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info(f"Iniciando MTF Futures Scanner v{__version__}")
        logger.info("=" * 60)
        
        # Verificar variables de entorno críticas
        import os
        critical_vars = ["BOT_TOKEN", "MONGODB_URI", "DATABASE_NAME"]
        missing_vars = [var for var in critical_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"❌ Variables de entorno faltantes: {missing_vars}")
            return False
        else:
            logger.info("✅ Variables de entorno críticas verificadas")
        
        logger.info("✅ Aplicación inicializada correctamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante la inicialización: {e}")
        return False

# ======================================================
# MÓDULO PRINCIPAL
# ======================================================

if __name__ == "__main__":
    print(f"{__description__} v{__version__}")
    print(f"Por: {__author__}")
    print()
    print("Este archivo es parte del paquete 'app'.")
    print("Para iniciar el bot, ejecuta: python main.py")
