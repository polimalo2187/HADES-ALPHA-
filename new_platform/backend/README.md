# Backend Hades Platform

Backend en Python/FastAPI para la plataforma Web + App sin dependencias de Telegram.

## Estructura

```
backend/
├── main.py                 # Aplicación principal FastAPI
├── config.py               # Configuración y variables de entorno
├── database.py             # Conexión MongoDB
├── observability.py        # Health checks y logs
├── models/
│   └── __init__.py         # Modelos de datos (Usuario, Señal, Pago, etc.)
├── routes/
│   ├── __init__.py
│   ├── auth.py             # Login/Registro con teléfono+contraseña
│   ├── users.py            # Gestión de usuarios
│   ├── signals.py          # Señales de trading
│   ├── market.py           # Datos de mercado
│   ├── payments.py         # Pagos BEP-20
│   ├── admin.py            # Panel de administración
│   └── websocket.py        # Push notifications internas
└── services/
    ├── __init__.py
    ├── scheduler_service.py    # Tareas programadas
    └── scanner_service.py      # Escaneo de mercado
```

## Características Principales

### 1. Autenticación
- **Registro/Login con teléfono + contraseña**
- **Sin verificación SMS** - registro directo
- JWT tokens para sesiones
- Password hashing con bcrypt

### 2. Push Notifications Internas
- **WebSockets nativos** - sin servicios de terceros
- Conexiones persistentes por usuario
- Heartbeat automático
- Notificaciones de:
  - Nuevas señales
  - Actualizaciones de pago
  - Mensajes del sistema

### 3. Base de Datos
- MongoDB con motor asíncrono
- Índices optimizados
- Colecciones: users, signals, user_signals, payment_orders, push_sessions, audit_logs

## Instalación

```bash
# Instalar dependencias
pip install fastapi uvicorn motor pydantic bcrypt python-jose[cryptography]

# Copiar configuración
cp config/.env.production .env

# Editar .env con tus valores reales
# MONGODB_URI, AUTH_SESSION_SECRET, PAYMENT_RECEIVER_ADDRESS, etc.

# Ejecutar
python backend/main.py
```

## Variables de Entorno Obligatorias

Ver `/workspace/new_platform/config/.env.production` para la lista completa.

### Mínimas requeridas:
- `MONGODB_URI` - Conexión a MongoDB
- `DATABASE_NAME` - Nombre de la base de datos
- `AUTH_SESSION_SECRET` - Secreto para JWT (min 32 chars)
- `PAYMENT_TOKEN_CONTRACT` - Contrato USDT BEP-20
- `PAYMENT_RECEIVER_ADDRESS` - Wallet receptora
- `BSC_RPC_HTTP_URL` - RPC de Binance Smart Chain

## Endpoints API

### Autenticación
- `POST /api/auth/register` - Registro con teléfono+password
- `POST /api/auth/login` - Login
- `GET /api/auth/me` - Perfil del usuario
- `POST /api/auth/refresh` - Refrescar token

### WebSocket
- `WS /api/ws/push?token=<JWT>&session_id=<ID>` - Conexión push

### Salud
- `GET /health` - Health check básico
- `GET /health/live` - Live probe
- `GET /health/ready` - Ready probe

## Diferencias con la Plataforma Original

| Original | Nueva Plataforma |
|----------|------------------|
| Auth vía Telegram initData | Auth teléfono+contraseña directo |
| Push via Telegram Bot API | Push interno vía WebSockets |
| Dependencia total de Telegram | Cero dependencias de Telegram |
| MiniApp embebida en Telegram | Web + App independientes |
