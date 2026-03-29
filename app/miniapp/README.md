# HADES Mini App V1

## Qué incluye
- Backend FastAPI embebido en el mismo proyecto.
- Autenticación de Telegram Mini App (`initData`).
- Sesión firmada del lado servidor.
- Dashboard, señales, mercado, historial, cuenta y planes.
- Creación / confirmación / cancelación de órdenes de pago desde la mini-app.
- Botón `🚀 HADES App` dentro del bot cuando `MINI_APP_URL` está configurada.

## Variables nuevas
- `ENABLE_MINI_APP_SERVER=true`
- `MINI_APP_URL=https://TU-DOMINIO/miniapp`
- `MINI_APP_SESSION_SECRET=` (opcional, recomendado)
- `MINI_APP_SESSION_TTL_SECONDS=43200` (opcional)
- `MINI_APP_DEV_USER_ID=` (opcional, solo para pruebas fuera de Telegram)

## Arranque
- Con `ENABLE_MINI_APP_SERVER=true`, `main.py` levanta:
  - el bot en background
  - FastAPI/Uvicorn en el puerto `PORT`
- Sin esa variable, el proyecto sigue arrancando solo el bot como antes.
