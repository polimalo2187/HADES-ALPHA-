# HADES Mini App V1

## Qué incluye
- Backend FastAPI embebido en el mismo proyecto.
- Autenticación de Telegram Mini App (`initData`).
- Sesión firmada del lado servidor.
- Dashboard, señales, mercado, historial, cuenta y planes.
- Creación / confirmación / cancelación de órdenes de pago desde la mini-app.
- Botón `🚀 HADES App` dentro del bot cuando `MINI_APP_URL` está configurada.

## Variables nuevas / relevantes
- `ENABLE_MINI_APP_SERVER=true`
- `MINI_APP_URL=https://TU-DOMINIO/miniapp`
- `MINI_APP_SESSION_SECRET=` (opcional, recomendado)
- `MINI_APP_SESSION_TTL_SECONDS=43200` (opcional)
- `APP_RUNTIME_ROLE=web|bot|bot_ui|signal_worker|scheduler`
- `MINI_APP_CORS_ORIGINS=https://tu-dominio.com,https://otro-dominio.com` (opcional)
- `MINI_APP_ALLOW_DEV_AUTH=true` (solo desarrollo)
- `MINI_APP_DEV_USER_ID=` (obligatorio si usas auth dev)

## Seguridad de auth dev
La autenticación de desarrollo **ya no acepta cualquier `dev_user_id` enviado por el cliente**.

Ahora solo funciona si se cumplen **las dos condiciones**:
1. `MINI_APP_ALLOW_DEV_AUTH=true`
2. `ENVIRONMENT` está en entorno no productivo (`dev`, `development`, `local`, `test`, `testing` o `staging`)

Además, el `dev_user_id` permitido queda fijado por `MINI_APP_DEV_USER_ID`.
Si el cliente manda otro ID, la autenticación se rechaza.

## CORS
No se usa `allow_origins=["*"]` en modo normal.

Orden de resolución:
1. Si defines `MINI_APP_CORS_ORIGINS`, se usan esos orígenes.
2. Si no, se deriva el origen desde `MINI_APP_URL`.
3. Si auth dev está habilitada, se agregan orígenes locales (`localhost:3000`, `localhost:5173`, etc.).

## Arranque por proceso
### Web / MiniApp API
```bash
ENABLE_MINI_APP_SERVER=true APP_RUNTIME_ROLE=web python main.py
```

### Bot Telegram completo (compatibilidad actual)
```bash
APP_RUNTIME_ROLE=bot python main.py
```
Levanta polling + scanner + scheduler.

### Bot solo UI Telegram
```bash
APP_RUNTIME_ROLE=bot_ui python main.py
```
Levanta solo polling, sin scanner ni scheduler.

### Worker dedicado de señales
```bash
APP_RUNTIME_ROLE=signal_worker python main.py
```
Levanta pipeline + scanner, sin polling Telegram.

### Worker dedicado de scheduler
```bash
APP_RUNTIME_ROLE=scheduler python main.py
```
Levanta solo el scheduler.

## Recomendación operativa de transición
Para MiniApp-first en producción:
- un proceso `web`
- un proceso `signal_worker`
- un proceso `scheduler`
- opcionalmente un proceso `bot_ui` si todavía mantienes el bot conversacional

## Nota importante
`ENABLE_MINI_APP_SERVER=true` **ya no significa** “levantar bot + web juntos”.
El comportamiento final lo controla `APP_RUNTIME_ROLE`.
Si no defines `APP_RUNTIME_ROLE`, el proyecto mantiene compatibilidad:
- con Mini App habilitada => arranca `web`
- sin Mini App habilitada => arranca `bot`
