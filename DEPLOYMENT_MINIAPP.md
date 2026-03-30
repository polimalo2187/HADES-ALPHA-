# Despliegue operativo MiniApp-first

Este proyecto ya puede correr por procesos separados. La combinación recomendada para producción es:

- `web`
- `signal_worker`
- `scheduler`
- `bot_ui` (opcional, solo mientras sigas usando el bot conversacional)

## Matriz de procesos

### 1) Web / MiniApp API
**Comando**
```bash
APP_RUNTIME_ROLE=web python main.py
```

**Variables mínimas**
- `ENVIRONMENT=production`
- `ENABLE_MINI_APP_SERVER=true`
- `BOT_TOKEN=...`
- `MONGODB_URI=...`
- `DATABASE_NAME=...`
- `MINI_APP_URL=https://TU-DOMINIO/miniapp`
- `MINI_APP_SESSION_SECRET=...`

**Variables recomendadas**
- `MINI_APP_CORS_ORIGINS=https://TU-DOMINIO`
- `PORT=8000` (lo normal es que Railway o el proveedor lo inyecte)

### 2) Signal worker
**Comando**
```bash
APP_RUNTIME_ROLE=signal_worker python main.py
```

**Variables mínimas**
- `ENVIRONMENT=production`
- `BOT_TOKEN=...`
- `MONGODB_URI=...`
- `DATABASE_NAME=...`

**Notas**
- Este proceso corre scanner + pipeline.
- No levanta FastAPI.
- No levanta polling del bot.

### 3) Scheduler
**Comando**
```bash
APP_RUNTIME_ROLE=scheduler python main.py
```

**Variables mínimas**
- `ENVIRONMENT=production`
- `MONGODB_URI=...`
- `DATABASE_NAME=...`

**Notas**
- Ya no depende de `BOT_TOKEN`.
- Corre expiración de planes, limpieza, stats, histórico y mantenimiento.

### 4) Bot UI (opcional)
**Comando**
```bash
APP_RUNTIME_ROLE=bot_ui python main.py
```

**Variables mínimas**
- `ENVIRONMENT=production`
- `BOT_TOKEN=...`
- `MONGODB_URI=...`
- `DATABASE_NAME=...`

**Notas**
- Solo polling Telegram.
- Sin scanner.
- Sin scheduler.
- Úsalo solo mientras terminas la migración funcional a MiniApp.

## Qué no debes hacer ya
No mezcles en un mismo proceso:
- FastAPI
- polling del bot
- scanner
- scheduler

Se puede hacer por compatibilidad, pero no es el layout correcto para producción MiniApp-first.

## Reparto recomendado en Railway
Crea servicios separados apuntando al mismo repo:

1. `hades-web`
   - Start command: `APP_RUNTIME_ROLE=web python main.py`
2. `hades-signal-worker`
   - Start command: `APP_RUNTIME_ROLE=signal_worker python main.py`
3. `hades-scheduler`
   - Start command: `APP_RUNTIME_ROLE=scheduler python main.py`
4. `hades-bot-ui` (opcional)
   - Start command: `APP_RUNTIME_ROLE=bot_ui python main.py`

## Health checks
- `web`: usa `/miniapp/health/live`, `/miniapp/health/ready` y `/miniapp/health`
- `signal_worker`: vigílalo por heartbeats en DB (`signal_worker`, `scanner`, `signal_pipeline`)
- `scheduler`: vigílalo por heartbeats en DB (`scheduler`, `statistics`, `history`, `database`)
- `bot_ui`: vigílalo por heartbeats en DB (`bot`, `database`)

`/miniapp/health/ready` devuelve `503` cuando faltan componentes requeridos, cuando un componente reporta error o cuando un heartbeat está viejo.

## Falla rápida de configuración
`main.py` ahora valida variables críticas según el rol. Si falta algo esencial, el proceso falla al arrancar con un error explícito.

Eso evita despliegues medio rotos donde aparentemente “corre” pero el proceso quedó mal configurado.

## Endpoints admin operativos (MiniApp)
Estos endpoints son de solo lectura y requieren un usuario autenticado cuyo `user_id` esté incluido en `ADMIN_USER_IDS`.

- `/api/miniapp/admin/overview`
  - Resumen operativo consolidado: runtimes, usuarios, señales, pagos y recuento básico de auditoría.
- `/api/miniapp/admin/health`
  - Matriz de salud por runtime (`web`, `bot`, `signal_worker`, `scheduler`).
- `/api/miniapp/admin/audit?limit=25&status=error&module=miniapp`
  - Eventos recientes de auditoría con filtros opcionales por estado y módulo.

Esto deja la base para un panel admin web sin depender de inspección manual de Mongo o de logs crudos.


## Operación y trazabilidad

La MiniApp ahora expone además:

- `/api/miniapp/admin/incidents`: feed consolidado de incidentes (health degradado + audit warning/error)
- `X-Request-ID` en respuestas HTTP para rastrear errores 5xx y requests problemáticas

Ante errores no controlados de la MiniApp, la API devuelve `500` con este formato:

```json
{
  "ok": false,
  "detail": "internal_server_error",
  "request_id": "..."
}
```
