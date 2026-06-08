# Hades AutoFutures Bridge - patrón Oraculum/Sentinel

Esta versión elimina completamente el flujo de secreto SSO.

AutoFutures usa el mismo patrón de enlace temporal que Oraculum y Sentinel:

```text
Hades Alpha premium user
  ↓
POST /api/miniapp/autofutures/link
  ↓
Hades Alpha crea un code temporal
  ↓
Guarda hash en MongoDB: autofutures_link_tokens
  ↓
Redirige a AutoFutures /auth/hades/link?code=...
  ↓
AutoFutures consume el code contra Hades Alpha
```

## Variables en Hades Alpha

```env
HADES_AUTOFUTURES_FRONTEND_URL=https://hades-autofutures-production-32f9.up.railway.app
HADES_AUTOFUTURES_LINK_TTL_SECONDS=60
```

No se usa secreto SSO en Hades Alpha.

## Endpoints

```text
POST /api/miniapp/autofutures/link
POST /api/miniapp/autofutures/consume
```

## Colección MongoDB

```text
autofutures_link_tokens
```


## Fix exhaustivo 1.0

Correcciones aplicadas para igualar el flujo a Oraculum/Sentinel/Guide y evitar 502 silenciosos:

```text
- consume_autofutures_code ya no importa app.miniapp.service; usa app.plans.get_user.
- Se evita importar el módulo pesado de MiniApp en el endpoint público de consumo.
- Se capturan errores PyMongo como storage_unavailable.
- storage_unavailable devuelve HTTP 503 controlado, no timeout/502 de infraestructura.
- MongoClient tiene timeouts explícitos.
```
