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
