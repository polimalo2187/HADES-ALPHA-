# Hades AutoFutures Bridge

Implementación del puente seguro Hades Alpha -> Hades AutoFutures.

## Arquitectura

```text
Hades Alpha premium user
  ↓
POST /api/miniapp/autofutures/link
  ↓
Hades Alpha genera JWT HS256 firmado
  ↓
Redirección a Hades AutoFutures /auth/callback?token=...
  ↓
AutoFutures valida issuer, audience, firma y premium
```

## Seguridad

No se envía `premium=true` ni `userId` sin firma. El acceso usa JWT firmado con:

```text
issuer: hades-alpha
audience: hades-autofutures
```

El secreto debe coincidir con la variable `HADES_ALPHA_SSO_SECRET` configurada en Hades AutoFutures.

## Variables nuevas en Hades Alpha

```env
HADES_AUTOFUTURES_FRONTEND_URL=https://hades-autofutures-production-32f9.up.railway.app
HADES_AUTOFUTURES_SSO_SECRET=...
HADES_AUTOFUTURES_LINK_TTL_SECONDS=60
```

## Endpoint nuevo

```text
POST /api/miniapp/autofutures/link
```

Requiere sesión MiniApp activa y usuario PREMIUM activo.


## Fix 1.1: feedback visible del botón

Se reforzó el botón de AutoFutures para evitar que el usuario toque y parezca que no ocurre nada.

Cambios:

```text
- feedback inmediato: “Validando premium...”
- alertas visibles en errores
- fallback de apertura si Telegram/browser bloquea la URL
- mensaje con URL copiable si no abre automáticamente
```
