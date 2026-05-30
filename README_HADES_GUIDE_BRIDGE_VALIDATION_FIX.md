# Hades Guide Bridge Validation Fix

## Problema corregido

El botón de Hades Guide ya redirigía correctamente, pero Hades Guide mostraba:

> No se pudo validar el acceso con Hades

La causa real era el contrato incompleto entre ambas apps:

- HADES emitía un enlace temporal con `code`.
- Hades Guide intentaba validar ese `code` contra `APP_BRIDGE_VERIFY_PATH`.
- En HADES principal faltaba exponer la ruta server-to-server `/api/miniapp/guide/consume` en el backend MiniApp actual.

## Cambios

### HADES principal

Archivo:

- `app/miniapp/app.py`

Agregado:

- `POST /api/miniapp/guide/link`
- `POST /api/miniapp/guide/consume`
- `HadesGuideConsumeRequest`
- Import de `create_hades_guide_link` y `consume_hades_guide_code`
- Auditoría de creación y consumo del código

### Hades Guide

Archivo:

- `backend/src/services/hadesBridgeService.js`

Agregado/mejorado:

- Normalización defensiva del payload devuelto por HADES
- Mensajes de error más precisos
- Soporte para `payload.user`, `payload.data.user` o `payload.result.user`
- Header interno `X-Hades-Guide-Bridge: 1`

## Flujo correcto

1. Usuario toca Hades Guide en HADES.
2. HADES crea un `code` temporal de un solo uso.
3. Hades Guide abre `/auth/hades/callback?code=...`.
4. Hades Guide backend llama a HADES: `POST /api/miniapp/guide/consume`.
5. HADES consume el código y devuelve usuario/permisos.
6. Hades Guide crea su sesión local.

## Importante

Subir ambos lados:

- HADES principal backend
- Hades Guide backend

Si solo subes Hades Guide, seguirá fallando si HADES no tiene `/api/miniapp/guide/consume`.
