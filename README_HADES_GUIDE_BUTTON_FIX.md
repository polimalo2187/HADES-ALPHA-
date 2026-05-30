# HADES Guide button fix

## Problema

Los botones `Abrir Hades Guide` del dashboard principal y de la vista Cuenta no respondían.

La causa real era doble:

1. El frontend sí llamaba a `POST /api/miniapp/guide/link`, pero el backend no tenía implementada esa ruta.
2. `app/miniapp/static/app.js` tenía una regresión de JavaScript: una constante `normalized` declarada dos veces dentro de `createAndOpenEcosystemLink`, lo que podía romper la ejecución del archivo completo y dejar botones sin handlers.

## Solución

Se agregó el endpoint backend:

```txt
POST /api/miniapp/guide/link
```

Ese endpoint usa `app.hades_guide_bridge.create_hades_guide_link()` y genera una URL temporal segura hacia Hades Guide.

También se corrigió el JavaScript de la MiniApp:

- se eliminó la doble declaración de `normalized`;
- se mejoró el manejo de errores del botón Hades Guide;
- se fuerza feedback visual con `Telegram.WebApp.showAlert` o `window.alert` como fallback;
- se hizo bump del Service Worker para expulsar assets viejos.

## Archivos modificados

```txt
app/miniapp/app.py
app/miniapp/static/app.js
app/miniapp/static/sw.js
README_HADES_GUIDE_BUTTON_FIX.md
```

## Validación

```txt
python -m py_compile app/miniapp/app.py app/hades_guide_bridge.py
node --check app/miniapp/static/app.js
node --check app/miniapp/static/sw.js
pytest -q tests/test_miniapp_bootstrap_endpoint.py tests/test_miniapp_auth.py tests/test_miniapp_account.py tests/test_miniapp_config.py
```

Resultado:

```txt
7 passed
```
