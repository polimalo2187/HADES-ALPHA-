# Watchlist (módulo nuevo)

✅ **No toca `handlers.py`** (como pediste).
✅ Guarda watchlist por usuario en MongoDB (colección: `watchlists`).
✅ Normaliza símbolos a USDT-M Futures (ej: BTC -> BTCUSDT).

## Archivos añadidos
- `app/watchlist.py` (CRUD + límites por plan)
- `app/watchlist_ui.py` (texto + teclado inline)

## Cómo se conecta (cuando tú lo autorices)
En `handlers.py` (cambio mínimo futuro):
- al tocar ⭐ Watchlist -> mostrar `render_watchlist_view(get_symbols(user_id))`
- capturar callbacks:
  - `wl_refresh`, `wl_clear`, `wl_rm:<SYM>`
- y capturar mensajes de texto para añadir símbolos (opcional).

## Límites por plan
- FREE: 2 símbolos
- PLUS: 10 símbolos
- PREMIUM: ilimitado
