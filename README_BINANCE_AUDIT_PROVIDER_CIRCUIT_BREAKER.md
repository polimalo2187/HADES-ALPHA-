# Auditoría Binance / Circuit Breaker de proveedores públicos

## Hallazgo

Se revisó el repositorio buscando llamadas directas a Binance con:

- `fapi.binance.com`
- `api.binance.com`
- `stream.binance.com`
- `binance.com`
- `Binance` / `binance`

Resultado operativo:

1. El scanner de producción ya no usa Binance directo; `app/scanner.py` conserva código legacy solo para pruebas (`PYTEST_CURRENT_TEST`). En producción usa `app.market_data_public.get_public_klines_df`.
2. `app.signals` ya no descarga klines directo desde Binance; usa `get_public_current_price` y `get_public_klines_between_rows`.
3. `app.binance_api` queda como módulo de compatibilidad histórica, pero internamente enruta a `app.market_data_public`.
4. Mercado / Watchlist / Radar usan la capa pública y la cobertura combinada Bybit + OKX + Binance.
5. Las llamadas reales a Binance quedan centralizadas en `app/market_data_public.py`, únicamente como un proveedor más dentro de `MARKET_DATA_PROVIDERS`.
6. El navegador ya no debe abrir WebSocket directo contra Binance; los comentarios/documentación que mencionan Binance no son llamadas operativas.

## Problema real detectado

Cuando Binance responde `451 restricted_location`, la app seguía intentando Binance en distintas rutas:

- precio vivo de inteligencia;
- klines de seguimiento;
- tickers 24h / cobertura;
- funding / open interest;
- fallback de símbolos que solo existen en Binance.

Aunque Binance estuviera tercero, si Bybit/OKX no cubrían un símbolo, la app volvía a intentar Binance y generaba errores repetidos, consumo innecesario y mensajes técnicos visibles al usuario.

## Implementación

Se agregó un circuit breaker local por proveedor en `app/market_data_public.py`.

Comportamiento:

- `451` abre circuito del proveedor por `PUBLIC_MARKET_PROVIDER_RESTRICTED_COOLDOWN_SECONDS`.
- `403` abre circuito por `PUBLIC_MARKET_PROVIDER_FORBIDDEN_COOLDOWN_SECONDS`.
- `418/429` abre circuito por `Retry-After` o `PUBLIC_MARKET_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS`.
- `5xx` abre circuito corto por `PUBLIC_MARKET_PROVIDER_5XX_COOLDOWN_SECONDS`.
- Mientras el circuito está abierto, el proveedor se salta sin hacer request de red.
- El sistema sigue usando los otros proveedores disponibles.

También se agregó caché negativa por símbolo/proveedor para no insistir de forma agresiva cuando un símbolo no está cubierto por un proveedor.

## Variables nuevas

```env
PUBLIC_MARKET_PROVIDER_HEALTH_ENABLED=true
PUBLIC_MARKET_PROVIDER_RESTRICTED_COOLDOWN_SECONDS=1800
PUBLIC_MARKET_PROVIDER_FORBIDDEN_COOLDOWN_SECONDS=300
PUBLIC_MARKET_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS=300
PUBLIC_MARKET_PROVIDER_5XX_COOLDOWN_SECONDS=60
PUBLIC_MARKET_SYMBOL_PROVIDER_MISS_TTL_SECONDS=900
```

## UX

La pantalla de Inteligencia ya no muestra el error técnico completo de proveedores al usuario. Ahora muestra un mensaje limpio:

> No hay precio en vivo disponible para este par en los proveedores actuales.

El detalle técnico queda en logs de servidor.

## Validación

Comandos ejecutados:

```bash
python -m py_compile app/market_data_public.py app/signals.py app/scanner.py app/binance_api.py app/service.py app/miniapp/service.py app/miniapp/app.py app/market_ui.py app/telegram_handlers/features.py
HADES_DISABLE_MARKET_CACHE=1 pytest -q tests/test_strategy_breakout_reset_adaptive.py tests/test_signal_detail_service.py tests/test_miniapp_market.py tests/test_market_radar_payload.py
```

Resultado:

```txt
33 passed
```
