# HADES Alpha — Egress Optimization + Top 50 Scanner

Base usada: `HADES-ALPHA-ecosystem-auth-flow-fix-v3`.

## Objetivo

Reducir consumo de Network Egress y llamadas externas sin degradar la calidad principal de señales.

## Cambios clave

1. Scanner limitado a los 50 pares USDT Futures de mayor volumen 24h.
2. Se mantiene escaneo balanceado cada 45s, no 120s.
3. Access logs de Uvicorn apagados por defecto.
4. Cache headers para assets estáticos de la Mini App.
5. Polling de live-signals reducido sin eliminar actualización activa.
6. Se evitan refrescos duplicados por eventos focus/visibility de Telegram.
7. Se detienen timers y WebSocket al salir de la página.

## Variables recomendadas

```env
UVICORN_ACCESS_LOG=false
UVICORN_LOG_LEVEL=warning

SCAN_INTERVAL_SECONDS=45
SCANNER_MAX_SYMBOLS=50
MIN_QUOTE_VOLUME=20000000
SCANNER_SYMBOL_CONCURRENCY=8
SCANNER_MAX_REQUESTS_PER_SECOND=4
SCANNER_MAX_BURST=8
REQUEST_TIMEOUT=8
REQUEST_MAX_RETRIES=2
SCANNER_5M_CACHE_SECONDS=45
ACTIVE_SYMBOLS_CACHE_SECONDS=900
```

## Por qué Top 50

Escanear los 50 pares de mayor volumen reduce:
- llamadas a Binance,
- payload de datos,
- CPU del scanner,
- escrituras/lecturas indirectas,
- riesgo de pares ilíquidos.

Y mantiene:
- liquidez,
- mejor ejecución,
- menos spread,
- mejor calidad operativa.
