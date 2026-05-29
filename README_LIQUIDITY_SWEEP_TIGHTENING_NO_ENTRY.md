# Liquidity Sweep quality tightening + No Entry label

## Objetivo

Reducir la cantidad de señales `Liquidity Sweep Reversal` sin dejar la estrategia muda. El ajuste endurece la calidad mínima del setup, mantiene el modelo de señal anticipada con entrada pendiente y actualiza el lenguaje visual de rendimiento para que los usuarios entiendan que una señal vencida antes de tocar entrada es `No Entry`.

## Cambios de estrategia

Archivo principal:

- `app/strategy_liquidity_sweep.py`

Cambios de defaults en código:

- Score calibration: `v4_3_liquidity_sweep_quality_tightened`
- Score mínimo:
  - Free: `69 -> 72`
  - Plus: `76 -> 78`
  - Premium: `83 -> 85`
- Sweep mínimo por ATR:
  - Free: `0.07 -> 0.10`
  - Plus: `0.10 -> 0.13`
  - Premium: `0.14 -> 0.17`
- Volumen relativo mínimo y confirmación subidos de forma moderada.
- Cuerpo/confirmación y posición de cierre subidos de forma moderada.
- RR mínimo subido moderadamente.
- Pullback aceptable ligeramente más limpio:
  - `LSR_PULLBACK_ATR_MIN=0.18`
  - `LSR_PULLBACK_ATR_MAX=0.44`
  - `LSR_PULLBACK_MARKET_GAP_ATR=0.05`
- Follow-through mínimo post-entry:
  - `18% -> 22%` hacia TP1.

## Importante sobre variables de entorno

Los valores quedaron dentro de los archivos como defaults. Si en Railway existen variables `LSR_*`, `FREE_RAW_SCORE_MIN`, `PLUS_RAW_SCORE_MIN` o `PREMIUM_RAW_SCORE_MIN`, esas variables ganan sobre el código. Para usar estos defaults, elimina variables antiguas conflictivas del servidor.

## Cambios visuales

Archivos:

- `app/miniapp/static/app.js`
- `app/miniapp/static/sw.js`
- `app/service.py`
- `app/miniapp/service.py`

Cambios:

- Donde rendimiento mostraba `Exp` / `Expirada`, ahora muestra `No Entry` para señales que no tocaron entrada.
- Se conserva `Fallo post-entry` para señales que sí tocaron entrada pero no desarrollaron.
- Se bumpó caché del frontend/service worker para evitar assets viejos.
