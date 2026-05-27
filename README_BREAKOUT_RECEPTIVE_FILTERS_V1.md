# HADES Alpha — Breakout + Reset Receptive Filters V1

## Objetivo

Aumentar frecuencia útil de Breakout + Reset sin tocar el score.

## Causa observada

En la captura de observabilidad, Breakout + Reset rechaza principalmente por:

- `BREAKOUT_SHAPE`
- `ATR_PCT`
- `ADX_STRENGTH`
- `TREND_STRUCTURE`
- `SYMBOL_REGIME_SYMBOL_SWEEP_CHOP`

## Cambios aplicados

- No se tocaron:
  - `PREMIUM_RAW_SCORE_MIN`
  - `PLUS_RAW_SCORE_MIN`
  - `FREE_RAW_SCORE_MIN`
  - fórmula de score
  - selección por ranking
- Se relajaron filtros terminales:
  - ADX mínimo
  - rango ATR%
  - body ratio de breakout/continuación
  - extensión ATR mínima/máxima
  - overshoot mínimo
  - pre-reset space
  - continuación premium pasó de 3/3 confirmaciones a 2/3 con condición direccional
  - trend structure permite fases tempranas de continuación con control anti-chop
- Se separó `breakout_reset_already_touched` de `breakout_shape` para mejor diagnóstico.

## Archivos tocados

- `app/strategy_breakout_reset.py`
- `env.runtime.example`
- `README_BREAKOUT_RECEPTIVE_FILTERS_V1.md`
