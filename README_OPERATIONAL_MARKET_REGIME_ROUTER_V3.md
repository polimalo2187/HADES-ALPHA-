# HADES Alpha — Operational Market Regime Router V3

## Objetivo

Evitar que la plataforma quede muerta porque `market_regime_risk_off` clasifique todo el mercado como no operable.

## Nueva lógica

- `continuation_clean` -> `Breakout + Reset`
- `sweep_reversal`, chop, trampa, vol shock o risk-off normal -> `Liquidity Sweep Reversal`
- `risk_off` terminal solo bloquea si `MARKET_REGIME_TERMINAL_RISK_OFF=true`

## Por qué

Los logs mostraban ciclos completos con:

`rejects={'market_regime_risk_off': 116}`

Eso significa que el router estaba bloqueando todos los símbolos antes de permitir trabajar a Breakout o Liquidity Sweep.

## Archivos tocados

- `app/regime_engine.py`
- `app/strategy_router.py`
- `env.runtime.example`
- `README_OPERATIONAL_MARKET_REGIME_ROUTER_V3.md`
