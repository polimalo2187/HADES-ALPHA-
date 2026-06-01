# Liquidity Sweep micro-tightening v4.4

Ajuste pequeño y controlado de Liquidity Sweep para reducir señales débiles y señales que quedan en No Entry sin apagar la estrategia.

## Cambios

- Score mínimo sube solo 1 punto por perfil:
  - Free: 72 → 73
  - Plus: 78 → 79
  - Premium: 85 → 86
- Sweep mínimo sube apenas 0.01 ATR por perfil.
- Volumen relativo y volumen de confirmación suben 0.02.
- Confirmación de cuerpo y posición de cierre suben 0.01.
- RR mínimo sube 0.02.
- Pullback pendiente queda un poco menos profundo:
  - retrace fraction: 0.42 → 0.40
  - max pullback ATR: 0.44 → 0.39
  - min pullback ATR: 0.18 → 0.17
- El seguimiento post-entry exige un poco más de avance mínimo hacia TP1:
  - 22% → 24%

## Objetivo

Reducir señales que no tocan entrada y setups débiles, sin dejar la estrategia muda.

## Nota operacional

Si existen variables `LSR_*`, `FREE_RAW_SCORE_MIN`, `PLUS_RAW_SCORE_MIN` o `PREMIUM_RAW_SCORE_MIN` en Railway, esas variables pisan estos defaults del código.
