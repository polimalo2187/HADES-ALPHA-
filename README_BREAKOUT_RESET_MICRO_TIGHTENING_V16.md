# Breakout + Reset v16 - Loss Control Micro Tightening

Ajuste pequeño y controlado para reducir señales Breakout + Reset débiles sin dejar la estrategia muda.

## Objetivo

La estrategia estaba generando demasiadas señales perdedoras. Este ajuste no cambia el modelo de envío anticipado: la señal sigue publicándose antes del reset, con entrada pendiente en la zona estructural.

## Cambios principales

- Se subió moderadamente el score mínimo por perfil.
- Se endureció levemente ADX, ATR válido, cuerpo de breakout, continuación, volumen relativo y cierre de continuación.
- Se estrechó el rango de extensión ATR para evitar entradas demasiado extendidas.
- Se exige mejor espacio previo al reset para evitar setups demasiado pegados al nivel.
- El perfil FREE ya no acepta continuación por una sola evidencia aislada; ahora necesita dos señales ligeras de follow-through.
- La invalidación post-breakout detecta antes cuando el precio vuelve dentro de la estructura rota.

## Nueva calibración

`v16_breakout_reset_loss_control_tightened`

## Archivos modificados

- `app/strategy_breakout_reset.py`

## Archivos nuevos

- `README_BREAKOUT_RESET_MICRO_TIGHTENING_V16.md`

## Nota operativa

Si existen variables de entorno antiguas como `FREE_ADX_MIN`, `PLUS_MIN_EXTENSION_ATR`, `PREMIUM_RAW_SCORE_MIN`, etc., esas variables pueden sobrescribir estos defaults del código.
