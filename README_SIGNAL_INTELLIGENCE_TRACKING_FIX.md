# Signal Intelligence Tracking Fix

## Problema corregido

La pantalla de inteligencia podía mostrar:

```txt
No pude reconstruir el tracking intrabar: name '_entry_window_allows_new_fill' is not defined
```

El error ocurría porque `app.signals` llamaba helpers de tracking/fill que habían quedado fuera del archivo durante refactors previos.

## Cambio aplicado

Se restauró la fuente canónica de helpers dentro de `app/signals.py` para que la evaluación automática y la inteligencia de señal compartan las mismas reglas:

- `_candle_time_bounds`
- `_entry_window_allows_new_fill`
- `_candle_within_window`
- `_pending_entry_touched`
- `_entry_touched_in_candle`
- `_entry_zone_touched_in_candle`
- `_tp1_progress_pct`
- `_excursions_after_entry_r`
- `_evaluation_observability_payload`
- `_tp1_protection_price`

## Semántica preservada

- Liquid Sweep y Breakout + Reset se envían anticipadas con `entry_price` pendiente.
- La entrada solo se marca como tocada si la vela toca el `entry_zone` o el `entry_price` dentro de `entry_valid_until`.
- Después de tocar entrada, la señal ya no expira por tiempo; continúa hasta TP1/TP2/SL.
- La pantalla de inteligencia reconstruye el tracking intrabar sin hacer llamadas extra fuera del provider público ya usado por `app.signals`.

## Validación ejecutada

```bash
python -m py_compile app/signals.py app/market_data_public.py app/service.py app/miniapp/service.py app/miniapp/app.py app/market_ui.py app/telegram_handlers/features.py
pytest -q tests/test_signal_detail_service.py tests/test_strategy_breakout_reset_adaptive.py
```

Resultado:

```txt
27 passed
```
