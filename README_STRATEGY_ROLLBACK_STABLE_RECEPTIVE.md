# HADES ALPHA — Strategy rollback to stable receptive calibration

## Purpose
This package rolls back the latest over-tightened strategy calibrations after the 7D performance window degraded.

The goal is not to make strategies loose blindly. The goal is to return to the last known operationally receptive baseline while preserving all infrastructure fixes already applied:

- multi-provider market data
- provider circuit breaker
- watchlist validation fixes
- trading session schedule
- live price in signal intelligence
- No Entry labeling
- Hades Guide bridge fixes where present in the base

## Strategy changes

### Liquidity Sweep
Rolled back from:

`v4_5_liquidity_sweep_entry_quality_tightened`

To:

`v4_2_liquidity_sweep_operational_receptive`

Reason:
The later tightening increased No Entry / weak resolution behavior. This returns Liquidity Sweep to the more receptive calibration that was closer to the previous strong operating window.

### Breakout + Reset
Rolled back from:

`v16_breakout_reset_loss_control_tightened`

To:

`v15_breakout_reset_recent_pending_entry`

Reason:
The v16 loss-control tightening likely filtered too late/too strict and worsened execution quality. v15 keeps the corrected pending-entry logic, recent breakout window, and pre-reset signal behavior without the extra restrictive quality gates.

## Files changed

Modified/touched:

- `app/strategy_liquidity_sweep.py`
- `app/strategy_breakout_reset.py`

New:

- `README_STRATEGY_ROLLBACK_STABLE_RECEPTIVE.md`

## Important
If Railway contains environment variables that override strategy thresholds, they can still override these code defaults. Remove old strategy variables such as:

- `LSR_*`
- `FREE_RAW_SCORE_MIN`
- `PLUS_RAW_SCORE_MIN`
- `PREMIUM_RAW_SCORE_MIN`
- breakout threshold variables

Use this rollback for observation before applying any new tightening.
