from __future__ import annotations

import inspect
import os
from typing import Dict, Optional

import pandas as pd

from app import strategy_breakout_reset as breakout_strategy
from app import strategy_liquidity_sweep as liquidity_strategy

ROUTER_VERSION = "v2_operational_fail_open_router"

_STRATEGY_MAP = {
    "breakout_reset": breakout_strategy,
    "liquidity_sweep_reversal": liquidity_strategy,
}

ROUTER_TERMINAL_RISK_OFF = str(os.getenv("MARKET_REGIME_TERMINAL_RISK_OFF", "false")).strip().lower() in {"1", "true", "yes", "on"}
ROUTER_TRY_ALTERNATE_STRATEGY = str(os.getenv("ROUTER_TRY_ALTERNATE_STRATEGY", "true")).strip().lower() in {"1", "true", "yes", "on"}


def _strategy_call_kwargs(
    strategy_module,
    *,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    reference_market_price: Optional[float],
    debug_counts: Optional[Dict[str, int]],
) -> Dict:
    kwargs: Dict = {
        "df_1h": df_1h,
        "df_15m": df_15m,
        "df_5m": df_5m,
    }
    try:
        signature = inspect.signature(strategy_module.mtf_strategy)
        parameters = signature.parameters
    except (TypeError, ValueError):
        return kwargs

    if "reference_market_price" in parameters:
        kwargs["reference_market_price"] = reference_market_price
    if "debug_counts" in parameters:
        kwargs["debug_counts"] = debug_counts if debug_counts is not None else {}
    return kwargs



def _record_reject(debug_counts: Optional[Dict[str, int]], reason: str) -> None:
    if debug_counts is None:
        return
    key = str(reason or "unknown").strip() or "unknown"
    debug_counts[key] = int(debug_counts.get(key, 0)) + 1



def select_strategy_name(market_regime: Optional[Dict]) -> str:
    snapshot = dict(market_regime or {})
    state = str(snapshot.get("state") or "unknown").strip().lower()
    explicit_strategy = str(snapshot.get("strategy_name") or "").strip().lower()
    if explicit_strategy in _STRATEGY_MAP:
        return explicit_strategy
    if explicit_strategy == "risk_off":
        return "risk_off" if ROUTER_TERMINAL_RISK_OFF else "liquidity_sweep_reversal"
    if state == "continuation_clean":
        return "breakout_reset"
    if state in {"sweep_reversal", "risk_off", "vol_shock", "cooldown"}:
        return "risk_off" if (state == "risk_off" and ROUTER_TERMINAL_RISK_OFF) else "liquidity_sweep_reversal"
    return "breakout_reset"



def route_candidate(
    *,
    symbol: str,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    market_regime: Optional[Dict],
    reference_market_price: Optional[float],
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Dict]:
    snapshot = dict(market_regime or {})
    state = str(snapshot.get("state") or "unknown").strip().lower()
    bias = str(snapshot.get("bias") or "neutral").strip().lower()
    reason = str(snapshot.get("reason") or "market_regime_unknown")
    strategy_name = select_strategy_name(snapshot)

    if strategy_name == "risk_off" or (state == "risk_off" and ROUTER_TERMINAL_RISK_OFF):
        _record_reject(debug_counts, "market_regime_terminal_risk_off")
        return None

    if state == "risk_off" and strategy_name != "liquidity_sweep_reversal":
        strategy_name = "liquidity_sweep_reversal"
        _record_reject(debug_counts, "market_regime_risk_off_rerouted_to_liquidity")

    strategy_module = _STRATEGY_MAP.get(strategy_name, breakout_strategy)
    strategy_kwargs = _strategy_call_kwargs(
        strategy_module,
        df_1h=df_1h,
        df_15m=df_15m,
        df_5m=df_5m,
        reference_market_price=reference_market_price,
        debug_counts=debug_counts,
    )
    result = strategy_module.mtf_strategy(**strategy_kwargs)
    selected_strategy_name = strategy_name
    selected_strategy_module = strategy_module

    if not result and ROUTER_TRY_ALTERNATE_STRATEGY:
        alternate_name = "liquidity_sweep_reversal" if strategy_name == "breakout_reset" else "breakout_reset"
        alternate_module = _STRATEGY_MAP.get(alternate_name, breakout_strategy)
        alternate_kwargs = _strategy_call_kwargs(
            alternate_module,
            df_1h=df_1h,
            df_15m=df_15m,
            df_5m=df_5m,
            reference_market_price=reference_market_price,
            debug_counts=debug_counts,
        )
        alternate_result = alternate_module.mtf_strategy(**alternate_kwargs)
        if alternate_result:
            _record_reject(debug_counts, f"strategy_router_fallback_from_{strategy_name}_to_{alternate_name}")
            result = alternate_result
            selected_strategy_name = alternate_name
            selected_strategy_module = alternate_module

    if not result:
        _record_reject(debug_counts, f"strategy_router_no_candidate_{strategy_name}")
        return None

    enriched = dict(result)
    strategy_name = selected_strategy_name
    strategy_module = selected_strategy_module
    enriched["strategy_name"] = strategy_name
    enriched["strategy_version"] = str(getattr(strategy_module, "SCORE_CALIBRATION_VERSION", "unknown"))
    enriched["router_version"] = ROUTER_VERSION
    enriched["regime_state"] = state
    enriched["regime_bias"] = bias
    enriched["regime_reason"] = reason
    enriched["regime_strategy_selected"] = strategy_name
    enriched.setdefault("timeframes", ["5M"] if strategy_name == "breakout_reset" else ["15M"])
    return enriched
