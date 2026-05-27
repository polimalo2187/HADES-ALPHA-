from __future__ import annotations

import inspect
from typing import Dict, Optional

import pandas as pd

from app import strategy_breakout_reset as breakout_strategy
from app import strategy_liquidity_sweep as liquidity_strategy

ROUTER_VERSION = "v1_regime_strategy_router"

_STRATEGY_MAP = {
    "breakout_reset": breakout_strategy,
    "liquidity_sweep_reversal": liquidity_strategy,
}


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
    if explicit_strategy in _STRATEGY_MAP or explicit_strategy == "risk_off":
        return explicit_strategy
    if state == "continuation_clean":
        return "breakout_reset"
    if state == "sweep_reversal":
        return "liquidity_sweep_reversal"
    if state == "risk_off":
        return "risk_off"
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

    if strategy_name == "risk_off" or state == "risk_off":
        _record_reject(debug_counts, "market_regime_risk_off")
        return None

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
    if not result:
        _record_reject(debug_counts, f"strategy_router_no_candidate_{strategy_name}")
        return None

    enriched = dict(result)
    enriched["strategy_name"] = strategy_name
    enriched["strategy_version"] = str(getattr(strategy_module, "SCORE_CALIBRATION_VERSION", "unknown"))
    enriched["router_version"] = ROUTER_VERSION
    enriched["regime_state"] = state
    enriched["regime_bias"] = bias
    enriched["regime_reason"] = reason
    enriched["regime_strategy_selected"] = strategy_name
    enriched.setdefault("timeframes", ["5M"] if strategy_name == "breakout_reset" else ["15M"])
    return enriched
