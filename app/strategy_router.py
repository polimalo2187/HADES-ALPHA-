from __future__ import annotations

import inspect
import os
from typing import Dict, Optional, Tuple

import pandas as pd

from app import strategy_breakout_reset as breakout_strategy
from app import strategy_liquidity_sweep as liquidity_strategy

ROUTER_VERSION = "v3_symbol_regime_aware_router"

_STRATEGY_MAP = {
    "breakout_reset": breakout_strategy,
    "liquidity_sweep_reversal": liquidity_strategy,
}

# Phase 1: the global BTC regime must not be the only hard gate for Breakout + Reset.
# Keep a kill-switch so production can be rolled back immediately from env if needed.
ROUTER_ALLOW_BREAKOUT_DURING_RISK_OFF = str(
    os.getenv("ROUTER_ALLOW_BREAKOUT_DURING_RISK_OFF", "true")
).strip().lower() in {"1", "true", "yes", "on"}
ROUTER_ALLOW_BREAKOUT_FALLBACK_AFTER_SWEEP = str(
    os.getenv("ROUTER_ALLOW_BREAKOUT_FALLBACK_AFTER_SWEEP", "true")
).strip().lower() in {"1", "true", "yes", "on"}


def _strategy_call_kwargs(
    strategy_module,
    *,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    reference_market_price: Optional[float],
    debug_counts: Optional[Dict[str, int]],
    symbol: Optional[str] = None,
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
    if "symbol" in parameters:
        kwargs["symbol"] = symbol
    return kwargs



def _record_reject(debug_counts: Optional[Dict[str, int]], reason: str) -> None:
    if debug_counts is None:
        return
    key = str(reason or "unknown").strip() or "unknown"
    debug_counts[key] = int(debug_counts.get(key, 0)) + 1



def _snapshot_state(snapshot: Dict) -> str:
    return str(snapshot.get("state") or "unknown").strip().lower() or "unknown"



def _explicit_strategy(snapshot: Dict) -> str:
    return str(snapshot.get("strategy_name") or "").strip().lower()



def _risk_severity(snapshot: Dict) -> str:
    # Phase 2 will provide this field directly from regime_engine. Until then, unknown
    # must be treated as overrideable, not as a guaranteed hard shutdown.
    value = str(snapshot.get("risk_severity") or snapshot.get("severity") or "unknown").strip().lower()
    return value or "unknown"


def _symbol_state(symbol_regime: Optional[Dict]) -> str:
    return str((symbol_regime or {}).get("state") or "symbol_unknown").strip().lower() or "symbol_unknown"


def _symbol_allows_breakout(symbol_regime: Optional[Dict]) -> bool:
    if not symbol_regime:
        return True
    return bool(symbol_regime.get("allow_breakout", True))


def _symbol_bias(symbol_regime: Optional[Dict]) -> str:
    return str((symbol_regime or {}).get("bias") or "neutral").strip().lower() or "neutral"



def _is_risk_off_hard_block(snapshot: Dict) -> bool:
    severity = _risk_severity(snapshot)
    if severity in {"severe", "critical", "hard", "blocked"}:
        return True
    if not ROUTER_ALLOW_BREAKOUT_DURING_RISK_OFF:
        return True
    return False



def select_strategy_name(market_regime: Optional[Dict], symbol_regime: Optional[Dict] = None) -> str:
    snapshot = dict(market_regime or {})
    state = _snapshot_state(snapshot)
    explicit_strategy = _explicit_strategy(snapshot)
    local_state = _symbol_state(symbol_regime)

    if state == "risk_off":
        return "risk_off" if _is_risk_off_hard_block(snapshot) else "breakout_reset"

    # Phase 3: the local symbol regime can override BTC/global sweep routing.
    # A symbol already in clean continuation should let Breakout + Reset compete
    # instead of being forced into the liquidity-sweep strategy only.
    if local_state == "symbol_continuation_clean":
        return "breakout_reset"

    if explicit_strategy in _STRATEGY_MAP:
        return explicit_strategy
    if explicit_strategy == "risk_off":
        return "risk_off" if _is_risk_off_hard_block(snapshot) else "breakout_reset"

    if state == "continuation_clean":
        return "breakout_reset"
    if state == "sweep_reversal":
        return "liquidity_sweep_reversal"
    return "breakout_reset"



def _call_strategy(
    strategy_name: str,
    *,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    reference_market_price: Optional[float],
    debug_counts: Optional[Dict[str, int]],
    symbol: Optional[str] = None,
) -> Tuple[Optional[Dict], str, object]:
    strategy_module = _STRATEGY_MAP.get(strategy_name, breakout_strategy)
    strategy_kwargs = _strategy_call_kwargs(
        strategy_module,
        df_1h=df_1h,
        df_15m=df_15m,
        df_5m=df_5m,
        reference_market_price=reference_market_price,
        debug_counts=debug_counts,
        symbol=symbol,
    )
    return strategy_module.mtf_strategy(**strategy_kwargs), strategy_name, strategy_module



def route_candidate(
    *,
    symbol: str,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    market_regime: Optional[Dict],
    symbol_regime: Optional[Dict] = None,
    reference_market_price: Optional[float] = None,
    debug_counts: Optional[Dict[str, int]] = None,
) -> Optional[Dict]:
    snapshot = dict(market_regime or {})
    state = _snapshot_state(snapshot)
    bias = str(snapshot.get("bias") or "neutral").strip().lower()
    reason = str(snapshot.get("reason") or "market_regime_unknown")
    raw_reason = str(snapshot.get("raw_reason") or reason)
    local_regime = dict(symbol_regime or {})
    local_state = _symbol_state(local_regime)
    local_bias = _symbol_bias(local_regime)
    selected_strategy = select_strategy_name(snapshot, local_regime)
    router_override = None

    if selected_strategy == "breakout_reset" and not _symbol_allows_breakout(local_regime):
        _record_reject(debug_counts, f"symbol_regime_block_{local_state}")
        return None

    if selected_strategy == "risk_off":
        _record_reject(debug_counts, "market_regime_risk_off_hard_block")
        return None

    if state == "risk_off" and selected_strategy == "breakout_reset":
        router_override = "risk_off_breakout_reset_guarded_override"
        _record_reject(debug_counts, "router_risk_off_breakout_reset_override")

    if state == "sweep_reversal" and selected_strategy == "breakout_reset" and local_state == "symbol_continuation_clean":
        router_override = "symbol_continuation_breakout_override"
        _record_reject(debug_counts, "router_symbol_continuation_breakout_override")

    if selected_strategy == "breakout_reset":
        _record_reject(debug_counts, "router_allowed_breakout_total")
        if router_override:
            _record_reject(debug_counts, "router_allowed_breakout_override")
        else:
            _record_reject(debug_counts, "router_allowed_breakout_direct")

    result, strategy_name, strategy_module = _call_strategy(
        selected_strategy,
        df_1h=df_1h,
        df_15m=df_15m,
        df_5m=df_5m,
        reference_market_price=reference_market_price,
        debug_counts=debug_counts,
        symbol=symbol,
    )

    fallback_from = None
    if (
        not result
        and selected_strategy == "liquidity_sweep_reversal"
        and ROUTER_ALLOW_BREAKOUT_FALLBACK_AFTER_SWEEP
    ):
        fallback_from = selected_strategy
        _record_reject(debug_counts, f"strategy_router_no_candidate_{selected_strategy}")
        _record_reject(debug_counts, "strategy_router_try_breakout_reset_fallback")
        _record_reject(debug_counts, "router_allowed_breakout_total")
        _record_reject(debug_counts, "router_allowed_breakout_fallback")
        result, strategy_name, strategy_module = _call_strategy(
            "breakout_reset",
            df_1h=df_1h,
            df_15m=df_15m,
            df_5m=df_5m,
            reference_market_price=reference_market_price,
            debug_counts=debug_counts,
            symbol=symbol,
        )

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
    enriched["regime_raw_reason"] = raw_reason
    enriched["regime_risk_severity"] = _risk_severity(snapshot)
    enriched["regime_strategy_selected"] = strategy_name
    enriched["regime_strategy_requested"] = selected_strategy
    enriched["symbol_regime_state"] = local_state
    enriched["symbol_regime_bias"] = local_bias
    enriched["symbol_regime_reason"] = str(local_regime.get("reason") or "symbol_regime_unknown")
    enriched["symbol_regime_score"] = int(local_regime.get("score") or 0)
    enriched["symbol_regime_version"] = str(local_regime.get("version") or "unknown")
    enriched["symbol_regime_metrics"] = dict(local_regime.get("metrics") or {})
    enriched["router_override"] = router_override
    enriched["router_fallback_from"] = fallback_from
    enriched["router_policy"] = "guarded_breakout_fallback"
    enriched.setdefault("timeframes", ["5M"] if strategy_name == "breakout_reset" else ["15M"])
    return enriched
