from __future__ import annotations

import inspect
import os
from typing import Any, Dict, Optional

import pandas as pd

from app import strategy_breakout_reset as breakout_strategy
from app import strategy_liquidity_sweep as liquidity_strategy

ROUTER_VERSION = "v3_low_cost_strategy_observability"

_STRATEGY_MAP = {
    "breakout_reset": breakout_strategy,
    "liquidity_sweep_reversal": liquidity_strategy,
}

# Special debug buckets consumed by scanner.py. They are intentionally stored in the
# existing debug_counts object so we do not add extra DB reads, network requests or
# per-symbol persistent payloads.
DEBUG_STRATEGY_ATTEMPTS_KEY = "__strategy_attempts__"
DEBUG_STRATEGY_REJECTIONS_KEY = "__strategy_rejections__"
DEBUG_STRATEGY_TERMINAL_REASONS_KEY = "__strategy_terminal_reasons__"
DEBUG_ROUTER_EVENTS_KEY = "__router_events__"

ROUTER_TERMINAL_RISK_OFF = str(os.getenv("MARKET_REGIME_TERMINAL_RISK_OFF", "false")).strip().lower() in {"1", "true", "yes", "on"}
ROUTER_TRY_ALTERNATE_STRATEGY = str(os.getenv("ROUTER_TRY_ALTERNATE_STRATEGY", "true")).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_strategy_key(strategy_name: Optional[str]) -> str:
    normalized = str(strategy_name or "").strip().lower()
    if normalized in {"breakout_reset", "strategy_breakout_reset", "breakout+reset"}:
        return "breakout_reset"
    if normalized in {"liquidity_sweep_reversal", "strategy_liquidity_sweep", "liquidity_sweep", "liquidity_hunter"}:
        return "liquidity_sweep_reversal"
    if normalized == "risk_off":
        return "risk_off"
    return normalized or "legacy_unknown"


def _strategy_call_kwargs(
    strategy_module,
    *,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    reference_market_price: Optional[float],
    debug_counts: Optional[Dict[str, int]],
) -> Dict:
    kwargs: Dict[str, Any] = {
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


def _increment_flat(debug_counts: Optional[Dict[str, Any]], key: str, amount: int = 1) -> None:
    if debug_counts is None:
        return
    normalized = str(key or "unknown").strip() or "unknown"
    try:
        debug_counts[normalized] = int(debug_counts.get(normalized, 0)) + int(amount or 0)
    except Exception:
        debug_counts[normalized] = int(amount or 0)


def _increment_nested(debug_counts: Optional[Dict[str, Any]], bucket_name: str, key: str, amount: int = 1) -> None:
    if debug_counts is None:
        return
    normalized = _normalize_strategy_key(key)
    bucket = debug_counts.setdefault(bucket_name, {})
    if not isinstance(bucket, dict):
        bucket = {}
        debug_counts[bucket_name] = bucket
    bucket[normalized] = int(bucket.get(normalized, 0)) + int(amount or 0)


def _increment_reason_nested(
    debug_counts: Optional[Dict[str, Any]],
    strategy_name: str,
    reason: str,
    amount: int = 1,
) -> None:
    if debug_counts is None:
        return
    strategy_key = _normalize_strategy_key(strategy_name)
    reason_key = str(reason or "unknown").strip() or "unknown"
    bucket = debug_counts.setdefault(DEBUG_STRATEGY_TERMINAL_REASONS_KEY, {})
    if not isinstance(bucket, dict):
        bucket = {}
        debug_counts[DEBUG_STRATEGY_TERMINAL_REASONS_KEY] = bucket
    per_strategy = bucket.setdefault(strategy_key, {})
    if not isinstance(per_strategy, dict):
        per_strategy = {}
        bucket[strategy_key] = per_strategy
    per_strategy[reason_key] = int(per_strategy.get(reason_key, 0)) + int(amount or 0)


def _record_reject(debug_counts: Optional[Dict[str, int]], reason: str) -> None:
    _increment_flat(debug_counts, reason, 1)


def _record_router_event(debug_counts: Optional[Dict[str, Any]], event: str) -> None:
    if debug_counts is None:
        return
    key = str(event or "unknown").strip() or "unknown"
    bucket = debug_counts.setdefault(DEBUG_ROUTER_EVENTS_KEY, {})
    if not isinstance(bucket, dict):
        bucket = {}
        debug_counts[DEBUG_ROUTER_EVENTS_KEY] = bucket
    bucket[key] = int(bucket.get(key, 0)) + 1


def _merge_strategy_debug(
    parent_debug: Optional[Dict[str, Any]],
    strategy_name: str,
    strategy_debug: Optional[Dict[str, int]],
) -> Optional[str]:
    """Merge strategy debug counts into flat legacy counters and structured buckets.

    Returns the terminal reason inferred from the strategy-local debug map. This is
    intentionally cheap: no per-symbol records are persisted, only aggregate counts.
    """
    if parent_debug is None or not strategy_debug:
        return None

    terminal_reason: Optional[str] = None
    terminal_count = -1
    for reason, count in strategy_debug.items():
        if str(reason).startswith("__"):
            continue
        try:
            numeric_count = int(count or 0)
        except Exception:
            continue
        if numeric_count <= 0:
            continue
        _increment_flat(parent_debug, reason, numeric_count)
        if numeric_count > terminal_count:
            terminal_count = numeric_count
            terminal_reason = str(reason)

    if terminal_reason:
        _increment_nested(parent_debug, DEBUG_STRATEGY_REJECTIONS_KEY, strategy_name, 1)
        _increment_reason_nested(parent_debug, strategy_name, terminal_reason, 1)
    return terminal_reason


def _run_strategy(
    *,
    strategy_name: str,
    strategy_module,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    reference_market_price: Optional[float],
    parent_debug_counts: Optional[Dict[str, Any]],
) -> Optional[Dict]:
    strategy_key = _normalize_strategy_key(strategy_name)
    _increment_nested(parent_debug_counts, DEBUG_STRATEGY_ATTEMPTS_KEY, strategy_key, 1)
    strategy_debug: Dict[str, int] = {}
    strategy_kwargs = _strategy_call_kwargs(
        strategy_module,
        df_1h=df_1h,
        df_15m=df_15m,
        df_5m=df_5m,
        reference_market_price=reference_market_price,
        debug_counts=strategy_debug,
    )
    result = strategy_module.mtf_strategy(**strategy_kwargs)
    if not result:
        _merge_strategy_debug(parent_debug_counts, strategy_key, strategy_debug)
    else:
        # Preserve non-terminal warnings if a strategy ever records debug counts and still returns a candidate.
        _merge_strategy_debug(parent_debug_counts, strategy_key, strategy_debug)
    return result


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
    primary_strategy_name = select_strategy_name(snapshot)
    selected_strategy_name = primary_strategy_name

    _record_router_event(debug_counts, f"primary_{_normalize_strategy_key(primary_strategy_name)}")

    if primary_strategy_name == "risk_off" or (state == "risk_off" and ROUTER_TERMINAL_RISK_OFF):
        _record_reject(debug_counts, "market_regime_terminal_risk_off")
        _record_router_event(debug_counts, "terminal_risk_off")
        return None

    if state == "risk_off" and primary_strategy_name != "liquidity_sweep_reversal":
        primary_strategy_name = "liquidity_sweep_reversal"
        selected_strategy_name = primary_strategy_name
        _record_reject(debug_counts, "market_regime_risk_off_rerouted_to_liquidity")
        _record_router_event(debug_counts, "risk_off_rerouted_to_liquidity")

    strategy_module = _STRATEGY_MAP.get(primary_strategy_name, breakout_strategy)
    result = _run_strategy(
        strategy_name=primary_strategy_name,
        strategy_module=strategy_module,
        df_1h=df_1h,
        df_15m=df_15m,
        df_5m=df_5m,
        reference_market_price=reference_market_price,
        parent_debug_counts=debug_counts,
    )
    selected_strategy_module = strategy_module
    fallback_used = False

    if not result and ROUTER_TRY_ALTERNATE_STRATEGY:
        alternate_name = "liquidity_sweep_reversal" if primary_strategy_name == "breakout_reset" else "breakout_reset"
        alternate_module = _STRATEGY_MAP.get(alternate_name, breakout_strategy)
        _record_router_event(debug_counts, f"fallback_attempt_{_normalize_strategy_key(primary_strategy_name)}_to_{_normalize_strategy_key(alternate_name)}")
        alternate_result = _run_strategy(
            strategy_name=alternate_name,
            strategy_module=alternate_module,
            df_1h=df_1h,
            df_15m=df_15m,
            df_5m=df_5m,
            reference_market_price=reference_market_price,
            parent_debug_counts=debug_counts,
        )
        if alternate_result:
            _record_reject(debug_counts, f"strategy_router_fallback_from_{primary_strategy_name}_to_{alternate_name}")
            _record_router_event(debug_counts, f"fallback_success_{_normalize_strategy_key(primary_strategy_name)}_to_{_normalize_strategy_key(alternate_name)}")
            result = alternate_result
            selected_strategy_name = alternate_name
            selected_strategy_module = alternate_module
            fallback_used = True
        else:
            _record_router_event(debug_counts, f"fallback_empty_{_normalize_strategy_key(primary_strategy_name)}_to_{_normalize_strategy_key(alternate_name)}")

    if not result:
        _record_reject(debug_counts, f"strategy_router_no_candidate_{primary_strategy_name}")
        _record_router_event(debug_counts, f"no_candidate_{_normalize_strategy_key(primary_strategy_name)}")
        return None

    enriched = dict(result)
    selected_key = _normalize_strategy_key(selected_strategy_name)
    primary_key = _normalize_strategy_key(primary_strategy_name)
    enriched["strategy_name"] = selected_key
    enriched["strategy_version"] = str(getattr(selected_strategy_module, "SCORE_CALIBRATION_VERSION", "unknown"))
    enriched["router_version"] = ROUTER_VERSION
    enriched["regime_state"] = state
    enriched["regime_bias"] = bias
    enriched["regime_reason"] = reason
    enriched["regime_strategy_selected"] = selected_key
    enriched["router_primary_strategy"] = primary_key
    enriched["router_fallback_used"] = bool(fallback_used)
    enriched.setdefault("timeframes", ["5M"] if selected_key == "breakout_reset" else ["15M"])
    return enriched
