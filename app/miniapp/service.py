from __future__ import annotations

from datetime import datetime
from math import isfinite
from typing import Any, Dict, Iterable, List, Optional

from app.binance_api import get_futures_24h_tickers, get_radar_opportunities
from app.database import users_collection, user_signals_collection, watchlists_collection
from app.history_service import get_history_entries_for_user
from app.market import get_market_state_snapshot
from app.payment_service import get_active_payment_order_for_user
from app.plans import get_plan_catalog, get_plan_name, normalize_plan, plan_status
from app.statistics import get_performance_snapshot
from app.user_service import get_or_create_user
from app.watchlist import get_watchlist, get_watchlist_limit_for_plan
from app.signals import get_signal_analysis_for_user, get_signal_tracking_for_user


def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _label_subscription_status(value: Any) -> str:
    normalized = str(value or "free").lower().strip()
    mapping = {
        "free": "Free",
        "trial": "Trial",
        "active": "Activo",
        "expired": "Expirado",
        "banned": "Bloqueado",
    }
    return mapping.get(normalized, normalized.title())


def _label_order_status(value: Any) -> str:
    normalized = str(value or "awaiting_payment").lower().strip()
    mapping = {
        "awaiting_payment": "Esperando pago",
        "verification_in_progress": "Verificando",
        "paid_unconfirmed": "Pago sin confirmar",
        "completed": "Completado",
        "cancelled": "Cancelado",
        "expired": "Expirada",
    }
    return mapping.get(normalized, normalized.replace("_", " ").title())


def _plan_features(plan: str) -> list[str]:
    plan_value = normalize_plan(plan)
    if plan_value == "premium":
        return [
            "Señales Free + Plus + Premium",
            "Historial completo y premium",
            "Radar y mercado ampliado",
            "Seguimiento y análisis avanzado",
        ]
    if plan_value == "plus":
        return [
            "Señales Free + Plus",
            "Historial y watchlist pro",
            "Mercado y radar ampliado",
            "Cobertura operativa mejorada",
        ]
    return ["Acceso básico"]


def serialize_order_public(order: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not order:
        return None
    base_price = order.get("base_price_usdt")
    amount = order.get("amount_usdt")
    unique_delta = None
    try:
        if base_price is not None and amount is not None:
            unique_delta = round(float(amount) - float(base_price), 3)
    except Exception:
        unique_delta = None
    plan = normalize_plan(order.get("plan"))
    return {
        "order_id": order.get("order_id"),
        "plan": plan,
        "plan_name": get_plan_name(plan),
        "days": order.get("days"),
        "base_price_usdt": base_price,
        "amount_usdt": amount,
        "amount_unique_delta": unique_delta,
        "network": order.get("network"),
        "token_symbol": order.get("token_symbol"),
        "deposit_address": order.get("deposit_address"),
        "status": order.get("status"),
        "status_label": _label_order_status(order.get("status")),
        "expires_at": _iso(order.get("expires_at")),
        "created_at": _iso(order.get("created_at")),
    }


def _serialize_signal(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "signal_id": str(doc.get("signal_id") or doc.get("_id") or ""),
        "symbol": doc.get("symbol"),
        "direction": str(doc.get("direction") or "").upper(),
        "visibility": doc.get("visibility"),
        "score": doc.get("normalized_score", doc.get("score")),
        "setup_group": doc.get("setup_group"),
        "entry_price": doc.get("entry_price"),
        "status": doc.get("status") or doc.get("result") or "active",
        "resolution": doc.get("resolution"),
        "created_at": _iso(doc.get("created_at") or doc.get("signal_created_at")),
        "telegram_valid_until": _iso(doc.get("telegram_valid_until")),
        "result": doc.get("result"),
    }


def _serialize_history(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "signal_id": str(doc.get("signal_id") or doc.get("_id") or ""),
        "symbol": doc.get("symbol"),
        "direction": str(doc.get("direction") or "").upper(),
        "visibility": doc.get("visibility"),
        "score": doc.get("normalized_score", doc.get("score")),
        "setup_group": doc.get("setup_group"),
        "result": doc.get("result") or "unknown",
        "resolution": doc.get("resolution"),
        "entry_price": doc.get("entry_price"),
        "r_multiple": doc.get("r_multiple"),
        "resolution_minutes": doc.get("resolution_minutes"),
        "signal_created_at": _iso(doc.get("signal_created_at") or doc.get("created_at")),
        "evaluated_at": _iso(doc.get("evaluated_at")),
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return default
    if not isfinite(parsed):
        return default
    return parsed


def _watchlist_range_bias(position_pct: Optional[float]) -> str:
    if position_pct is None:
        return "Sin rango"
    if position_pct >= 80.0:
        return "Cerca del máximo 24h"
    if position_pct <= 20.0:
        return "Cerca del mínimo 24h"
    return "Zona media 24h"


def _watchlist_volatility_label(range_pct: float) -> str:
    if range_pct >= 12.0:
        return "Expansivo"
    if range_pct >= 6.0:
        return "Activo"
    if range_pct >= 3.0:
        return "Moderado"
    return "Calmo"


def _serialize_watchlist(symbols: Iterable[str]) -> List[Dict[str, Any]]:
    selected_order = [str(symbol).upper() for symbol in symbols if symbol]
    if not selected_order:
        return []

    selected = set(selected_order)
    tickers = get_futures_24h_tickers()
    ticker_by_symbol: Dict[str, Dict[str, Any]] = {}
    for item in tickers:
        symbol = str(item.get("symbol", "")).upper()
        if symbol and symbol in selected:
            ticker_by_symbol[symbol] = item

    rows: List[Dict[str, Any]] = []
    for symbol in selected_order:
        item = ticker_by_symbol.get(symbol)
        if not item:
            rows.append({
                "symbol": symbol,
                "last_price": 0.0,
                "change_pct": 0.0,
                "quote_volume": 0.0,
                "volume_base": 0.0,
                "trade_count": 0,
                "high_24h": 0.0,
                "low_24h": 0.0,
                "range_pct_24h": 0.0,
                "range_position_pct": None,
                "range_bias_label": "Sin datos de Binance",
                "volatility_label": "Sin datos",
                "price_change_abs": 0.0,
                "is_positive": True,
            })
            continue

        last_price = _safe_float(item.get("lastPrice"))
        change_pct = _safe_float(item.get("priceChangePercent"))
        quote_volume = _safe_float(item.get("quoteVolume"))
        volume_base = _safe_float(item.get("volume"))
        high_24h = _safe_float(item.get("highPrice"), last_price)
        low_24h = _safe_float(item.get("lowPrice"), last_price)
        trade_count = int(_safe_float(item.get("count"), 0.0))
        price_change_abs = _safe_float(item.get("priceChange"))

        if high_24h > 0 and low_24h > 0 and high_24h >= low_24h:
            range_width = max(high_24h - low_24h, 0.0)
            range_pct_24h = (range_width / low_24h * 100.0) if low_24h > 0 else 0.0
            if range_width > 0 and last_price > 0:
                range_position_pct: Optional[float] = max(0.0, min(100.0, ((last_price - low_24h) / range_width) * 100.0))
            else:
                range_position_pct = None
        else:
            range_pct_24h = 0.0
            range_position_pct = None

        rows.append({
            "symbol": symbol,
            "last_price": last_price,
            "change_pct": change_pct,
            "quote_volume": quote_volume,
            "volume_base": volume_base,
            "trade_count": trade_count,
            "high_24h": high_24h,
            "low_24h": low_24h,
            "range_pct_24h": range_pct_24h,
            "range_position_pct": range_position_pct,
            "range_bias_label": _watchlist_range_bias(range_position_pct),
            "volatility_label": _watchlist_volatility_label(range_pct_24h),
            "price_change_abs": price_change_abs,
            "is_positive": change_pct >= 0,
        })

    return rows


def _safe_call(fn, default, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        return default


def _empty_summary() -> Dict[str, Any]:
    return {
        "total": 0,
        "resolved": 0,
        "won": 0,
        "lost": 0,
        "expired": 0,
        "tp1": 0,
        "tp2": 0,
        "sl": 0,
        "winrate": 0.0,
        "profit_factor": 0.0,
        "expectancy_r": 0.0,
        "max_drawdown_r": 0.0,
    }



def _tracking_feature_tier(plan: str) -> str:
    plan_value = normalize_plan(plan)
    if plan_value == "premium":
        return "advanced"
    if plan_value == "plus":
        return "full"
    return "basic"


def _human_component_label(raw_label: Any) -> str:
    normalized = str(raw_label or "").strip().lower()
    mapping = {
        "trend_structure": "Estructura de tendencia",
        "adx_strength": "Fuerza ADX",
        "atr_quality": "Calidad ATR",
        "breakout_quality": "Calidad breakout",
        "retest_quality": "Calidad retest",
        "continuation_quality": "Continuación",
        "volume_quality": "Calidad de volumen",
        "entry_freshness": "Frescura de entrada",
        "profile_penalty": "Ajuste por perfil",
    }
    return mapping.get(normalized, str(raw_label or "—").replace("_", " ").title())


def _serialize_score_components(items: Any, *, limit: Optional[int] = None) -> list[Dict[str, Any]]:
    if not isinstance(items, list):
        return []
    rows: list[Dict[str, Any]] = []
    for raw_item in items:
        label = None
        points = None
        if isinstance(raw_item, (list, tuple)) and raw_item:
            label = raw_item[0]
            if len(raw_item) > 1:
                points = raw_item[1]
        elif isinstance(raw_item, dict):
            label = raw_item.get("label") or raw_item.get("name")
            points = raw_item.get("points")
        else:
            label = raw_item
        score_value = None
        if points is not None:
            try:
                score_value = round(float(points), 2)
            except Exception:
                score_value = None
        row = {
            "label": _human_component_label(label),
            "score": score_value,
            "tone": "positive" if (score_value or 0) >= 0 else "negative",
        }
        rows.append(row)
    if limit is not None:
        return rows[:max(0, int(limit))]
    return rows


def _component_extreme(rows: list[Dict[str, Any]], *, pick: str) -> Optional[Dict[str, Any]]:
    eligible = [row for row in rows if row.get("score") is not None]
    if not eligible:
        return None
    key_fn = lambda row: float(row.get("score") or 0.0)
    return max(eligible, key=key_fn) if pick == "max" else min(eligible, key=key_fn)


def _signal_visibility_rank(plan: Any) -> int:
    value = normalize_plan(plan)
    return {"free": 0, "plus": 1, "premium": 2}.get(value, 0)


def build_signal_detail_payload(user: Dict[str, Any], signal_id: str, *, profile_name: str = "moderado") -> Optional[Dict[str, Any]]:
    user_id = int(user.get("user_id") or 0)
    status = plan_status(user)
    effective_plan = normalize_plan(status.get("plan") or user.get("plan"))
    tier = _tracking_feature_tier(effective_plan)

    selected_profile = str(profile_name or "moderado").strip().lower()
    if selected_profile not in {"conservador", "moderado", "agresivo"}:
        selected_profile = "moderado"
    if tier == "basic":
        selected_profile = "moderado"

    tracking = get_signal_tracking_for_user(user_id, signal_id, profile_name=selected_profile)
    if not tracking:
        return None

    analysis = get_signal_analysis_for_user(user_id, signal_id, profile_name=selected_profile) or {}
    signal_row = _serialize_signal(tracking)
    visibility = normalize_plan(tracking.get("visibility") or signal_row.get("visibility"))
    score_components = _serialize_score_components(analysis.get("components"), limit=6 if tier == "advanced" else 4)
    raw_components = _serialize_score_components(analysis.get("raw_components"), limit=6) if tier == "advanced" else []
    normalized_components = _serialize_score_components(analysis.get("normalized_components"), limit=6) if tier == "advanced" else []
    strongest_component = _component_extreme(score_components, pick="max")
    weakest_component = _component_extreme(score_components, pick="min")
    take_profits = tracking.get("take_profits") or []

    tracking_payload: Dict[str, Any] = {
        "selected_profile": selected_profile,
        "state_label": tracking.get("state_label"),
        "entry_state_label": tracking.get("entry_state_label"),
        "result_label": tracking.get("result_label"),
        "recommendation": tracking.get("recommendation"),
        "current_price": tracking.get("current_price"),
        "entry_price": tracking.get("entry_price"),
        "entry_zone_low": tracking.get("entry_zone_low"),
        "entry_zone_high": tracking.get("entry_zone_high"),
        "stop_loss": tracking.get("stop_loss"),
        "take_profits": take_profits,
        "current_move_pct": tracking.get("current_move_pct"),
        "distance_to_entry_pct": tracking.get("distance_to_entry_pct"),
        "stop_distance_pct": tracking.get("stop_distance_pct"),
        "tp1_distance_pct": tracking.get("tp1_distance_pct"),
        "tp2_distance_pct": analysis.get("selected_tp2_distance_pct"),
        "progress_to_tp1_pct": tracking.get("progress_to_tp1_pct"),
        "in_entry_zone": bool(tracking.get("in_entry_zone")),
        "tp1_hit_now": bool(tracking.get("tp1_hit_now")),
        "tp2_hit_now": bool(tracking.get("tp2_hit_now")),
        "stop_hit_now": bool(tracking.get("stop_hit_now")),
        "is_operable_now": bool(tracking.get("is_operable_now")),
        "created_at": _iso(tracking.get("created_at")),
        "telegram_valid_until": _iso(tracking.get("telegram_valid_until")),
        "evaluation_valid_until": _iso(tracking.get("evaluation_valid_until") or tracking.get("valid_until")),
        "warnings": list((tracking.get("warnings") or [])[:(4 if tier == "advanced" else 2)]),
    }

    analysis_payload: Dict[str, Any] = {
        "setup_group": analysis.get("setup_group"),
        "score": analysis.get("score"),
        "normalized_score": analysis.get("normalized_score"),
        "atr_pct": analysis.get("atr_pct"),
        "timeframes": list(analysis.get("timeframes") or []),
        "strongest_component": strongest_component,
        "weakest_component": weakest_component,
        "components": score_components,
        "selected_stop_distance_pct": analysis.get("selected_stop_distance_pct"),
        "selected_tp1_distance_pct": analysis.get("selected_tp1_distance_pct"),
        "selected_tp2_distance_pct": analysis.get("selected_tp2_distance_pct"),
        "warnings": list((analysis.get("warnings") or [])[:(4 if tier == "advanced" else 2)]),
    }

    if tier in {"full", "advanced"}:
        analysis_payload.update({
            "market_validity_minutes": analysis.get("market_validity_minutes"),
            "leverage": (analysis.get("selected_profile_payload") or {}).get("leverage"),
        })

    if tier == "advanced":
        analysis_payload.update({
            "score_profile": analysis.get("score_profile"),
            "score_calibration": analysis.get("score_calibration"),
            "raw_components": raw_components,
            "normalized_components": normalized_components,
        })

    if tier == "basic":
        upgrade_hint = "Plus desbloquea estructura operativa completa y Premium añade desglose interno del scoring."
    elif tier == "full":
        upgrade_hint = "Premium añade desglose interno del scoring y componentes avanzados."
    else:
        upgrade_hint = None

    return {
        "signal": {
            **signal_row,
            "visibility_rank": _signal_visibility_rank(visibility),
            "created_at": signal_row.get("created_at") or _iso(tracking.get("created_at")),
            "visibility": visibility,
        },
        "viewer_plan": effective_plan,
        "tracking_tier": tier,
        "selected_profile": selected_profile,
        "profile_options": ["moderado"] if tier == "basic" else ["conservador", "moderado", "agresivo"],
        "tracking": tracking_payload,
        "analysis": analysis_payload,
        "upgrade_hint": upgrade_hint,
    }

def ensure_mini_app_user(*, user_id: int, username: Optional[str], telegram_language: Optional[str]) -> Dict[str, Any]:
    user, _ = get_or_create_user(
        user_id=int(user_id),
        username=username,
        telegram_language=telegram_language,
    )
    return user


def build_me_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    status = plan_status(user)
    raw_plan = normalize_plan(user.get("plan"))
    effective_plan = normalize_plan(status.get("plan") or raw_plan)
    subscription_status = str(status.get("status") or user.get("subscription_status") or "free").lower()

    if bool(user.get("banned")):
        effective_plan = raw_plan
        subscription_status = "banned"

    plan_for_display = raw_plan if raw_plan != "free" else effective_plan
    expires_at = status.get("expires") or user.get("plan_end") or user.get("trial_end")

    return {
        "user_id": int(user.get("user_id") or 0),
        "username": user.get("username"),
        "language": user.get("language") or "es",
        "plan": plan_for_display,
        "plan_name": get_plan_name(plan_for_display),
        "subscription_status": subscription_status,
        "subscription_status_label": _label_subscription_status(subscription_status),
        "days_left": int(status.get("days_left") or 0),
        "expires_at": _iso(expires_at),
        "banned": bool(user.get("banned")),
        "ref_code": user.get("ref_code"),
        "valid_referrals_total": int(user.get("valid_referrals_total") or 0),
        "reward_days_total": int(user.get("reward_days_total") or 0),
    }


def build_dashboard_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    user_id = int(user.get("user_id") or 0)
    snapshot = _safe_call(get_performance_snapshot, {}) or {}
    summary_7d = snapshot.get("summary_7d") or _empty_summary()
    summary_30d = snapshot.get("summary_30d") or _empty_summary()

    active_query = {"user_id": user_id, "telegram_valid_until": {"$gte": datetime.utcnow()}}
    active_signals = _safe_call(
        lambda: list(
            user_signals_collection()
            .find(active_query)
            .sort("created_at", -1)
            .limit(6)
        ),
        [],
    )
    latest_signals = _safe_call(
        lambda: list(
            user_signals_collection()
            .find({"user_id": user_id})
            .sort("created_at", -1)
            .limit(30)
        ),
        [],
    )
    recent_history = _safe_call(
        lambda: get_history_entries_for_user(user_id, user_plan=user.get("plan"), limit=5),
        [],
    )
    active_order = _safe_call(lambda: get_active_payment_order_for_user(user_id), None)
    watchlist_doc = _safe_call(lambda: watchlists_collection().find_one({"user_id": user_id}) or {}, {})

    signal_mix = {"free": 0, "plus": 0, "premium": 0}
    active_mix = {"free": 0, "plus": 0, "premium": 0}

    for doc in latest_signals:
        visibility = normalize_plan(doc.get("visibility"))
        if visibility in signal_mix:
            signal_mix[visibility] += 1
    for doc in active_signals:
        visibility = normalize_plan(doc.get("visibility"))
        if visibility in active_mix:
            active_mix[visibility] += 1

    active_count = _safe_call(lambda: int(user_signals_collection().count_documents(active_query)), len(active_signals))

    return {
        "summary_7d": summary_7d,
        "summary_30d": summary_30d,
        "active_signals_count": active_count,
        "recent_signals": [_serialize_signal(doc) for doc in active_signals],
        "recent_history": [_serialize_history(doc) for doc in recent_history],
        "active_payment_order": serialize_order_public(active_order),
        "watchlist_count": len(watchlist_doc.get("symbols") or []),
        "signal_mix": signal_mix,
        "active_mix": active_mix,
    }


def build_signals_payload(user: Dict[str, Any], *, limit: int = 20) -> List[Dict[str, Any]]:
    user_id = int(user.get("user_id") or 0)
    docs = list(
        user_signals_collection()
        .find({"user_id": user_id})
        .sort("created_at", -1)
        .limit(max(1, int(limit)))
    )
    return [_serialize_signal(doc) for doc in docs]


def build_history_payload(user: Dict[str, Any], *, limit: int = 20) -> List[Dict[str, Any]]:
    docs = get_history_entries_for_user(int(user.get("user_id") or 0), user_plan=user.get("plan"), limit=limit)
    return [_serialize_history(doc) for doc in docs]


def build_market_payload() -> Dict[str, Any]:
    snapshot = get_market_state_snapshot() or {}
    radar = get_radar_opportunities(limit=6)
    snapshot["radar"] = [
        {
            "symbol": row.get("symbol"),
            "score": row.get("score"),
            "direction": row.get("direction"),
            "change_pct": row.get("change_pct"),
            "last_price": row.get("last_price"),
            "momentum": row.get("momentum"),
            "quote_volume": row.get("quote_volume"),
        }
        for row in radar
    ]
    snapshot["top_gainers"] = list(snapshot.get("top_gainers") or [])[:5]
    snapshot["top_losers"] = list(snapshot.get("top_losers") or [])[:5]
    snapshot["top_volume"] = list(snapshot.get("top_volume") or [])[:5]
    snapshot["top_open_interest"] = list(snapshot.get("top_open_interest") or [])[:4]
    return snapshot


def build_watchlist_context(user: Dict[str, Any]) -> Dict[str, Any]:
    raw_symbols = get_watchlist(int(user.get("user_id") or 0))
    status = plan_status(user)
    plan_value = normalize_plan(status.get("plan") or user.get("plan"))
    max_symbols = get_watchlist_limit_for_plan(plan_value)
    symbols_count = len(raw_symbols)
    slots_left = None if max_symbols is None else max(max_symbols - symbols_count, 0)
    return {
        "items": _serialize_watchlist(raw_symbols),
        "meta": {
            "symbols": raw_symbols,
            "symbols_count": symbols_count,
            "max_symbols": max_symbols,
            "slots_left": slots_left,
            "plan": plan_value,
            "plan_name": get_plan_name(plan_value),
            "can_add_more": True if max_symbols is None else symbols_count < max_symbols,
        },
    }


def build_watchlist_payload(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    return build_watchlist_context(user)["items"]


def build_plans_payload(current_plan: Optional[str] = None) -> Dict[str, Any]:
    current_value = normalize_plan(current_plan)
    catalog = get_plan_catalog()
    enriched: Dict[str, Any] = {}
    for plan, rows in catalog.items():
        enriched[plan] = []
        for row in rows:
            item = dict(row)
            item["plan_name"] = get_plan_name(plan)
            item["is_current_plan"] = current_value == normalize_plan(plan)
            item["features"] = _plan_features(plan)
            enriched[plan].append(item)
    return enriched


def build_bootstrap_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "me": _safe_call(lambda: build_me_payload(user), {
            "user_id": int(user.get("user_id") or 0),
            "username": user.get("username"),
            "language": user.get("language") or "es",
            "plan": normalize_plan(user.get("plan")),
            "plan_name": get_plan_name(user.get("plan")),
            "subscription_status": str(user.get("subscription_status") or "free").lower(),
            "subscription_status_label": _label_subscription_status(user.get("subscription_status") or "free"),
            "days_left": 0,
            "expires_at": None,
            "banned": bool(user.get("banned")),
            "ref_code": user.get("ref_code"),
            "valid_referrals_total": int(user.get("valid_referrals_total") or 0),
            "reward_days_total": int(user.get("reward_days_total") or 0),
        }),
        "dashboard": _safe_call(lambda: build_dashboard_payload(user), {
            "summary_7d": _empty_summary(),
            "summary_30d": _empty_summary(),
            "active_signals_count": 0,
            "recent_signals": [],
            "recent_history": [],
            "active_payment_order": None,
            "watchlist_count": 0,
            "signal_mix": {"free": 0, "plus": 0, "premium": 0},
            "active_mix": {"free": 0, "plus": 0, "premium": 0},
        }),
        "signals": _safe_call(lambda: build_signals_payload(user, limit=12), []),
        "history": _safe_call(lambda: build_history_payload(user, limit=10), []),
        "market": _safe_call(build_market_payload, {
            "bias": "—",
            "regime": "—",
            "volatility": "—",
            "environment": "—",
            "recommendation": "Sin datos de mercado por ahora.",
            "top_gainers": [],
            "top_losers": [],
            "top_volume": [],
            "top_open_interest": [],
            "radar": [],
            "btc": {},
            "eth": {},
            "preferred_side": "—",
            "participation": "—",
            "adv_ratio_pct": 0.0,
        }),
        "watchlist": _safe_call(lambda: build_watchlist_payload(user), []),
        "watchlist_meta": _safe_call(lambda: build_watchlist_context(user)["meta"], {"symbols": [], "symbols_count": 0, "max_symbols": 2, "slots_left": 2, "plan": "free", "plan_name": "FREE", "can_add_more": True}),
        "plans": _safe_call(lambda: build_plans_payload(user.get("plan")), {"plus": [], "premium": []}),
        "support_url": "https://chat.whatsapp.com/JXxSGjaKtqRH9c0jTlGv2l?mode=gi_t",
        "generated_at": datetime.utcnow().isoformat(),
    }


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    return users_collection().find_one({"user_id": int(user_id)})
