from __future__ import annotations

from datetime import datetime
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


def _serialize_watchlist(symbols: Iterable[str]) -> List[Dict[str, Any]]:
    selected = {str(symbol).upper() for symbol in symbols if symbol}
    if not selected:
        return []
    tickers = get_futures_24h_tickers()
    rows: List[Dict[str, Any]] = []
    for item in tickers:
        symbol = str(item.get("symbol", "")).upper()
        if symbol not in selected:
            continue
        try:
            last_price = float(item.get("lastPrice", 0.0))
        except Exception:
            last_price = 0.0
        try:
            change_pct = float(item.get("priceChangePercent", 0.0))
        except Exception:
            change_pct = 0.0
        try:
            quote_volume = float(item.get("quoteVolume", 0.0))
        except Exception:
            quote_volume = 0.0
        rows.append({
            "symbol": symbol,
            "last_price": last_price,
            "change_pct": change_pct,
            "quote_volume": quote_volume,
            "is_positive": change_pct >= 0,
        })
    rows.sort(key=lambda row: abs(row["change_pct"]), reverse=True)
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
