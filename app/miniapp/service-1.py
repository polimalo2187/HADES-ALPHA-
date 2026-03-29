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
    snapshot = get_performance_snapshot()
    active_query = {"user_id": user_id, "telegram_valid_until": {"$gte": datetime.utcnow()}}
    active_signals = list(
        user_signals_collection()
        .find(active_query)
        .sort("created_at", -1)
        .limit(6)
    )
    latest_signals = list(
        user_signals_collection()
        .find({"user_id": user_id})
        .sort("created_at", -1)
        .limit(30)
    )
    recent_history = get_history_entries_for_user(user_id, user_plan=user.get("plan"), limit=5)
    active_order = get_active_payment_order_for_user(user_id)
    watchlist_doc = watchlists_collection().find_one({"user_id": user_id}) or {}
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

    return {
        "summary_7d": snapshot.get("summary_7d", {}),
        "summary_30d": snapshot.get("summary_30d", {}),
        "active_signals_count": int(user_signals_collection().count_documents(active_query)),
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


def build_watchlist_payload(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    doc = watchlists_collection().find_one({"user_id": int(user.get("user_id") or 0)}) or {}
    return _serialize_watchlist(doc.get("symbols") or [])


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
        "me": build_me_payload(user),
        "dashboard": build_dashboard_payload(user),
        "signals": build_signals_payload(user, limit=12),
        "history": build_history_payload(user, limit=10),
        "market": build_market_payload(),
        "watchlist": build_watchlist_payload(user),
        "plans": build_plans_payload(user.get("plan")),
        "support_url": "https://chat.whatsapp.com/JXxSGjaKtqRH9c0jTlGv2l?mode=gi_t",
        "generated_at": datetime.utcnow().isoformat(),
    }


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    return users_collection().find_one({"user_id": int(user_id)})
