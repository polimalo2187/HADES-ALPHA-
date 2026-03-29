from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from app.database import (
    signal_results_collection,
    signals_collection,
    signal_history_collection,
    stats_snapshots_collection,
    user_signals_collection,
)
from app.models import new_stats_snapshot


MATERIALIZED_WINDOWS_DAYS = (1, 7, 30)
MATERIALIZED_MAX_AGE_SECONDS = 15 * 60


# ======================================================
# UTILIDADES DE FECHAS
# ======================================================


def _start_of_day(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day)



def _start_of_week(dt: datetime) -> datetime:
    start = dt - timedelta(days=dt.weekday())
    return datetime(start.year, start.month, start.day)



def _start_of_month(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1)


# ======================================================
# HELPERS
# ======================================================


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None



def _safe_dt(value: Any) -> Optional[datetime]:
    return value if isinstance(value, datetime) else None



def _result_plan(result_doc: Dict[str, Any]) -> str:
    return str(result_doc.get("plan") or result_doc.get("visibility") or "").lower()



def _signal_plan(signal_doc: Dict[str, Any]) -> str:
    return str(signal_doc.get("visibility") or signal_doc.get("plan") or "").lower()



def _score_value(result_doc: Dict[str, Any]) -> Optional[float]:
    return _safe_float(result_doc.get("normalized_score", result_doc.get("score")))



def _resolution_key(result_doc: Dict[str, Any]) -> str:
    resolution = str(result_doc.get("resolution") or "").lower().strip()
    if resolution:
        return resolution
    outcome = str(result_doc.get("result") or "").lower().strip()
    return outcome



def _is_win_result(result_doc: Dict[str, Any]) -> bool:
    return _resolution_key(result_doc) in {"tp1", "tp2", "won"}



def _is_loss_result(result_doc: Dict[str, Any]) -> bool:
    return _resolution_key(result_doc) in {"sl", "lost"}



def _is_expired_clean(result_doc: Dict[str, Any]) -> bool:
    return _resolution_key(result_doc) in {"expired", "expired_clean"}



def _resolved_r_bucket(result_doc: Dict[str, Any]) -> Optional[str]:
    resolution = _resolution_key(result_doc)
    if resolution in {"tp1", "tp2", "sl"}:
        return resolution
    if resolution == "won":
        r_multiple = _r_multiple_value(result_doc)
        if r_multiple is None:
            return "tp1"
        return "tp2" if r_multiple >= 1.5 else "tp1"
    if resolution == "lost":
        return "sl"
    return None



def _r_multiple_value(result_doc: Dict[str, Any]) -> Optional[float]:
    resolution = _resolution_key(result_doc)
    if resolution == "tp1":
        return 1.0
    if resolution == "tp2":
        return 2.0
    if resolution == "sl":
        return -1.0
    if resolution in {"expired", "expired_clean"}:
        return None

    value = _safe_float(result_doc.get("r_multiple"))
    if value is not None:
        outcome = str(result_doc.get("result") or "").lower().strip()
        if outcome == "won":
            return 2.0 if value >= 1.5 else 1.0
        if outcome == "lost":
            return -1.0
        return None

    outcome = str(result_doc.get("result") or "").lower().strip()
    if outcome == "lost":
        return -1.0
    if outcome == "won":
        reward_pct = _safe_float(result_doc.get("reward_pct"))
        risk_pct = _safe_float(result_doc.get("risk_pct"))
        if reward_pct is not None and risk_pct not in (None, 0):
            ratio = reward_pct / risk_pct
            return 2.0 if ratio >= 1.5 else 1.0
    return None



def _max_drawdown_from_r(results: Iterable[Dict[str, Any]]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0

    ordered = sorted(
        results,
        key=lambda r: _safe_dt(r.get("evaluated_at")) or datetime.min,
    )

    for result in ordered:
        if not (_is_win_result(result) or _is_loss_result(result)):
            continue
        r_multiple = _r_multiple_value(result)
        if r_multiple is None:
            continue
        cumulative += float(r_multiple)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)

    return round(max_drawdown, 4)



def _calculate_stats_from_results(results: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = list(results)

    total = 0
    won = 0
    lost = 0
    expired = 0
    tp1_hits = 0
    tp2_hits = 0
    sl_hits = 0
    gross_profit_r = 0.0
    gross_loss_r = 0.0
    total_r = 0.0
    resolved_r_count = 0
    resolution_minutes_total = 0.0
    resolution_minutes_count = 0

    for row in rows:
        total += 1
        bucket = _resolved_r_bucket(row)
        if _is_win_result(row):
            won += 1
            if bucket == "tp2":
                tp2_hits += 1
            else:
                tp1_hits += 1
        elif _is_loss_result(row):
            lost += 1
            sl_hits += 1
        elif _is_expired_clean(row):
            expired += 1

        r_multiple = _r_multiple_value(row)
        if (_is_win_result(row) or _is_loss_result(row)) and r_multiple is not None:
            resolved_r_count += 1
            total_r += r_multiple
            if r_multiple > 0:
                gross_profit_r += r_multiple
            elif r_multiple < 0:
                gross_loss_r += abs(r_multiple)

        resolution_minutes = _safe_float(row.get("resolution_minutes"))
        if resolution_minutes is not None:
            resolution_minutes_total += resolution_minutes
            resolution_minutes_count += 1

    resolved = won + lost
    winrate = round((won / resolved) * 100, 2) if resolved > 0 else 0.0
    loss_rate = round((lost / resolved) * 100, 2) if resolved > 0 else 0.0
    expiry_rate = round((expired / total) * 100, 2) if total > 0 else 0.0

    if gross_loss_r > 0:
        profit_factor = round(gross_profit_r / gross_loss_r, 2)
    elif gross_profit_r > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    expectancy_r = round((total_r / resolved_r_count), 4) if resolved_r_count > 0 else 0.0
    avg_resolution_minutes = round((resolution_minutes_total / resolution_minutes_count), 2) if resolution_minutes_count > 0 else None
    max_drawdown_r = _max_drawdown_from_r(rows)

    return {
        "total": total,
        "won": won,
        "lost": lost,
        "expired": expired,
        "resolved": resolved,
        "tp1": tp1_hits,
        "tp2": tp2_hits,
        "sl": sl_hits,
        "winrate": winrate,
        "loss_rate": loss_rate,
        "expiry_rate": expiry_rate,
        "gross_profit_r": round(gross_profit_r, 4),
        "gross_loss_r": round(gross_loss_r, 4),
        "net_r": round(total_r, 4),
        "profit_factor": profit_factor,
        "expectancy_r": expectancy_r,
        "max_drawdown_r": max_drawdown_r,
        "avg_resolution_minutes": avg_resolution_minutes,
    }



def _activity_stats_from_signals(signals: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    total = 0
    score_sum = 0.0
    score_n = 0

    for signal in signals:
        total += 1
        score = _safe_float(signal.get("normalized_score", signal.get("score")))
        if score is not None:
            score_sum += score
            score_n += 1

    avg_score = round(score_sum / score_n, 2) if score_n > 0 else "—"
    return {
        "signals_total": total,
        "avg_score": avg_score,
    }



def _fetch_results(from_date: datetime, extra_query: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {
        "evaluated_at": {"$gte": from_date},
        "evaluation_scope": "base",
    }
    if extra_query:
        query.update(extra_query)
    return list(signal_results_collection().find(query))



def _fetch_signals(from_date: datetime, extra_query: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {"created_at": {"$gte": from_date}}
    if extra_query:
        query.update(extra_query)
    return list(signals_collection().find(query))



def _build_score_buckets(
    results: Iterable[Dict[str, Any]],
    buckets: Optional[List[tuple[int, int, str]]] = None,
) -> Dict[str, Any]:
    if buckets is None:
        buckets = [
            (0, 70, "<70"),
            (70, 80, "70–79"),
            (80, 90, "80–89"),
            (90, 101, "90+"),
        ]

    rows = list(results)
    output = []
    for lo, hi, label in buckets:
        won = 0
        lost = 0
        net_r = 0.0
        for result in rows:
            score = _score_value(result)
            if score is None:
                continue
            if lo <= score < hi:
                if result.get("result") == "won":
                    won += 1
                elif result.get("result") == "lost":
                    lost += 1
                r_multiple = _r_multiple_value(result)
                if r_multiple is not None:
                    net_r += r_multiple

        n = won + lost
        winrate = round((won / n) * 100, 2) if n > 0 else 0.0
        output.append({"label": label, "winrate": winrate, "n": n, "won": won, "lost": lost, "net_r": round(net_r, 4)})

    return {"buckets": output}



def _build_direction_stats(results: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for result in results:
        direction = str(result.get("direction") or "?").upper()
        grouped[direction].append(result)

    rows = []
    for direction in ("LONG", "SHORT"):
        stats = _calculate_stats_from_results(grouped.get(direction, []))
        rows.append(
            {
                "direction": direction,
                "resolved": stats["resolved"],
                "won": stats["won"],
                "lost": stats["lost"],
                "expired": stats["expired"],
                "winrate": stats["winrate"],
                "profit_factor": stats["profit_factor"],
                "expectancy_r": stats["expectancy_r"],
            }
        )
    return rows



def _build_symbol_diagnostics(
    results: Iterable[Dict[str, Any]],
    *,
    min_resolved: int = 3,
    limit: int = 3,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for result in results:
        symbol = str(result.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        grouped[symbol].append(result)

    rows = []
    for symbol, symbol_results in grouped.items():
        stats = _calculate_stats_from_results(symbol_results)
        if stats["resolved"] < min_resolved:
            continue
        rows.append(
            {
                "symbol": symbol,
                "resolved": stats["resolved"],
                "won": stats["won"],
                "lost": stats["lost"],
                "expired": stats["expired"],
                "winrate": stats["winrate"],
                "loss_rate": stats["loss_rate"],
                "profit_factor": stats["profit_factor"],
                "expectancy_r": stats["expectancy_r"],
            }
        )

    rows.sort(key=lambda row: (row["winrate"], -row["resolved"], -row["lost"], row["symbol"]))
    return rows[:limit]



def _build_setup_group_stats(results: Iterable[Dict[str, Any]], signals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signal_by_id = {str(s.get("_id")): s for s in signals if s.get("_id") is not None}
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for result in results:
        group = str(result.get("setup_group") or "").lower().strip()
        if not group:
            signal = signal_by_id.get(str(result.get("base_signal_id")))
            group = str((signal or {}).get("setup_group") or "").lower().strip()
        if not group:
            continue
        grouped[group].append(result)

    rows = []
    for group_name, group_results in grouped.items():
        stats = _calculate_stats_from_results(group_results)
        rows.append(
            {
                "setup_group": group_name,
                "resolved": stats["resolved"],
                "won": stats["won"],
                "lost": stats["lost"],
                "expired": stats["expired"],
                "winrate": stats["winrate"],
                "profit_factor": stats["profit_factor"],
                "expectancy_r": stats["expectancy_r"],
            }
        )

    rows.sort(key=lambda row: (row["setup_group"]))
    return rows



def _build_diagnostics_summary(results: List[Dict[str, Any]], signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = _calculate_stats_from_results(results)
    result_scores = [_score_value(result) for result in results]
    valid_scores = [score for score in result_scores if score is not None]
    avg_result_score = round(sum(valid_scores) / len(valid_scores), 2) if valid_scores else "—"

    pending_to_evaluate = signals_collection().count_documents({"evaluated": {"$ne": True}})

    return {
        "evaluated_total": stats["total"],
        "resolved_total": stats["resolved"],
        "won": stats["won"],
        "lost": stats["lost"],
        "expired": stats["expired"],
        "winrate": stats["winrate"],
        "loss_rate": stats["loss_rate"],
        "expiry_rate": stats["expiry_rate"],
        "scanner_signals_total": len(signals),
        "pending_to_evaluate": pending_to_evaluate,
        "avg_result_score": avg_result_score,
        "profit_factor": stats["profit_factor"],
        "expectancy_r": stats["expectancy_r"],
        "max_drawdown_r": stats["max_drawdown_r"],
        "avg_resolution_minutes": stats["avg_resolution_minutes"],
    }


# ======================================================
# MATERIALIZED SNAPSHOTS
# ======================================================


def _window_key(days: int) -> str:
    return f"performance:{int(days)}d"



def build_performance_window(days: int) -> Dict[str, Any]:
    now = datetime.utcnow()
    from_date = now - timedelta(days=days)
    results = _fetch_results(from_date)
    signals = _fetch_signals(from_date)

    by_plan = {
        "free": _calculate_stats_from_results(r for r in results if _result_plan(r) == "free"),
        "plus": _calculate_stats_from_results(r for r in results if _result_plan(r) == "plus"),
        "premium": _calculate_stats_from_results(r for r in results if _result_plan(r) == "premium"),
    }
    activity_by_plan = {
        "free": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "free"),
        "plus": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "plus"),
        "premium": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "premium"),
    }

    return {
        "days": days,
        "summary": _calculate_stats_from_results(results),
        "activity": _activity_stats_from_signals(signals),
        "by_plan": by_plan,
        "activity_by_plan": activity_by_plan,
        "by_score": {"days": days, **_build_score_buckets(results)},
        "direction": _build_direction_stats(results),
        "worst_symbols": _build_symbol_diagnostics(results, min_resolved=3, limit=3),
        "setup_groups": _build_setup_group_stats(results, signals),
        "diagnostics": _build_diagnostics_summary(results, signals),
        "computed_for_range": {
            "from": from_date,
            "to": now,
        },
    }



def refresh_materialized_stats(windows: Iterable[int] = MATERIALIZED_WINDOWS_DAYS) -> int:
    collection = stats_snapshots_collection()
    processed = 0

    for window_days in windows:
        payload = build_performance_window(int(window_days))
        snapshot = new_stats_snapshot(
            key=_window_key(int(window_days)),
            window_days=int(window_days),
            payload=payload,
        )
        collection.update_one(
            {"key": snapshot["key"]},
            {
                "$set": {
                    "window_days": snapshot["window_days"],
                    "payload": snapshot["payload"],
                    "computed_at": snapshot["computed_at"],
                    "schema_version": snapshot["schema_version"],
                    "updated_at": snapshot["updated_at"],
                },
                "$setOnInsert": {
                    "created_at": snapshot["created_at"],
                },
            },
            upsert=True,
        )
        processed += 1

    return processed



def get_materialized_window(days: int, *, max_age_seconds: int = MATERIALIZED_MAX_AGE_SECONDS) -> Optional[Dict[str, Any]]:
    doc = stats_snapshots_collection().find_one({"key": _window_key(days)})
    if not doc:
        return None
    computed_at = _safe_dt(doc.get("computed_at"))
    if not computed_at:
        return None
    age_seconds = (datetime.utcnow() - computed_at).total_seconds()
    if age_seconds > max_age_seconds:
        return None
    return doc.get("payload") if isinstance(doc.get("payload"), dict) else None


# ======================================================
# RESET DE ESTADÍSTICAS
# ======================================================


def reset_statistics(preserve_signals: bool = False) -> Dict[str, Any]:
    if preserve_signals:
        deleted_results = signal_results_collection().delete_many({}).deleted_count
        deleted_history = signal_history_collection().delete_many({}).deleted_count
        deleted_snapshots = stats_snapshots_collection().delete_many({}).deleted_count

        base_modified = signals_collection().update_many(
            {},
            {
                "$set": {"evaluated": False},
                "$unset": {
                    "result": "",
                    "evaluated_at": "",
                    "evaluated_profile": "",
                },
            },
        ).modified_count

        user_modified = user_signals_collection().update_many(
            {},
            {
                "$set": {"evaluated": False},
                "$unset": {
                    "result": "",
                    "evaluated_at": "",
                    "evaluated_profile": "",
                },
            },
        ).modified_count

        return {
            "mode": "results_only",
            "deleted_results": deleted_results,
            "deleted_history": deleted_history,
            "deleted_snapshots": deleted_snapshots,
            "reset_base_signals": base_modified,
            "reset_user_signals": user_modified,
        }

    deleted_base = signals_collection().delete_many({}).deleted_count
    deleted_user = user_signals_collection().delete_many({}).deleted_count
    deleted_results = signal_results_collection().delete_many({}).deleted_count
    deleted_history = signal_history_collection().delete_many({}).deleted_count
    deleted_snapshots = stats_snapshots_collection().delete_many({}).deleted_count
    return {
        "mode": "full_reset",
        "deleted_base_signals": deleted_base,
        "deleted_user_signals": deleted_user,
        "deleted_results": deleted_results,
        "deleted_history": deleted_history,
        "deleted_snapshots": deleted_snapshots,
    }


# ======================================================
# ESTADÍSTICAS LEGACY / COMPAT
# ======================================================


def get_daily_stats() -> Dict[str, Any]:
    materialized = get_materialized_window(1)
    if materialized:
        return materialized.get("summary", {})
    now = datetime.utcnow()
    return _calculate_stats_from_results(_fetch_results(_start_of_day(now)))



def get_weekly_stats() -> Dict[str, Any]:
    now = datetime.utcnow()
    return _calculate_stats_from_results(_fetch_results(_start_of_week(now)))



def get_monthly_stats() -> Dict[str, Any]:
    now = datetime.utcnow()
    return _calculate_stats_from_results(_fetch_results(_start_of_month(now)))



def get_last_days_stats(days: int) -> Dict[str, Any]:
    materialized = get_materialized_window(days)
    if materialized:
        return materialized.get("summary", {})
    now = datetime.utcnow()
    return _calculate_stats_from_results(_fetch_results(now - timedelta(days=days)))



def get_last_days_stats_by_plan(days: int) -> Dict[str, Any]:
    materialized = get_materialized_window(days)
    if materialized:
        return materialized.get("by_plan", {})
    now = datetime.utcnow()
    from_date = now - timedelta(days=days)
    results = _fetch_results(from_date)

    return {
        "free": _calculate_stats_from_results(r for r in results if _result_plan(r) == "free"),
        "plus": _calculate_stats_from_results(r for r in results if _result_plan(r) == "plus"),
        "premium": _calculate_stats_from_results(r for r in results if _result_plan(r) == "premium"),
    }



def get_signal_activity_stats(days: int) -> Dict[str, Any]:
    materialized = get_materialized_window(days)
    if materialized:
        return materialized.get("activity", {})
    now = datetime.utcnow()
    return _activity_stats_from_signals(_fetch_signals(now - timedelta(days=days)))



def get_signal_activity_stats_by_plan(days: int) -> Dict[str, Any]:
    materialized = get_materialized_window(days)
    if materialized:
        return materialized.get("activity_by_plan", {})
    now = datetime.utcnow()
    from_date = now - timedelta(days=days)
    signals = _fetch_signals(from_date)

    return {
        "free": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "free"),
        "plus": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "plus"),
        "premium": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "premium"),
    }



def get_winrate_by_score(days: int, buckets=None) -> Dict[str, Any]:
    materialized = get_materialized_window(days)
    if materialized and buckets is None:
        return materialized.get("by_score", {})
    now = datetime.utcnow()
    out = _build_score_buckets(_fetch_results(now - timedelta(days=days)), buckets=buckets)
    out["days"] = days
    return out


# ======================================================
# SNAPSHOT CONSOLIDADO
# ======================================================


def get_performance_snapshot(
    *,
    short_days: int = 7,
    long_days: int = 30,
    worst_symbols_limit: int = 3,
    worst_symbols_min_resolved: int = 3,
) -> Dict[str, Any]:
    short_materialized = get_materialized_window(short_days)
    long_materialized = get_materialized_window(long_days)

    short_payload = short_materialized or build_performance_window(short_days)
    long_payload = long_materialized or build_performance_window(long_days)

    worst_symbols = long_payload.get("worst_symbols", [])
    if worst_symbols_limit != 3 or worst_symbols_min_resolved != 3:
        results_long = _fetch_results(datetime.utcnow() - timedelta(days=long_days))
        worst_symbols = _build_symbol_diagnostics(
            results_long,
            min_resolved=worst_symbols_min_resolved,
            limit=worst_symbols_limit,
        )

    snapshot = {
        "summary_7d": short_payload.get("summary", {}),
        "summary_30d": long_payload.get("summary", {}),
        "by_plan_30d": long_payload.get("by_plan", {}),
        "activity_7d": short_payload.get("activity", {}),
        "activity_30d": long_payload.get("activity", {}),
        "activity_by_plan_30d": long_payload.get("activity_by_plan", {}),
        "by_score_30d": long_payload.get("by_score", {}),
        "direction_30d": long_payload.get("direction", []),
        "worst_symbols_30d": worst_symbols,
        "setup_groups_30d": long_payload.get("setup_groups", []),
        "diagnostics_30d": long_payload.get("diagnostics", {}),
        "materialized_7d": short_materialized is not None,
        "materialized_30d": long_materialized is not None,
    }

    return snapshot
