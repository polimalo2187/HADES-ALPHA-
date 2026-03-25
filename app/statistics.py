from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from app.database import (
    signal_results_collection,
    signals_collection,
    user_signals_collection,
)
from app.signals import evaluate_expired_signals


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



def _result_plan(result_doc: Dict[str, Any]) -> str:
    return str(result_doc.get("plan") or result_doc.get("visibility") or "").lower()



def _signal_plan(signal_doc: Dict[str, Any]) -> str:
    return str(signal_doc.get("visibility") or signal_doc.get("plan") or "").lower()



def _calculate_stats_from_results(results: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    total = 0
    won = 0
    lost = 0
    expired = 0

    for r in results:
        total += 1
        outcome = r.get("result")
        if outcome == "won":
            won += 1
        elif outcome == "lost":
            lost += 1
        elif outcome == "expired":
            expired += 1

    resolved = won + lost
    winrate = round((won / resolved) * 100, 2) if resolved > 0 else 0.0
    loss_rate = round((lost / resolved) * 100, 2) if resolved > 0 else 0.0
    expiry_rate = round((expired / total) * 100, 2) if total > 0 else 0.0

    return {
        "total": total,
        "won": won,
        "lost": lost,
        "expired": expired,
        "resolved": resolved,
        "winrate": winrate,
        "loss_rate": loss_rate,
        "expiry_rate": expiry_rate,
    }



def _activity_stats_from_signals(signals: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    total = 0
    score_sum = 0.0
    score_n = 0

    for signal in signals:
        total += 1
        score = _safe_float(signal.get("score"))
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

    output = []
    for lo, hi, label in buckets:
        won = 0
        lost = 0
        for result in results:
            score = _safe_float(result.get("score"))
            if score is None:
                continue
            if lo <= score < hi:
                if result.get("result") == "won":
                    won += 1
                elif result.get("result") == "lost":
                    lost += 1

        n = won + lost
        winrate = round((won / n) * 100, 2) if n > 0 else 0.0
        output.append({"label": label, "winrate": winrate, "n": n, "won": won, "lost": lost})

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
            }
        )

    rows.sort(key=lambda row: (row["winrate"], -row["resolved"], -row["lost"], row["symbol"]))
    return rows[:limit]



def _build_setup_group_stats(results: Iterable[Dict[str, Any]], signals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    signal_by_id = {str(s.get("_id")): s for s in signals if s.get("_id") is not None}
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for result in results:
        signal = signal_by_id.get(str(result.get("base_signal_id")))
        if not signal:
            continue
        group = str(signal.get("setup_group") or "").lower().strip()
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
            }
        )

    rows.sort(key=lambda row: (row["setup_group"]))
    return rows



def _build_diagnostics_summary(results: List[Dict[str, Any]], signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = _calculate_stats_from_results(results)

    result_scores = [_safe_float(result.get("score")) for result in results]
    valid_scores = [score for score in result_scores if score is not None]
    avg_result_score = round(sum(valid_scores) / len(valid_scores), 2) if valid_scores else "—"

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
        "pending_to_evaluate": max(len(signals) - stats["total"], 0),
        "avg_result_score": avg_result_score,
    }


# ======================================================
# RESET DE ESTADÍSTICAS
# ======================================================


def reset_statistics(preserve_signals: bool = False) -> Dict[str, Any]:
    """
    Reinicia estadísticas.

    preserve_signals=False (por defecto):
        - borra señales base
        - borra señales usuario
        - borra resultados evaluados

    preserve_signals=True:
        - borra documentos evaluados de signal_results
        - conserva el histórico bruto del scanner
        - limpia marcas de evaluación para poder recalcular desde las señales base
    """
    if preserve_signals:
        deleted_results = signal_results_collection().delete_many({}).deleted_count

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
            "reset_base_signals": base_modified,
            "reset_user_signals": user_modified,
        }

    deleted_base = signals_collection().delete_many({}).deleted_count
    deleted_user = user_signals_collection().delete_many({}).deleted_count
    deleted_results = signal_results_collection().delete_many({}).deleted_count
    return {
        "mode": "full_reset",
        "deleted_base_signals": deleted_base,
        "deleted_user_signals": deleted_user,
        "deleted_results": deleted_results,
    }


# ======================================================
# ESTADÍSTICAS LEGACY / COMPAT
# ======================================================


def get_daily_stats() -> Dict[str, Any]:
    evaluate_expired_signals()
    now = datetime.utcnow()
    return _calculate_stats_from_results(_fetch_results(_start_of_day(now)))



def get_weekly_stats() -> Dict[str, Any]:
    evaluate_expired_signals()
    now = datetime.utcnow()
    return _calculate_stats_from_results(_fetch_results(_start_of_week(now)))



def get_monthly_stats() -> Dict[str, Any]:
    evaluate_expired_signals()
    now = datetime.utcnow()
    return _calculate_stats_from_results(_fetch_results(_start_of_month(now)))



def get_last_days_stats(days: int) -> Dict[str, Any]:
    evaluate_expired_signals()
    now = datetime.utcnow()
    return _calculate_stats_from_results(_fetch_results(now - timedelta(days=days)))



def get_last_days_stats_by_plan(days: int) -> Dict[str, Any]:
    evaluate_expired_signals()
    now = datetime.utcnow()
    from_date = now - timedelta(days=days)
    results = _fetch_results(from_date)

    return {
        "free": _calculate_stats_from_results(r for r in results if _result_plan(r) == "free"),
        "plus": _calculate_stats_from_results(r for r in results if _result_plan(r) == "plus"),
        "premium": _calculate_stats_from_results(r for r in results if _result_plan(r) == "premium"),
    }



def get_signal_activity_stats(days: int) -> Dict[str, Any]:
    now = datetime.utcnow()
    return _activity_stats_from_signals(_fetch_signals(now - timedelta(days=days)))



def get_signal_activity_stats_by_plan(days: int) -> Dict[str, Any]:
    now = datetime.utcnow()
    from_date = now - timedelta(days=days)
    signals = _fetch_signals(from_date)

    return {
        "free": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "free"),
        "plus": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "plus"),
        "premium": _activity_stats_from_signals(s for s in signals if _signal_plan(s) == "premium"),
    }



def get_winrate_by_score(days: int, buckets=None) -> Dict[str, Any]:
    evaluate_expired_signals()
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
    evaluate_expired_signals()

    now = datetime.utcnow()
    from_short = now - timedelta(days=short_days)
    from_long = now - timedelta(days=long_days)

    results_long = _fetch_results(from_long)
    results_short = [r for r in results_long if r.get("evaluated_at") and r["evaluated_at"] >= from_short]

    signals_long = _fetch_signals(from_long)
    signals_short = [s for s in signals_long if s.get("created_at") and s["created_at"] >= from_short]

    by_plan = {
        "free": _calculate_stats_from_results(r for r in results_long if _result_plan(r) == "free"),
        "plus": _calculate_stats_from_results(r for r in results_long if _result_plan(r) == "plus"),
        "premium": _calculate_stats_from_results(r for r in results_long if _result_plan(r) == "premium"),
    }

    activity_by_plan = {
        "free": _activity_stats_from_signals(s for s in signals_long if _signal_plan(s) == "free"),
        "plus": _activity_stats_from_signals(s for s in signals_long if _signal_plan(s) == "plus"),
        "premium": _activity_stats_from_signals(s for s in signals_long if _signal_plan(s) == "premium"),
    }

    snapshot = {
        "summary_7d": _calculate_stats_from_results(results_short),
        "summary_30d": _calculate_stats_from_results(results_long),
        "by_plan_30d": by_plan,
        "activity_7d": _activity_stats_from_signals(signals_short),
        "activity_30d": _activity_stats_from_signals(signals_long),
        "activity_by_plan_30d": activity_by_plan,
        "by_score_30d": {"days": long_days, **_build_score_buckets(results_long)},
        "direction_30d": _build_direction_stats(results_long),
        "worst_symbols_30d": _build_symbol_diagnostics(
            results_long,
            min_resolved=worst_symbols_min_resolved,
            limit=worst_symbols_limit,
        ),
        "setup_groups_30d": _build_setup_group_stats(results_long, signals_long),
        "diagnostics_30d": _build_diagnostics_summary(results_long, signals_long),
    }

    return snapshot
