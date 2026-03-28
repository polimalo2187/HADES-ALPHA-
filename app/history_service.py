from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from bson import ObjectId

from app.config import is_admin
from app.database import signal_history_collection, signal_results_collection, signals_collection, users_collection
from app.models import new_signal_history_record
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM

logger = logging.getLogger(__name__)


def _plan_visibility_filter(plan: Optional[str], user_id: Optional[int] = None) -> Dict[str, object]:
    if user_id is not None and is_admin(int(user_id)):
        return {}

    plan_value = str(plan or PLAN_FREE).lower()
    if plan_value == PLAN_PREMIUM:
        return {"visibility": {"$in": [PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM]}}
    if plan_value == PLAN_PLUS:
        return {"visibility": {"$in": [PLAN_FREE, PLAN_PLUS]}}
    return {"visibility": PLAN_FREE}


def build_signal_history_record(base_signal: Dict, result_doc: Dict) -> Dict:
    take_profits = list(base_signal.get("take_profits") or [])
    tp1 = take_profits[0] if len(take_profits) > 0 else result_doc.get("tp_used")
    tp2 = take_profits[1] if len(take_profits) > 1 else None

    record = new_signal_history_record(
        signal_id=str(base_signal.get("_id") or result_doc.get("base_signal_id") or result_doc.get("signal_id")),
        result_id=str(result_doc.get("_id")) if result_doc.get("_id") is not None else None,
        symbol=base_signal.get("symbol") or result_doc.get("symbol"),
        direction=base_signal.get("direction") or result_doc.get("direction"),
        visibility=base_signal.get("visibility") or result_doc.get("visibility"),
        score=base_signal.get("score", result_doc.get("score")),
        normalized_score=base_signal.get("normalized_score", result_doc.get("normalized_score")),
        setup_group=base_signal.get("setup_group", result_doc.get("setup_group")),
        score_profile=base_signal.get("score_profile"),
        score_calibration=base_signal.get("score_calibration"),
        result=result_doc.get("result"),
        entry_price=base_signal.get("entry_price", result_doc.get("entry_price")),
        stop_loss=base_signal.get("stop_loss"),
        tp1=tp1,
        tp2=tp2,
        risk_pct=result_doc.get("risk_pct"),
        reward_pct=result_doc.get("reward_pct"),
        r_multiple=result_doc.get("r_multiple"),
        resolution_minutes=result_doc.get("resolution_minutes"),
        market_validity_minutes=base_signal.get("market_validity_minutes", result_doc.get("market_validity_minutes")),
        signal_created_at=base_signal.get("created_at", result_doc.get("signal_created_at")),
        signal_valid_until=result_doc.get("signal_valid_until") or base_signal.get("valid_until"),
        evaluation_valid_until=result_doc.get("evaluation_valid_until") or base_signal.get("evaluation_valid_until"),
        telegram_valid_until=result_doc.get("telegram_valid_until") or base_signal.get("telegram_valid_until"),
        timeframes=base_signal.get("timeframes") or [],
    )
    if isinstance(result_doc.get("evaluated_at"), datetime):
        record["evaluated_at"] = result_doc.get("evaluated_at")
    return record


def upsert_signal_history_record(base_signal: Dict, result_doc: Dict) -> None:
    record = build_signal_history_record(base_signal, result_doc)
    signal_history_collection().update_one(
        {"signal_id": record["signal_id"]},
        {"$set": record, "$setOnInsert": {"created_at": record.get("created_at") or datetime.utcnow()}},
        upsert=True,
    )


def backfill_signal_history(limit: int = 200) -> int:
    history_col = signal_history_collection()
    result_rows = list(
        signal_results_collection()
        .find({"base_signal_id": {"$exists": True, "$ne": None}})
        .sort("evaluated_at", -1)
        .limit(max(1, int(limit)))
    )
    processed = 0
    for result_doc in result_rows:
        signal_id = str(result_doc.get("base_signal_id") or result_doc.get("signal_id") or "")
        if not signal_id:
            continue
        if history_col.find_one({"signal_id": signal_id}, {"_id": 1}):
            continue
        try:
            base_signal = None
            try:
                base_signal = signals_collection().find_one({"_id": ObjectId(signal_id)})
            except Exception:
                base_signal = None
            if not base_signal:
                base_signal = {
                    "_id": signal_id,
                    "symbol": result_doc.get("symbol"),
                    "direction": result_doc.get("direction"),
                    "visibility": result_doc.get("visibility"),
                    "score": result_doc.get("score"),
                    "normalized_score": result_doc.get("normalized_score"),
                    "setup_group": result_doc.get("setup_group"),
                    "entry_price": result_doc.get("entry_price"),
                    "created_at": result_doc.get("signal_created_at"),
                    "valid_until": result_doc.get("signal_valid_until"),
                    "evaluation_valid_until": result_doc.get("evaluation_valid_until"),
                    "telegram_valid_until": result_doc.get("telegram_valid_until"),
                    "market_validity_minutes": result_doc.get("market_validity_minutes"),
                }
            upsert_signal_history_record(base_signal, result_doc)
            processed += 1
        except Exception as exc:
            logger.error("❌ Error haciendo backfill de histórico | signal_id=%s error=%s", signal_id, exc, exc_info=True)
    if processed:
        logger.info("🧾 Backfill de histórico completado | processed=%s", processed)
    return processed


def get_history_entries_for_user(user_id: int, *, user_plan: Optional[str] = None, limit: int = 10) -> List[Dict]:
    if user_plan is None:
        user = users_collection().find_one({"user_id": int(user_id)}, {"plan": 1}) or {}
        user_plan = user.get("plan", PLAN_FREE)

    query = _plan_visibility_filter(user_plan, user_id=user_id)
    return list(
        signal_history_collection()
        .find(query)
        .sort("signal_created_at", -1)
        .limit(max(1, int(limit)))
    )
