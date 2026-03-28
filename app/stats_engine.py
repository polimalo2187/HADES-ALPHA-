from __future__ import annotations

import logging
from typing import Dict

from app.signals import evaluate_expired_signals
from app.statistics import refresh_materialized_stats

logger = logging.getLogger(__name__)


def run_statistics_cycle(*, evaluation_limit: int = 200) -> Dict[str, int]:
    evaluated = 0
    snapshots = 0

    try:
        evaluated = evaluate_expired_signals(limit=evaluation_limit)
    except Exception as exc:
        logger.error("❌ Error evaluando señales expiradas: %s", exc, exc_info=True)

    try:
        snapshots = refresh_materialized_stats()
    except Exception as exc:
        logger.error("❌ Error refrescando snapshots de estadísticas: %s", exc, exc_info=True)

    if evaluated or snapshots:
        logger.info(
            "📊 Ciclo de estadísticas completado | evaluated=%s snapshots=%s",
            evaluated,
            snapshots,
        )

    return {"evaluated": evaluated, "snapshots": snapshots}
