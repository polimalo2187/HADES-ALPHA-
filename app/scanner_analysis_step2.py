# app/scanner.py

import os
import time
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

import pandas as pd
import requests
from telegram import Bot

from app.database import signals_collection
from app.notifier import notify_new_signal_alert
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
from app.signals import create_base_signal
from app.strategy import mtf_strategy

logger = logging.getLogger(__name__)

BINANCE_FUTURES_API = "https://fapi.binance.com"

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
MIN_QUOTE_VOLUME = int(os.getenv("MIN_QUOTE_VOLUME", "20000000"))
DEDUP_MINUTES = int(os.getenv("DEDUP_MINUTES", "10"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

# Thresholds basados en raw_score real.
PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "78"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "72"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "64"))


class RateLimiter:
    def __init__(self, delay: float):
        self.delay = delay
        self.last_request = 0.0

    def wait(self) -> None:
        elapsed = time.time() - self.last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request = time.time()


rate_limiter = RateLimiter(REQUEST_DELAY)


def get_klines(symbol: str, interval: str, limit: int = 220) -> pd.DataFrame:
    rate_limiter.wait()
    url = f"{BINANCE_FUTURES_API}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    df = pd.DataFrame(
        response.json(),
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )
    return df[["open", "high", "low", "close", "volume"]].astype(float)


def get_active_futures_symbols() -> List[str]:
    rate_limiter.wait()
    url = f"{BINANCE_FUTURES_API}/fapi/v1/ticker/24hr"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    symbols = [
        item["symbol"]
        for item in response.json()
        if item["symbol"].endswith("USDT")
        and float(item["quoteVolume"]) >= MIN_QUOTE_VOLUME
    ]
    logger.info("📊 %s símbolos activos con volumen suficiente", len(symbols))
    return symbols


def recent_duplicate_exists(symbol: str, direction: str, visibility: str) -> bool:
    since = datetime.utcnow() - timedelta(minutes=DEDUP_MINUTES)
    exists = (
        signals_collection().find_one(
            {
                "symbol": symbol,
                "direction": direction,
                "visibility": visibility,
                "created_at": {"$gte": since},
            }
        )
        is not None
    )

    if exists:
        logger.info(
            "♻️ Duplicado reciente detectado: %s %s (%s)",
            symbol,
            direction,
            visibility,
        )
    return exists


def _safe_ratio(num: float, den: float) -> float:
    try:
        den = float(den)
        if den == 0:
            return 0.0
        return float(num) / den
    except Exception:
        return 0.0


def _entry_quality(df_5m: pd.DataFrame, direction: str) -> float:
    """
    Bonus suave por entrada menos tardía.
    10 = entrada fresca.
    0 = entrada demasiado perseguida.
    """
    try:
        last = df_5m.iloc[-1]
        close = float(last["close"])
        high = float(last["high"])
        low = float(last["low"])

        candle_range = max(high - low, 1e-9)

        if direction == "LONG":
            progress = _safe_ratio(close - low, candle_range)
        else:
            progress = _safe_ratio(high - close, candle_range)

        freshness = max(0.0, min(1.0, 1.0 - progress))
        return round(freshness * 10.0, 2)
    except Exception:
        return 0.0


def _volume_quality(df_5m: pd.DataFrame) -> float:
    """
    Bonus suave de volumen entre 0 y 5.
    """
    try:
        last = df_5m.iloc[-1]
        volume = float(last["volume"])
        vol_ma = float(df_5m["volume"].tail(20).mean())

        if vol_ma <= 0:
            return 0.0

        ratio = volume / vol_ma

        if ratio >= 1.8:
            return 5.0
        if ratio >= 1.5:
            return 4.0
        if ratio >= 1.3:
            return 3.0
        if ratio >= 1.15:
            return 2.0
        if ratio >= 1.0:
            return 1.0
        return 0.0
    except Exception:
        return 0.0


def _raw_score(signal: Dict) -> float:
    return float(signal.get("raw_score", signal.get("score", 0.0)))


def _normalized_score(signal: Dict) -> float:
    return float(
        signal.get(
            "normalized_score",
            signal.get("score", signal.get("raw_score", 0.0)),
        )
    )


def _setup_group(signal: Dict) -> str:
    return str(signal.get("setup_group", "")).strip().lower()


# ------------------------------------------------------
# CLASIFICACIÓN MUTUAMENTE EXCLUSIVA POR PLAN
# ------------------------------------------------------
# PREMIUM: capa alta del universo shared.
# PLUS: capa intermedia real del universo shared.
# FREE: solo universo free. No compite con shared.
# ------------------------------------------------------

def _qualifies_for_premium(signal: Dict) -> bool:
    return _setup_group(signal) == "shared" and _raw_score(signal) >= PREMIUM_RAW_SCORE_MIN


def _qualifies_for_plus(signal: Dict) -> bool:
    score = _raw_score(signal)
    return _setup_group(signal) == "shared" and PLUS_RAW_SCORE_MIN <= score < PREMIUM_RAW_SCORE_MIN


def _qualifies_for_free(signal: Dict) -> bool:
    return _setup_group(signal) == "free" and _raw_score(signal) >= FREE_RAW_SCORE_MIN


def _pick_best(
    pool: List[Dict],
    predicate,
    used_symbols: Set[str],
) -> Optional[Dict]:
    for signal in pool:
        symbol = str(signal.get("symbol", ""))
        if not symbol or symbol in used_symbols:
            continue
        if predicate(signal):
            used_symbols.add(symbol)
            return signal
    return None


def _build_candidate(symbol: str, result: Dict, df_5m: pd.DataFrame) -> Dict:
    direction = str(result["direction"]).upper()
    raw_score = _raw_score(result)
    normalized_score = _normalized_score(result)
    entry_quality = _entry_quality(df_5m, direction)
    volume_quality = _volume_quality(df_5m)

    # Ranking operativo con score comparable entre perfiles.
    # Los thresholds siguen basados en raw_score para no romper el tiering.
    final_score = round(
        normalized_score + (entry_quality * 0.35) + (volume_quality * 0.40),
        2,
    )

    candidate = dict(result)
    candidate["symbol"] = symbol
    candidate["direction"] = direction
    candidate["raw_score"] = raw_score
    candidate["normalized_score"] = normalized_score
    candidate["entry_quality"] = entry_quality
    candidate["volume_quality"] = volume_quality
    candidate["final_score"] = final_score
    return candidate


async def scan_market_async(bot: Bot):
    logger.info(
        "📡 Scanner iniciado — clasificación exclusiva por plan + ranking con normalized_score"
    )

    while True:
        try:
            symbols = get_active_futures_symbols()
            candidates: List[Dict] = []

            for symbol in symbols:
                try:
                    df_1h = get_klines(symbol, "1h")
                    df_15m = get_klines(symbol, "15m")
                    df_5m = get_klines(symbol, "5m")

                    result = mtf_strategy(df_1h, df_15m, df_5m)
                    if result:
                        candidates.append(_build_candidate(symbol, result, df_5m))

                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.debug("⚠️ Error procesando %s: %s", symbol, e)

            if not candidates:
                logger.info("📭 No hay oportunidades fuertes en este ciclo")
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                continue

            candidates.sort(
                key=lambda x: (
                    x.get("final_score", _normalized_score(x)),
                    x.get("normalized_score", x.get("score", _raw_score(x))),
                    x.get("raw_score", x.get("score", 0)),
                    x.get("entry_quality", 0),
                    x.get("volume_quality", 0),
                    x.get("symbol", ""),
                ),
                reverse=True,
            )

            premium_candidates = [c for c in candidates if _qualifies_for_premium(c)]
            plus_candidates = [c for c in candidates if _qualifies_for_plus(c)]
            free_candidates = [c for c in candidates if _qualifies_for_free(c)]

            logger.info(
                "📚 Candidatos | total=%s | premium=%s | plus=%s | free=%s",
                len(candidates),
                len(premium_candidates),
                len(plus_candidates),
                len(free_candidates),
            )

            used_symbols: Set[str] = set()
            premium_signal = _pick_best(premium_candidates, lambda _: True, used_symbols)
            plus_signal = _pick_best(plus_candidates, lambda _: True, used_symbols)
            free_signal = _pick_best(free_candidates, lambda _: True, used_symbols)

            selected = [
                (PLAN_PREMIUM, "🥇 ORO", premium_signal),
                (PLAN_PLUS, "🥈 PLATA", plus_signal),
                (PLAN_FREE, "🥉 BRONCE", free_signal),
            ]

            for visibility, medal, signal in selected:
                if not signal:
                    continue

                symbol = str(signal["symbol"])
                direction = str(signal["direction"]).upper()
                entry_price = float(signal["entry_price"])
                raw_score = _raw_score(signal)
                normalized_score = _normalized_score(signal)
                final_score = float(signal.get("final_score", normalized_score))

                if recent_duplicate_exists(symbol, direction, visibility):
                    continue

                base_signal = create_base_signal(
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry_price,
                    stop_loss=float(signal["stop_loss"]),
                    take_profits=list(signal["take_profits"]),
                    timeframes=list(signal.get("timeframes", ["5M"])),
                    visibility=visibility,
                    # Conservamos raw_score aquí para no mezclar todavía
                    # este paso con la lógica de validez/evaluación del paso 3.
                    score=raw_score,
                    components=signal.get("components", []),
                    profiles=signal.get("profiles"),
                    atr_pct=signal.get("atr_pct"),
                    normalized_score=normalized_score,
                    raw_components=signal.get("raw_components"),
                    normalized_components=signal.get("normalized_components"),
                    setup_group=signal.get("setup_group"),
                    score_profile=signal.get("score_profile"),
                    score_calibration=signal.get("score_calibration"),
                )

                if not base_signal:
                    logger.info(
                        "⏭️ Señal descartada al crear base_signal: %s %s (%s)",
                        symbol,
                        direction,
                        visibility,
                    )
                    continue

                try:
                    await notify_new_signal_alert(
                        bot,
                        visibility,
                        base_signal=base_signal,
                    )
                except Exception as e:
                    logger.error("⚠️ Error notificando señal: %s", e)

                logger.info(
                    "✅ %s | %s %s | raw_score=%s | normalized_score=%s | final_score=%s | entry_q=%s | vol_q=%s | setup=%s | plan=%s | calib=%s",
                    medal,
                    symbol,
                    direction,
                    raw_score,
                    normalized_score,
                    final_score,
                    signal.get("entry_quality", 0),
                    signal.get("volume_quality", 0),
                    signal.get("setup_group", "unknown"),
                    visibility,
                    signal.get("score_calibration", "unknown"),
                )

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

        except Exception:
            logger.error("❌ Error crítico en scanner", exc_info=True)
            await asyncio.sleep(60)


def scan_market(bot: Bot):
    logger.info("🚀 Iniciando scanner en thread separado")
    asyncio.run(scan_market_async(bot))
