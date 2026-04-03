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
from app.realtime_pipeline import enqueue_signal_dispatch
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
from app.signals import create_base_signal
from app.observability import heartbeat
from app import strategy as strategy_engine

logger = logging.getLogger(__name__)

BINANCE_FUTURES_API = "https://fapi.binance.com"

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "20"))
MIN_QUOTE_VOLUME = int(os.getenv("MIN_QUOTE_VOLUME", "20000000"))
DEDUP_MINUTES = int(os.getenv("DEDUP_MINUTES", "10"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

# Thresholds basados en raw_score real.
PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "78"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "72"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "64"))


MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT = float(os.getenv("MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT", "15"))
MAX_CLOSE_MARKET_R_PROGRESS = float(os.getenv("MAX_CLOSE_MARKET_R_PROGRESS", "0.15"))
SCORE_CALIBRATION_VERSION = strategy_engine.SCORE_CALIBRATION_VERSION
ENTRY_MODEL_NAME = "liquidity_zone_offset_v1"
SETUP_STAGE_CLOSED_CONFIRMED = "closed_confirmed"

_PROFILE_CONFIGS = {
    "premium": dict(strategy_engine.PREMIUM_PROFILE),
    "plus": dict(strategy_engine.PLUS_PROFILE),
    "free": dict(strategy_engine.FREE_PROFILE),
}
_PROFILE_SCORE_MAP = {
    round(float(cfg.get("score", 0.0)), 2): name for name, cfg in _PROFILE_CONFIGS.items()
}


def _closed_15m_frame(df_15m: pd.DataFrame) -> pd.DataFrame:
    if df_15m.empty or "close_time" not in df_15m.columns:
        return df_15m.copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    closed = df_15m[df_15m["close_time"] <= now_utc].copy()
    if not closed.empty:
        return closed
    if len(df_15m) > 1:
        return df_15m.iloc[:-1].copy()
    return df_15m.copy()


def _infer_setup_group(signal: Dict) -> str:
    explicit = str(signal.get("setup_group") or signal.get("score_profile") or signal.get("candidate_tier") or "").strip().lower()
    if explicit in _PROFILE_CONFIGS:
        return explicit
    try:
        score = round(float(signal.get("score", signal.get("raw_score", 0.0))), 2)
    except Exception:
        score = 0.0
    return _PROFILE_SCORE_MAP.get(score, "free")


def _profile_config_for_signal(signal: Dict) -> Dict:
    return dict(_PROFILE_CONFIGS.get(_infer_setup_group(signal), _PROFILE_CONFIGS["free"]))


def _safe_rr(entry_price: float, stop_loss: float, target_price: float) -> float:
    risk = abs(stop_loss - entry_price)
    if risk <= 1e-9:
        return 0.0
    return abs(target_price - entry_price) / risk


def _progress_from_model_to_tp1_pct(model_entry: float, model_tp1: float, sent_entry: float, direction: str) -> float:
    denominator = abs(model_tp1 - model_entry)
    if denominator <= 1e-9:
        return 0.0
    if direction == "LONG":
        moved = max(0.0, sent_entry - model_entry)
    else:
        moved = max(0.0, model_entry - sent_entry)
    return round((moved / denominator) * 100.0, 2)


def _r_progress_from_model_entry(model_entry: float, stop_loss: float, sent_entry: float, direction: str) -> float:
    denominator = abs(model_entry - stop_loss)
    if denominator <= 1e-9:
        return 0.0
    if direction == "LONG":
        moved = sent_entry - model_entry
    else:
        moved = model_entry - sent_entry
    return round(moved / denominator, 4)


def _reprice_profiles(model_profiles: Dict[str, Dict], model_entry: float, stop_loss: float, market_entry: float, direction: str) -> Dict[str, Dict]:
    repriced: Dict[str, Dict] = {}
    market_risk = abs(stop_loss - market_entry)
    for profile_name, payload in (model_profiles or {}).items():
        take_profits = list(payload.get("take_profits") or [])
        tp1 = take_profits[0] if len(take_profits) > 0 else None
        tp2 = take_profits[1] if len(take_profits) > 1 else None
        tp1_rr = _safe_rr(model_entry, stop_loss, float(tp1)) if tp1 is not None else 0.0
        tp2_rr = _safe_rr(model_entry, stop_loss, float(tp2)) if tp2 is not None else 0.0
        if direction == "LONG":
            new_tps = [
                round(market_entry + (market_risk * tp1_rr), 8),
                round(market_entry + (market_risk * tp2_rr), 8),
            ]
        else:
            new_tps = [
                round(market_entry - (market_risk * tp1_rr), 8),
                round(market_entry - (market_risk * tp2_rr), 8),
            ]
        repriced[profile_name] = {
            "stop_loss": round(float(payload.get("stop_loss", stop_loss)), 8),
            "take_profits": new_tps,
            "leverage": payload.get("leverage"),
        }
    return repriced


def _apply_close_market_execution(result: Dict, current_price: float) -> Optional[Dict]:
    if not result:
        return None

    enriched = dict(result)
    direction = str(enriched.get("direction") or "").upper().strip()
    if direction not in {"LONG", "SHORT"}:
        return None

    model_entry = float(enriched.get("entry_price") or 0.0)
    stop_loss = float(enriched.get("stop_loss") or 0.0)
    market_entry = float(current_price or 0.0)
    if market_entry <= 0 or stop_loss <= 0 or model_entry <= 0:
        return None

    if direction == "LONG" and market_entry <= stop_loss:
        return None
    if direction == "SHORT" and market_entry >= stop_loss:
        return None

    setup_group = _infer_setup_group(enriched)
    profile_cfg = _profile_config_for_signal(enriched)
    risk_pct = abs(stop_loss - market_entry) / max(market_entry, 1e-9)
    if risk_pct > float(profile_cfg.get("max_risk_pct", 1.0)):
        return None

    model_profiles = dict(enriched.get("profiles") or {})
    conservative = model_profiles.get("conservador") or {}
    model_take_profits = list(conservative.get("take_profits") or enriched.get("take_profits") or [])
    if not model_take_profits:
        return None
    model_tp1 = float(model_take_profits[0])
    progress_pct = _progress_from_model_to_tp1_pct(model_entry, model_tp1, market_entry, direction)
    r_progress = _r_progress_from_model_entry(model_entry, stop_loss, market_entry, direction)
    if progress_pct > MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT or r_progress > MAX_CLOSE_MARKET_R_PROGRESS:
        return None

    repriced_profiles = _reprice_profiles(model_profiles, model_entry, stop_loss, market_entry, direction)
    conservative_profile = repriced_profiles.get("conservador") or {}
    take_profits = list(conservative_profile.get("take_profits") or [])
    if not take_profits:
        return None
    conservative_tp1_rr = _safe_rr(market_entry, stop_loss, float(take_profits[0]))
    if conservative_tp1_rr < float(profile_cfg.get("min_rr", 0.0)):
        return None

    enriched.update({
        "entry_price": round(market_entry, 8),
        "take_profits": take_profits,
        "profiles": repriced_profiles,
        "raw_score": float(enriched.get("raw_score", enriched.get("score", 0.0))),
        "normalized_score": float(enriched.get("normalized_score", enriched.get("score", 0.0))),
        "components": list(enriched.get("components") or []),
        "raw_components": list(enriched.get("raw_components") or enriched.get("components") or []),
        "normalized_components": list(enriched.get("normalized_components") or enriched.get("components") or []),
        "setup_group": setup_group,
        "score_profile": setup_group,
        "score_calibration": SCORE_CALIBRATION_VERSION,
        "send_mode": "market_on_close",
        "entry_model_price": round(model_entry, 8),
        "entry_sent_price": round(market_entry, 8),
        "tp1_progress_at_send_pct": progress_pct,
        "r_progress_at_send": r_progress,
        "setup_stage": SETUP_STAGE_CLOSED_CONFIRMED,
        "candidate_tier": setup_group,
        "final_tier": setup_group,
        "entry_model": ENTRY_MODEL_NAME,
    })
    return enriched


def build_symbol_candidate(symbol: str, df_1h: pd.DataFrame, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> Optional[Dict]:
    closed_15m = _closed_15m_frame(df_15m)
    result = strategy_engine.mtf_strategy(df_1h=df_1h, df_15m=closed_15m, df_5m=df_5m)
    if not result:
        return None
    return _build_candidate(symbol, result, df_5m)


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
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df[["open_time", "close_time", "open", "high", "low", "close", "volume"]]


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
# PREMIUM / PLUS / FREE salen directamente del perfil nativo de liquidez.
# No existe promoción posterior entre tiers. La jerarquía la maneja el routing al usuario.
# ------------------------------------------------------

def _qualifies_for_premium(signal: Dict) -> bool:
    return _setup_group(signal) == "premium" and _raw_score(signal) >= PREMIUM_RAW_SCORE_MIN


def _qualifies_for_plus(signal: Dict) -> bool:
    return _setup_group(signal) == "plus" and _raw_score(signal) >= PLUS_RAW_SCORE_MIN


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
            return signal
    return None


def _build_base_signal(signal: Dict, visibility: str) -> Optional[Dict]:
    return create_base_signal(
        symbol=str(signal["symbol"]),
        direction=str(signal["direction"]).upper(),
        entry_price=float(signal["entry_price"]),
        stop_loss=float(signal["stop_loss"]),
        take_profits=list(signal["take_profits"]),
        timeframes=list(signal.get("timeframes", ["5M"])),
        visibility=visibility,
        score=_raw_score(signal),
        components=signal.get("components", []),
        profiles=signal.get("profiles"),
        atr_pct=signal.get("atr_pct"),
        normalized_score=_normalized_score(signal),
        raw_components=signal.get("raw_components"),
        normalized_components=signal.get("normalized_components"),
        setup_group=signal.get("setup_group"),
        score_profile=signal.get("score_profile"),
        score_calibration=signal.get("score_calibration"),
        send_mode=signal.get("send_mode"),
        entry_model_price=signal.get("entry_model_price"),
        entry_sent_price=signal.get("entry_sent_price"),
        tp1_progress_at_send_pct=signal.get("tp1_progress_at_send_pct"),
        r_progress_at_send=signal.get("r_progress_at_send"),
        setup_stage=signal.get("setup_stage"),
    )


def _select_dispatchable_signal(
    pool: List[Dict],
    visibility: str,
    used_symbols: Set[str],
) -> Optional[tuple[Dict, Dict]]:
    for signal in pool:
        symbol = str(signal.get("symbol", ""))
        if not symbol or symbol in used_symbols:
            continue

        direction = str(signal.get("direction", "")).upper()
        if recent_duplicate_exists(symbol, direction, visibility):
            continue

        base_signal = _build_base_signal(signal, visibility)
        if not base_signal:
            logger.info(
                "⏭️ Señal descartada al crear base_signal: %s %s (%s)",
                symbol,
                direction,
                visibility,
            )
            continue

        used_symbols.add(symbol)
        return signal, base_signal

    return None


def _build_candidate(symbol: str, result: Dict, df_5m: pd.DataFrame) -> Optional[Dict]:
    current_price = float(df_5m.iloc[-1]["close"])
    executed = _apply_close_market_execution(result, current_price)
    if not executed:
        return None

    direction = str(executed["direction"]).upper()
    raw_score = _raw_score(executed)
    normalized_score = _normalized_score(executed)
    entry_quality = _entry_quality(df_5m, direction)
    volume_quality = _volume_quality(df_5m)

    final_score = round(
        normalized_score + (entry_quality * 0.35) + (volume_quality * 0.40),
        2,
    )

    candidate = dict(executed)
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

    cycle_number = 0

    while True:
        try:
            cycle_started_at = datetime.utcnow()
            symbols = get_active_futures_symbols()
            candidates: List[Dict] = []

            symbol_failures = 0
            symbol_failure_samples: List[str] = []

            for symbol in symbols:
                try:
                    df_1h = get_klines(symbol, "1h")
                    df_15m = get_klines(symbol, "15m")
                    df_5m = get_klines(symbol, "5m")

                    candidate = build_symbol_candidate(symbol, df_1h, df_15m, df_5m)
                    if candidate:
                        candidates.append(candidate)

                    await asyncio.sleep(0.05)
                except Exception as e:
                    symbol_failures += 1
                    if len(symbol_failure_samples) < 5:
                        symbol_failure_samples.append(f"{symbol}: {e}")
                    logger.warning("⚠️ Error procesando %s: %s", symbol, e)

            if not candidates:
                if symbol_failures:
                    logger.warning(
                        "📭 Sin oportunidades en este ciclo, pero hubo errores de scanner | cycle=%s symbols=%s failures=%s samples=%s",
                        cycle_number,
                        len(symbols),
                        symbol_failures,
                        symbol_failure_samples,
                    )
                else:
                    logger.info("📭 No hay oportunidades fuertes en este ciclo")
                heartbeat(
                    "scanner",
                    status="degraded" if symbol_failures else "ok",
                    details={
                        "cycle": cycle_number,
                        "symbols": len(symbols),
                        "candidates": 0,
                        "selected": 0,
                        "symbol_failures": symbol_failures,
                        "symbol_failure_samples": symbol_failure_samples,
                        "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
                        "cycle_started_at": cycle_started_at.isoformat(),
                    },
                )
                cycle_number += 1
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
            selected = [
                (PLAN_PREMIUM, "🥇 ORO", premium_candidates),
                (PLAN_PLUS, "🥈 PLATA", plus_candidates),
                (PLAN_FREE, "🥉 BRONCE", free_candidates),
            ]
            selected_count = 0

            for visibility, medal, pool in selected:
                chosen = _select_dispatchable_signal(pool, visibility, used_symbols)
                if not chosen:
                    continue

                signal, base_signal = chosen
                symbol = str(signal["symbol"])
                direction = str(signal["direction"]).upper()
                raw_score = _raw_score(signal)
                normalized_score = _normalized_score(signal)
                final_score = float(signal.get("final_score", normalized_score))

                try:
                    enqueue_signal_dispatch(base_signal)
                    selected_count += 1
                except Exception as e:
                    logger.error("⚠️ Error encolando señal para dispatch: %s", e, exc_info=True)
                    continue

                logger.info(
                    "✅ %s | %s %s | raw_score=%s | normalized_score=%s | final_score=%s | entry_q=%s | vol_q=%s | setup=%s | plan=%s | calib=%s | send_mode=%s | stage=%s",
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
                    signal.get("send_mode", "unknown"),
                    signal.get("setup_stage", "unknown"),
                )

            heartbeat(
                "scanner",
                status="degraded" if symbol_failures else "ok",
                details={
                    "cycle": cycle_number,
                    "symbols": len(symbols),
                    "candidates": len(candidates),
                    "premium_candidates": len(premium_candidates),
                    "plus_candidates": len(plus_candidates),
                    "free_candidates": len(free_candidates),
                    "selected": selected_count,
                    "symbol_failures": symbol_failures,
                    "symbol_failure_samples": symbol_failure_samples,
                    "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
                    "cycle_started_at": cycle_started_at.isoformat(),
                },
            )
            cycle_number += 1
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

        except Exception as exc:
            heartbeat("scanner", status="error", details={"error": str(exc), "cycle": cycle_number})
            logger.error("❌ Error crítico en scanner", exc_info=True)
            await asyncio.sleep(60)


def scan_market(bot: Bot):
    logger.info("🚀 Iniciando scanner en thread separado")
    asyncio.run(scan_market_async(bot))
