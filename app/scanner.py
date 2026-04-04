# app/scanner.py

import os
import time
import logging
import asyncio
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
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
SCAN_IDLE_POLL_SECONDS = float(os.getenv("SCAN_IDLE_POLL_SECONDS", "2"))
SCAN_CLOSE_GRACE_SECONDS = float(os.getenv("SCAN_CLOSE_GRACE_SECONDS", "3"))
SCANNER_SYMBOL_CONCURRENCY = int(os.getenv("SCANNER_SYMBOL_CONCURRENCY", "24"))
MIN_QUOTE_VOLUME = int(os.getenv("MIN_QUOTE_VOLUME", "20000000"))
DEDUP_MINUTES = int(os.getenv("DEDUP_MINUTES", "10"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
REQUEST_CONNECT_TIMEOUT = float(os.getenv("REQUEST_CONNECT_TIMEOUT", str(min(4.0, REQUEST_TIMEOUT))))
REQUEST_READ_TIMEOUT = float(os.getenv("REQUEST_READ_TIMEOUT", str(REQUEST_TIMEOUT)))
HTTP_POOL_SIZE = max(8, SCANNER_SYMBOL_CONCURRENCY * 3)
SCANNER_FETCH_5M = str(os.getenv("SCANNER_FETCH_5M", "false")).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_KLINE_LIMITS = {"1h": 96, "15m": 96, "5m": 64}

# Thresholds basados en raw_score real.
PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "78"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "72"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "64"))


MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT = float(os.getenv("MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT", "15"))
MAX_CLOSE_MARKET_R_PROGRESS = float(os.getenv("MAX_CLOSE_MARKET_R_PROGRESS", "0.15"))
SCORE_CALIBRATION_VERSION = strategy_engine.SCORE_CALIBRATION_VERSION
ENTRY_MODEL_NAME = "liquidity_zone_offset_v1"
SETUP_STAGE_CLOSED_CONFIRMED = "closed_confirmed"

_thread_local = threading.local()


def _http_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=HTTP_POOL_SIZE, pool_maxsize=HTTP_POOL_SIZE, max_retries=0)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _thread_local.session = session
    return session


def _http_get(url: str, *, params: Optional[Dict] = None) -> requests.Response:
    rate_limiter.wait()
    response = _http_session().get(
        url,
        params=params,
        timeout=(REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT),
    )
    response.raise_for_status()
    return response


def _latest_ready_15m_close(now: Optional[datetime] = None) -> datetime:
    current = now or datetime.utcnow()
    reference = current.replace(
        minute=(current.minute // 15) * 15,
        second=0,
        microsecond=0,
    )
    if (current - reference).total_seconds() < SCAN_CLOSE_GRACE_SECONDS:
        reference -= timedelta(minutes=15)
    return reference


def _should_scan_reference_close(last_processed_close: Optional[datetime], reference_close: datetime) -> bool:
    return last_processed_close is None or reference_close > last_processed_close


def _reference_price_from_frame(df_frame: Optional[pd.DataFrame], reference_close: Optional[datetime]) -> Optional[float]:
    if df_frame is None or df_frame.empty:
        return None
    try:
        if reference_close is None or "close_time" not in df_frame.columns:
            return float(df_frame.iloc[-1]["close"])
        ref_ts = pd.Timestamp(reference_close, tz="UTC") if pd.Timestamp(reference_close).tzinfo is None else pd.Timestamp(reference_close).tz_convert("UTC")
        closed = df_frame[df_frame["close_time"] <= ref_ts]
        if not closed.empty:
            return float(closed.iloc[-1]["close"])
        if len(df_frame) > 1:
            return float(df_frame.iloc[-2]["close"])
        return float(df_frame.iloc[-1]["close"])
    except Exception:
        return None


def _candidate_rank_frame(df_15m: pd.DataFrame, df_5m: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df_5m is not None and not df_5m.empty:
        return df_5m
    return df_15m


def _reference_price_from_5m(df_5m: Optional[pd.DataFrame], reference_close: Optional[datetime]) -> Optional[float]:
    """Backward-compatible wrapper kept for tests and existing callers."""
    return _reference_price_from_frame(df_5m, reference_close)


def _fetch_symbol_candidate(symbol: str, reference_close: datetime) -> tuple[Optional[Dict], Dict[str, int]]:
    df_1h = get_klines(symbol, "1h")
    df_15m = get_klines(symbol, "15m")
    df_5m = get_klines(symbol, "5m") if SCANNER_FETCH_5M else None
    return build_symbol_candidate(symbol, df_1h, df_15m, df_5m, reference_close)


async def _collect_symbol_candidates(symbols: List[str], reference_close: datetime) -> Tuple[List[Dict], int, List[str], Dict[str, int]]:
    semaphore = asyncio.Semaphore(max(1, SCANNER_SYMBOL_CONCURRENCY))
    candidates: List[Dict] = []
    symbol_failures = 0
    symbol_failure_samples: List[str] = []
    reject_totals: Dict[str, int] = {}

    async def _scan_one(symbol: str) -> None:
        nonlocal symbol_failures
        async with semaphore:
            try:
                candidate, reject_counts = await asyncio.to_thread(_fetch_symbol_candidate, symbol, reference_close)
                for key, value in reject_counts.items():
                    reject_totals[key] = reject_totals.get(key, 0) + int(value)
                if candidate:
                    candidates.append(candidate)
            except Exception as exc:
                symbol_failures += 1
                if len(symbol_failure_samples) < 5:
                    symbol_failure_samples.append(f"{symbol}: {exc}")
                logger.warning("⚠️ Error procesando %s: %s", symbol, exc)

    await asyncio.gather(*(_scan_one(symbol) for symbol in symbols))
    return candidates, symbol_failures, symbol_failure_samples, reject_totals

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

    setup_group = _infer_setup_group(enriched)
    profile_cfg = _profile_config_for_signal(enriched)
    stop_loss = float(enriched.get("stop_loss") or 0.0)

    prepriced_market_signal = str(enriched.get("send_mode") or "").strip().lower() == "market_on_close" and enriched.get("entry_sent_price") is not None

    if prepriced_market_signal:
        model_entry = float(enriched.get("entry_model_price") or enriched.get("entry_price") or 0.0)
        market_entry = float(enriched.get("entry_sent_price") or enriched.get("entry_price") or current_price or 0.0)
        profiles = dict(enriched.get("profiles") or {})
        conservative = profiles.get("conservador") or {}
        take_profits = list(conservative.get("take_profits") or enriched.get("take_profits") or [])
        if market_entry <= 0 or stop_loss <= 0 or model_entry <= 0 or not take_profits:
            return None

        if direction == "LONG" and market_entry <= stop_loss:
            return None
        if direction == "SHORT" and market_entry >= stop_loss:
            return None

        risk_pct = abs(stop_loss - market_entry) / max(market_entry, 1e-9)
        if risk_pct > float(profile_cfg.get("max_risk_pct", 1.0)):
            return None

        progress_pct = float(enriched.get("tp1_progress_at_send_pct") or 0.0)
        r_progress = float(enriched.get("r_progress_at_send") or 0.0)
        if progress_pct > MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT or r_progress > MAX_CLOSE_MARKET_R_PROGRESS:
            return None

        enriched.update({
            "entry_price": round(market_entry, 8),
            "take_profits": take_profits,
            "profiles": profiles,
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
            "setup_stage": str(enriched.get("setup_stage") or SETUP_STAGE_CLOSED_CONFIRMED),
            "candidate_tier": enriched.get("candidate_tier") or setup_group,
            "final_tier": enriched.get("final_tier") or setup_group,
            "entry_model": enriched.get("entry_model") or ENTRY_MODEL_NAME,
        })
        return enriched

    pending_signal = str(enriched.get("send_mode") or "").strip().lower() == "entry_zone_pending"
    model_entry = float(enriched.get("entry_model_price") or enriched.get("entry_price") or 0.0)

    if pending_signal:
        if model_entry <= 0 or stop_loss <= 0:
            return None

        risk_pct = abs(stop_loss - model_entry) / max(model_entry, 1e-9)
        if risk_pct > float(profile_cfg.get("max_risk_pct", 1.0)):
            return None

        model_profiles = dict(enriched.get("profiles") or {})
        conservative = model_profiles.get("conservador") or {}
        model_take_profits = list(conservative.get("take_profits") or enriched.get("take_profits") or [])
        if not model_take_profits:
            return None
        conservative_tp1_rr = _safe_rr(model_entry, stop_loss, float(model_take_profits[0]))
        if conservative_tp1_rr < float(profile_cfg.get("min_rr", 0.0)):
            return None

        enriched.update({
            "entry_price": round(model_entry, 8),
            "take_profits": model_take_profits,
            "profiles": model_profiles,
            "raw_score": float(enriched.get("raw_score", enriched.get("score", 0.0))),
            "normalized_score": float(enriched.get("normalized_score", enriched.get("score", 0.0))),
            "components": list(enriched.get("components") or []),
            "raw_components": list(enriched.get("raw_components") or enriched.get("components") or []),
            "normalized_components": list(enriched.get("normalized_components") or enriched.get("components") or []),
            "setup_group": setup_group,
            "score_profile": setup_group,
            "score_calibration": SCORE_CALIBRATION_VERSION,
            "send_mode": "entry_zone_pending",
            "entry_model_price": round(model_entry, 8),
            "entry_sent_price": None,
            "tp1_progress_at_send_pct": None,
            "r_progress_at_send": None,
            "setup_stage": str(enriched.get("setup_stage") or SETUP_STAGE_CLOSED_CONFIRMED),
            "candidate_tier": enriched.get("candidate_tier") or setup_group,
            "final_tier": enriched.get("final_tier") or setup_group,
            "entry_model": enriched.get("entry_model") or ENTRY_MODEL_NAME,
        })
        return enriched

    market_entry = float(current_price or enriched.get("entry_sent_price") or enriched.get("entry_price") or 0.0)
    if market_entry <= 0 or stop_loss <= 0 or model_entry <= 0:
        return None

    if direction == "LONG" and market_entry <= stop_loss:
        return None
    if direction == "SHORT" and market_entry >= stop_loss:
        return None

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


def build_symbol_candidate(symbol: str, df_1h: pd.DataFrame, df_15m: pd.DataFrame, df_5m: Optional[pd.DataFrame], reference_close: datetime) -> tuple[Optional[Dict], Dict[str, int]]:
    closed_15m = _closed_15m_frame(df_15m)
    rank_frame = _candidate_rank_frame(closed_15m, df_5m)
    reference_market_price = _reference_price_from_frame(rank_frame, reference_close)
    debug_counts: Dict[str, int] = {}
    result = strategy_engine.mtf_strategy(
        df_1h=df_1h,
        df_15m=closed_15m,
        df_5m=df_5m,
        reference_market_price=reference_market_price,
        debug_counts=debug_counts,
    )
    if not result:
        return None, debug_counts
    return _build_candidate(symbol, result, closed_15m, df_5m), debug_counts


class RateLimiter:
    def __init__(self, delay: float):
        self.delay = max(0.0, float(delay))
        self.last_request = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        if self.delay <= 0:
            return
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
                now = time.monotonic()
            self.last_request = now


rate_limiter = RateLimiter(REQUEST_DELAY)


def get_klines(symbol: str, interval: str, limit: Optional[int] = None) -> pd.DataFrame:
    if limit is None:
        limit = DEFAULT_KLINE_LIMITS.get(interval, 96)
    url = f"{BINANCE_FUTURES_API}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = _http_get(url, params=params)

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
    url = f"{BINANCE_FUTURES_API}/fapi/v1/ticker/24hr"
    response = _http_get(url)

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


def _build_candidate(symbol: str, result: Dict, df_15m: Optional[pd.DataFrame] = None, df_5m: Optional[pd.DataFrame] = None) -> Optional[Dict]:
    direction = str(result.get("direction") or "").upper()
    send_mode = str(result.get("send_mode") or "").strip().lower()
    if df_15m is None:
        df_15m = df_5m if df_5m is not None else pd.DataFrame()
    rank_frame = _candidate_rank_frame(df_15m, df_5m)

    if send_mode == "market_on_close":
        current_price = float(rank_frame.iloc[-1]["close"])
        candidate = _apply_close_market_execution(result, current_price)
        if not candidate:
            return None
    else:
        candidate = dict(result)
        candidate.setdefault("send_mode", "entry_zone_pending")
        candidate.setdefault("entry_model_price", candidate.get("entry_price"))
        candidate.setdefault("entry_sent_price", None)

    raw_score = _raw_score(candidate)
    normalized_score = _normalized_score(candidate)
    entry_quality = _entry_quality(rank_frame, direction)
    volume_quality = _volume_quality(rank_frame)

    final_score = round(
        normalized_score + (entry_quality * 0.35) + (volume_quality * 0.40),
        2,
    )

    candidate = dict(candidate)
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
    last_processed_close: Optional[datetime] = None

    while True:
        try:
            reference_close = _latest_ready_15m_close()
            if not _should_scan_reference_close(last_processed_close, reference_close):
                await asyncio.sleep(SCAN_IDLE_POLL_SECONDS)
                continue

            cycle_started_at = datetime.utcnow()
            scan_lag_seconds = round(max(0.0, (cycle_started_at - reference_close).total_seconds()), 3)
            symbols = get_active_futures_symbols()
            candidates, symbol_failures, symbol_failure_samples, reject_totals = await _collect_symbol_candidates(symbols, reference_close)

            if not candidates:
                cycle_finished_at = datetime.utcnow()
                cycle_duration_seconds = round(max(0.0, (cycle_finished_at - cycle_started_at).total_seconds()), 3)
                if symbol_failures:
                    logger.warning(
                        "📭 Sin oportunidades en este ciclo, pero hubo errores de scanner | cycle=%s symbols=%s failures=%s lag=%ss duration=%ss samples=%s",
                        cycle_number,
                        len(symbols),
                        symbol_failures,
                        scan_lag_seconds,
                        cycle_duration_seconds,
                        symbol_failure_samples,
                    )
                else:
                    logger.info("📭 No hay oportunidades fuertes en este ciclo | lag=%ss | duration=%ss | rejects=%s", scan_lag_seconds, cycle_duration_seconds, reject_totals)
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
                        "reference_close": reference_close.isoformat(),
                        "scan_lag_seconds": scan_lag_seconds,
                        "cycle_started_at": cycle_started_at.isoformat(),
                        "cycle_duration_seconds": cycle_duration_seconds,
                        "fetch_5m_enabled": SCANNER_FETCH_5M,
                        "reject_totals": reject_totals,
                    },
                )
                last_processed_close = reference_close
                cycle_number += 1
                await asyncio.sleep(SCAN_IDLE_POLL_SECONDS)
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

            cycle_finished_at = datetime.utcnow()
            cycle_duration_seconds = round(max(0.0, (cycle_finished_at - cycle_started_at).total_seconds()), 3)

            logger.info(
                "📚 Candidatos | total=%s | premium=%s | plus=%s | free=%s | lag=%ss | duration=%ss | rejects=%s",
                len(candidates),
                len(premium_candidates),
                len(plus_candidates),
                len(free_candidates),
                scan_lag_seconds,
                cycle_duration_seconds,
                reject_totals,
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
                    "scanner_symbol_concurrency": SCANNER_SYMBOL_CONCURRENCY,
                    "reference_close": reference_close.isoformat(),
                    "scan_lag_seconds": scan_lag_seconds,
                    "cycle_started_at": cycle_started_at.isoformat(),
                    "cycle_duration_seconds": cycle_duration_seconds,
                    "fetch_5m_enabled": SCANNER_FETCH_5M,
                    "reject_totals": reject_totals,
                },
            )
            last_processed_close = reference_close
            cycle_number += 1
            await asyncio.sleep(SCAN_IDLE_POLL_SECONDS)

        except Exception as exc:
            heartbeat("scanner", status="error", details={"error": str(exc), "cycle": cycle_number})
            logger.error("❌ Error crítico en scanner", exc_info=True)
            await asyncio.sleep(60)

def scan_market(bot: Bot):
    logger.info("🚀 Iniciando scanner en thread separado")
    asyncio.run(scan_market_async(bot))
