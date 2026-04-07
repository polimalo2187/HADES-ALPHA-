# app/scanner.py

import os
import inspect
import time
import logging
import asyncio
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

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

# ==============================
# Scanner runtime config
# ==============================

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "20"))
MIN_QUOTE_VOLUME = int(os.getenv("MIN_QUOTE_VOLUME", "20000000"))
DEDUP_MINUTES = int(os.getenv("DEDUP_MINUTES", "10"))

# Networking / rate limiting
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
REQUEST_MAX_RETRIES = max(1, int(os.getenv("REQUEST_MAX_RETRIES", "4")))
REQUEST_RETRY_BASE_SLEEP = float(os.getenv("REQUEST_RETRY_BASE_SLEEP", "0.6"))

# Concurrency: how many symbols we process in parallel.
SCANNER_SYMBOL_CONCURRENCY = max(1, int(os.getenv("SCANNER_SYMBOL_CONCURRENCY", "24")))

# Force a global inter-request delay (serializes requests); keep false by default.
SCANNER_FORCE_REQUEST_DELAY = str(os.getenv("SCANNER_FORCE_REQUEST_DELAY", "false")).strip().lower() in {"1", "true", "yes", "on"}

# Optional: soft QPS limiter (token-bucket). Defaults are conservative to avoid Binance bans.
SCANNER_MAX_REQUESTS_PER_SECOND = float(os.getenv("SCANNER_MAX_REQUESTS_PER_SECOND", "8"))
SCANNER_MAX_BURST = int(os.getenv("SCANNER_MAX_BURST", "16"))

# 5m fetching.
SCANNER_FETCH_5M_ENV = str(os.getenv("SCANNER_FETCH_5M", "false")).strip().lower() in {"1", "true", "yes", "on"}
# Esta estrategia adaptada es 5M-nativa; forzar 5M evita pasar df_5m=None a mtf_strategy().
SCANNER_FETCH_5M = True

# Kline limits: IMPORTANT — must be large enough for the slowest indicator (EMA200).
# Defaults are intentionally >= 260 to ensure warm-up and to enable the HTF context gate (>= 220).
KLINE_LIMIT_1H = int(os.getenv("SCANNER_KLINE_LIMIT_1H", "260"))
KLINE_LIMIT_15M = int(os.getenv("SCANNER_KLINE_LIMIT_15M", "260"))
KLINE_LIMIT_5M = int(os.getenv("SCANNER_KLINE_LIMIT_5M", "260"))

# Some strategies export these constants; if present, enforce minimums to avoid NaN/indicator-warmup outages.
_STRATEGY_EMA_SLOW = int(getattr(strategy_engine, "EMA_SLOW", 200))
_STRATEGY_LOOKBACK = int(getattr(strategy_engine, "BREAKOUT_LOOKBACK", 24))
_MIN_5M_BARS = max(_STRATEGY_EMA_SLOW + 30, _STRATEGY_LOOKBACK + 90, 260)
_MIN_HTF_BARS = 220

if KLINE_LIMIT_5M < _MIN_5M_BARS:
    logger.warning("⚠️ SCANNER_KLINE_LIMIT_5M=%s es insuficiente para EMA_SLOW=%s; subiendo a %s para evitar NaNs y bloqueo por trend_structure", KLINE_LIMIT_5M, _STRATEGY_EMA_SLOW, _MIN_5M_BARS)
    KLINE_LIMIT_5M = _MIN_5M_BARS

if KLINE_LIMIT_15M < _MIN_HTF_BARS:
    logger.warning("⚠️ SCANNER_KLINE_LIMIT_15M=%s < %s; el filtro HTF de shorts queda deshabilitado. Subiendo a %s.", KLINE_LIMIT_15M, _MIN_HTF_BARS, _MIN_HTF_BARS)
    KLINE_LIMIT_15M = _MIN_HTF_BARS

if KLINE_LIMIT_1H < _MIN_HTF_BARS:
    logger.warning("⚠️ SCANNER_KLINE_LIMIT_1H=%s < %s; el filtro HTF de shorts queda deshabilitado. Subiendo a %s.", KLINE_LIMIT_1H, _MIN_HTF_BARS, _MIN_HTF_BARS)
    KLINE_LIMIT_1H = _MIN_HTF_BARS

# Global inter-request delay (serializes requests across threads). Use only if you really need to.
EFFECTIVE_REQUEST_DELAY = REQUEST_DELAY if SCANNER_FORCE_REQUEST_DELAY else 0.0

# Thresholds basados en raw_score real.
PREMIUM_RAW_SCORE_MIN = float(os.getenv("PREMIUM_RAW_SCORE_MIN", "78"))
PLUS_RAW_SCORE_MIN = float(os.getenv("PLUS_RAW_SCORE_MIN", "72"))
FREE_RAW_SCORE_MIN = float(os.getenv("FREE_RAW_SCORE_MIN", "64"))

MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT = float(os.getenv("MAX_CLOSE_MARKET_PROGRESS_TO_TP1_PCT", "15"))
MAX_CLOSE_MARKET_R_PROGRESS = float(os.getenv("MAX_CLOSE_MARKET_R_PROGRESS", "0.15"))
SCORE_CALIBRATION_VERSION = "v6_liquidity_original_close_market"
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


def _strategy_call_kwargs(
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
    df_5m: Optional[pd.DataFrame],
    *,
    reference_market_price: Optional[float],
    debug_counts: Optional[Dict[str, int]],
) -> Dict:
    kwargs: Dict = {
        "df_1h": df_1h,
        "df_15m": df_15m,
        "df_5m": df_5m,
    }
    try:
        signature = inspect.signature(strategy_engine.mtf_strategy)
        parameters = signature.parameters
    except (TypeError, ValueError):
        return kwargs

    if "reference_market_price" in parameters:
        kwargs["reference_market_price"] = reference_market_price

    if "debug_counts" in parameters:
        kwargs["debug_counts"] = debug_counts if debug_counts is not None else {}

    return kwargs


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


def _coerce_reference_price(current_price) -> float:
    try:
        if isinstance(current_price, pd.DataFrame):
            if current_price.empty or "close" not in current_price.columns:
                return 0.0
            return float(current_price.iloc[-1]["close"])
        if isinstance(current_price, pd.Series):
            if "close" in current_price.index:
                return float(current_price["close"])
            return 0.0
        return float(current_price or 0.0)
    except Exception:
        return 0.0


def _apply_close_market_execution(result: Dict, current_price: float) -> Optional[Dict]:
    if not result:
        return None

    enriched = dict(result)
    if str(enriched.get("send_mode") or "").strip().lower() == "market_on_close":
        # La estrategia actual ya viene emitida a mercado. Evitar doble repricing/discard.
        return enriched

    direction = str(enriched.get("direction") or "").upper().strip()
    if direction not in {"LONG", "SHORT"}:
        return None

    model_entry = float(enriched.get("entry_price") or 0.0)
    stop_loss = float(enriched.get("stop_loss") or 0.0)
    market_entry = _coerce_reference_price(current_price)
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


def build_symbol_candidate(symbol: str, df_1h: pd.DataFrame, df_15m: pd.DataFrame, df_5m: Optional[pd.DataFrame], *, debug_counts: Optional[Dict[str, int]] = None) -> Optional[Dict]:
    closed_15m = _closed_15m_frame(df_15m)
    reference_price = None
    try:
        if df_5m is not None and len(df_5m) > 0:
            reference_price = float(df_5m.iloc[-1]["close"])
        elif not closed_15m.empty:
            reference_price = float(closed_15m.iloc[-1]["close"])
    except Exception:
        reference_price = None

    strategy_kwargs = _strategy_call_kwargs(
        df_1h=df_1h,
        df_15m=closed_15m,
        df_5m=df_5m,
        reference_market_price=reference_price,
        debug_counts=debug_counts,
    )
    result = strategy_engine.mtf_strategy(**strategy_kwargs)
    if not result:
        return None
    price_for_candidate = reference_price
    if price_for_candidate is None:
        try:
            price_for_candidate = float(result.get("entry_sent_price") or result.get("entry_price") or 0.0)
        except Exception:
            price_for_candidate = 0.0
    return _build_candidate(symbol, result, price_for_candidate)


class RateLimiter:
    def __init__(self, delay: float):
        self.delay = max(0.0, float(delay))
        self.last_request = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        if self.delay <= 0:
            return
        with self._lock:
            elapsed = time.time() - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_request = time.time()


rate_limiter = RateLimiter(EFFECTIVE_REQUEST_DELAY)


class TokenBucket:
    """Thread-safe token bucket to cap request rate without serializing all work.

    This avoids the previous "delay=0" + high concurrency situation that can silently
    trigger upstream rate-limits and starve the scanner.
    """

    def __init__(self, rate: float, capacity: int):
        self.rate = max(0.1, float(rate))
        self.capacity = max(1, int(capacity))
        self.tokens = float(self.capacity)
        self.updated_at = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        need = max(0.0, float(tokens))
        if need <= 0:
            return
        while True:
            with self._lock:
                now = time.time()
                elapsed = now - self.updated_at
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + (elapsed * self.rate))
                    self.updated_at = now
                if self.tokens >= need:
                    self.tokens -= need
                    return
                missing = need - self.tokens
                wait_s = missing / self.rate if self.rate > 0 else 0.25
            time.sleep(max(0.01, min(wait_s, 1.0)))


_token_bucket = TokenBucket(SCANNER_MAX_REQUESTS_PER_SECOND, SCANNER_MAX_BURST)


def _request_json(url: str, *, params: dict | None = None, timeout: int = REQUEST_TIMEOUT):
    """Requests wrapper with retries + rate limiting."""
    last_exc: Exception | None = None
    for attempt in range(REQUEST_MAX_RETRIES):
        try:
            _token_bucket.acquire(1.0)
            rate_limiter.wait()
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code in (418, 429):
                raise RuntimeError(f"binance_rate_limited status={resp.status_code} body={resp.text[:120]}")
            if 500 <= resp.status_code < 600:
                raise RuntimeError(f"binance_5xx status={resp.status_code} body={resp.text[:120]}")
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            sleep_s = min(8.0, REQUEST_RETRY_BASE_SLEEP * (2 ** attempt))
            # tiny jitter to reduce lockstep retries
            sleep_s = sleep_s + (0.05 * (attempt + 1))
            time.sleep(sleep_s)
    raise last_exc or RuntimeError('request_failed')



def get_klines(symbol: str, interval: str, limit: int = 220) -> pd.DataFrame:
    url = f"{BINANCE_FUTURES_API}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": int(limit)}
    payload = _request_json(url, params=params, timeout=REQUEST_TIMEOUT)

    df = pd.DataFrame(
        payload,
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
    payload = _request_json(url, timeout=REQUEST_TIMEOUT)

    symbols = [
        item["symbol"]
        for item in payload
        if str(item.get("symbol", "")).endswith("USDT")
        and float(item.get("quoteVolume", 0.0) or 0.0) >= MIN_QUOTE_VOLUME
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


def _entry_quality(reference_price: float, direction: str, entry_price: float, stop_loss: float) -> float:
    """Bonus suave por entrada menos tardía usando precio de referencia."""
    try:
        risk = abs(entry_price - stop_loss)
        if risk <= 1e-9:
            return 0.0
        if direction == "LONG":
            progress = max(0.0, reference_price - entry_price) / risk
        else:
            progress = max(0.0, entry_price - reference_price) / risk
        freshness = max(0.0, min(1.0, 1.0 - progress))
        return round(freshness * 10.0, 2)
    except Exception:
        return 0.0


def _volume_quality(df_15m: pd.DataFrame) -> float:
    """Bonus suave de volumen entre 0 y 5."""
    try:
        last = df_15m.iloc[-1]
        volume = float(last["volume"])
        vol_ma = float(df_15m["volume"].tail(20).mean())

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


def _build_candidate(symbol: str, result: Dict, reference_price: float) -> Optional[Dict]:
    executed = _apply_close_market_execution(result, reference_price)
    if not executed:
        return None

    direction = str(executed["direction"]).upper()
    raw_score = _raw_score(executed)
    normalized_score = _normalized_score(executed)
    entry_quality = _entry_quality(reference_price, direction, float(executed.get("entry_price") or 0.0), float(executed.get("stop_loss") or 0.0))
    final_score = round(
        normalized_score + (entry_quality * 0.35),
        2,
    )

    candidate = dict(executed)
    candidate["symbol"] = symbol
    candidate["direction"] = direction
    candidate["raw_score"] = raw_score
    candidate["normalized_score"] = normalized_score
    candidate["entry_quality"] = entry_quality
    candidate["volume_quality"] = 0.0
    candidate["final_score"] = final_score
    return candidate


def _record_failure(debug_counts: Dict[str, int], reason: str) -> None:
    if not reason:
        reason = "unknown"
    debug_counts[reason] = int(debug_counts.get(reason, 0)) + 1


def _merge_debug_counts(total: Dict[str, int], local: Dict[str, int]) -> None:
    for key, value in (local or {}).items():
        total[key] = int(total.get(key, 0)) + int(value)


def _compact_rejects(rejects: Dict[str, int], limit: int = 10) -> Dict[str, int]:
    items = sorted((rejects or {}).items(), key=lambda kv: kv[1], reverse=True)
    compact = {k: int(v) for k, v in items[: max(1, int(limit))]}
    other = sum(int(v) for _k, v in items[max(1, int(limit)) :])
    if other:
        compact["__other__"] = other
    return compact



def _extract_failure_reason(local: Dict[str, int]) -> Optional[str]:
    if not local:
        return None
    return max(local.items(), key=lambda item: item[1])[0]


def _process_symbol(symbol: str) -> Tuple[Optional[Dict], Dict[str, int], Optional[str]]:
    local_debug: Dict[str, int] = {}
    try:
        df_1h = get_klines(symbol, "1h", limit=KLINE_LIMIT_1H)
        df_15m = get_klines(symbol, "15m", limit=KLINE_LIMIT_15M)
        df_5m = get_klines(symbol, "5m", limit=KLINE_LIMIT_5M) if SCANNER_FETCH_5M else None
        candidate = build_symbol_candidate(symbol, df_1h, df_15m, df_5m, debug_counts=local_debug)
        return candidate, local_debug, None
    except Exception as exc:
        return None, local_debug, f"{symbol}: {exc}"


async def scan_market_async(bot: Bot):
    logger.info(
        "📡 Scanner iniciado — clasificación exclusiva por plan + ranking con normalized_score"
    )
    logger.info(
        "⚙️ Scanner config | concurrency=%s | request_delay_env=%ss | effective_request_delay=%ss | force_request_delay=%s | fetch_5m=%s | kline_limits={'1h': %s, '15m': %s, '5m': %s}",
        SCANNER_SYMBOL_CONCURRENCY,
        REQUEST_DELAY,
        EFFECTIVE_REQUEST_DELAY,
        SCANNER_FORCE_REQUEST_DELAY,
        SCANNER_FETCH_5M,
        KLINE_LIMIT_1H,
        KLINE_LIMIT_15M,
        KLINE_LIMIT_5M,
    )

    cycle_number = 0

    while True:
        try:
            cycle_started_at = datetime.utcnow()
            symbols = get_active_futures_symbols()
            candidates: List[Dict] = []
            reject_totals: Dict[str, int] = {}
            failures = 0
            failure_samples: List[str] = []
            semaphore = asyncio.Semaphore(SCANNER_SYMBOL_CONCURRENCY)

            async def _run(symbol: str):
                async with semaphore:
                    return await asyncio.to_thread(_process_symbol, symbol)

            results = await asyncio.gather(*[_run(symbol) for symbol in symbols])
            for candidate, local_debug, failure in results:
                if candidate:
                    candidates.append(candidate)
                else:
                    _merge_debug_counts(reject_totals, local_debug)
                if failure:
                    failures += 1
                    if len(failure_samples) < 5:
                        failure_samples.append(failure)

            cycle_duration = (datetime.utcnow() - cycle_started_at).total_seconds()

            if failures and not candidates:
                logger.warning(
                    "📭 Sin oportunidades en este ciclo, pero hubo errores de scanner | cycle=%s symbols=%s failures=%s lag=n/a duration=%.3fs samples=%s",
                    cycle_number,
                    len(symbols),
                    failures,
                    cycle_duration,
                    failure_samples,
                )
                heartbeat(
                    "scanner",
                    status="warn",
                    details={
                        "cycle": cycle_number,
                        "symbols": len(symbols),
                        "candidates": 0,
                        "selected": 0,
                        "failures": failures,
                        "failure_samples": failure_samples,
                        "duration_seconds": cycle_duration,
                    },
                )
                cycle_number += 1
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                continue

            if not candidates:
                logger.info(
                    "📭 No hay oportunidades fuertes en este ciclo | duration=%.3fs | rejects=%s",
                    cycle_duration,
                    _compact_rejects(reject_totals),
                )
                heartbeat(
                    "scanner",
                    status="ok",
                    details={
                        "cycle": cycle_number,
                        "symbols": len(symbols),
                        "candidates": 0,
                        "selected": 0,
                        "failures": failures,
                        "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
                        "cycle_started_at": cycle_started_at.isoformat(),
                        "duration_seconds": cycle_duration,
                        "rejects": reject_totals,
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
                "📚 Candidatos | total=%s | premium=%s | plus=%s | free=%s | duration=%.3fs | rejects=%s",
                len(candidates),
                len(premium_candidates),
                len(plus_candidates),
                len(free_candidates),
                cycle_duration,
                _compact_rejects(reject_totals),
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
                status="ok",
                details={
                    "cycle": cycle_number,
                    "symbols": len(symbols),
                    "candidates": len(candidates),
                    "premium_candidates": len(premium_candidates),
                    "plus_candidates": len(plus_candidates),
                    "free_candidates": len(free_candidates),
                    "selected": selected_count,
                    "failures": failures,
                    "scan_interval_seconds": SCAN_INTERVAL_SECONDS,
                    "cycle_started_at": cycle_started_at.isoformat(),
                    "duration_seconds": cycle_duration,
                    "rejects": reject_totals,
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
