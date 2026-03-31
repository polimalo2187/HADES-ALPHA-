"""Market data cache & helpers.

Phase 2: performance + resiliency.

Goals:
- Reduce redundant Binance calls (premiumIndex/openInterest are per-symbol endpoints).
- Short TTL to avoid stale trading data.
- Deterministic tests: disable caching automatically when PYTEST_CURRENT_TEST is set.

This module is intentionally dependency-light and safe-failing.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional



def _caching_enabled() -> bool:
    # Keep tests deterministic (patched functions + side_effect lists).
    if os.getenv('PYTEST_CURRENT_TEST'):
        return False
    return os.getenv('HADES_DISABLE_MARKET_CACHE') not in {'1', 'true', 'TRUE', 'yes', 'YES'}


@dataclass
class _CacheItem:
    expires_at: float
    value: Any


class TTLCache:
    def __init__(self, ttl_seconds: int):
        self._ttl = max(1, int(ttl_seconds))
        self._lock = threading.Lock()
        self._data: Dict[str, _CacheItem] = {}

    def get(self, key: str) -> Optional[Any]:
        if not _caching_enabled():
            return None
        now = time.time()
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            if now >= item.expires_at:
                self._data.pop(key, None)
                return None
            return item.value

    def set(self, key: str, value: Any) -> None:
        if not _caching_enabled():
            return
        with self._lock:
            self._data[key] = _CacheItem(expires_at=time.time() + self._ttl, value=value)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_PREMIUM_RATE_CACHE = TTLCache(ttl_seconds=15)
_OPEN_INTEREST_CACHE = TTLCache(ttl_seconds=20)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _unique_symbols(symbols: Iterable[str]) -> list[str]:
    seen = set()
    ordered: list[str] = []
    for sym in symbols:
        key = str(sym or '').upper().strip()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def get_funding_rate_pct(symbol: str, *, premium_index_fn=None) -> float:
    """Funding rate in percent."""
    key = str(symbol or '').upper().strip()
    if not key:
        return 0.0

    cached = _PREMIUM_RATE_CACHE.get(key)
    if cached is not None:
        return float(cached)

    try:
        premium_index_fn = premium_index_fn or (lambda sym: __import__("app.binance_api", fromlist=["get_premium_index"]).get_premium_index(sym))
        premium = premium_index_fn(key) or {}
        funding_pct = _safe_float(premium.get('lastFundingRate')) * 100.0
    except Exception:
        funding_pct = 0.0

    _PREMIUM_RATE_CACHE.set(key, funding_pct)
    return funding_pct


def get_open_interest_value(symbol: str, *, open_interest_fn=None) -> float:
    """Open interest as raw number."""
    key = str(symbol or '').upper().strip()
    if not key:
        return 0.0

    cached = _OPEN_INTEREST_CACHE.get(key)
    if cached is not None:
        return float(cached)

    try:
        open_interest_fn = open_interest_fn or (lambda sym: __import__("app.binance_api", fromlist=["get_open_interest"]).get_open_interest(sym))
        payload = open_interest_fn(key) or {}
        oi = _safe_float(payload.get('openInterest'))
    except Exception:
        oi = 0.0

    _OPEN_INTEREST_CACHE.set(key, oi)
    return oi


def get_funding_rate_pct_map(symbols: Iterable[str], *, premium_index_fn=None) -> Dict[str, float]:
    result: Dict[str, float] = {}
    for sym in _unique_symbols(symbols):
        result[sym] = get_funding_rate_pct(sym, premium_index_fn=premium_index_fn)
    return result


def get_open_interest_map(symbols: Iterable[str], *, open_interest_fn=None) -> Dict[str, float]:
    result: Dict[str, float] = {}
    for sym in _unique_symbols(symbols):
        result[sym] = get_open_interest_value(sym, open_interest_fn=open_interest_fn)
    return result


def clear_market_data_caches() -> None:
    _PREMIUM_RATE_CACHE.clear()
    _OPEN_INTEREST_CACHE.clear()