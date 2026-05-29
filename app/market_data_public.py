"""Public market-data provider layer.

The bot must not depend on a single exchange endpoint. Binance can return HTTP 451
from some hosting regions, so this module uses public, unauthenticated endpoints from
multiple venues and normalizes their payloads to the Binance-like shapes the app
already expects.

Default provider order is intentionally NOT Binance-first:
    MARKET_DATA_PROVIDERS=bybit,okx,binance

No API keys are used here.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests

logger = logging.getLogger(__name__)

PUBLIC_MARKET_TIMEOUT_SECONDS = int(os.getenv("PUBLIC_MARKET_TIMEOUT_SECONDS", os.getenv("BINANCE_PUBLIC_TIMEOUT_SECONDS", "6")))
PUBLIC_MARKET_PROVIDERS = [
    p.strip().lower()
    for p in os.getenv("MARKET_DATA_PROVIDERS", "bybit,okx,binance").split(",")
    if p.strip()
]

ACTIVE_SYMBOLS_FALLBACK = [
    s.strip().upper()
    for s in os.getenv(
        "ACTIVE_SYMBOLS_FALLBACK",
        "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,LTCUSDT,TRXUSDT,NEARUSDT,OPUSDT,ARBUSDT,APTUSDT,INJUSDT,SUIUSDT,SEIUSDT,TONUSDT,DOTUSDT",
    ).split(",")
    if s.strip()
]

_CACHE: Dict[str, Tuple[float, Any]] = {}
_CACHE_LOCK = threading.Lock()

_TTL_TICKERS = int(os.getenv("PUBLIC_MARKET_TICKERS_TTL_SECONDS", "45"))
_TTL_SYMBOL_DETAILS = int(os.getenv("PUBLIC_MARKET_SYMBOL_DETAILS_TTL_SECONDS", "120"))
_TTL_KLINES = int(os.getenv("PUBLIC_MARKET_KLINES_TTL_SECONDS", "25"))

# Provider health / circuit breaker. This is deliberately process-local and cheap:
# it prevents repeated calls to a provider that already returned a hard block
# (Binance 451, WAF 403, rate-limit 429/418, etc.) without adding Redis/DB load.
_PROVIDER_HEALTH_ENABLED = str(os.getenv("PUBLIC_MARKET_PROVIDER_HEALTH_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
_PROVIDER_RESTRICTED_COOLDOWN_SECONDS = max(60, int(os.getenv("PUBLIC_MARKET_PROVIDER_RESTRICTED_COOLDOWN_SECONDS", "1800")))
_PROVIDER_FORBIDDEN_COOLDOWN_SECONDS = max(30, int(os.getenv("PUBLIC_MARKET_PROVIDER_FORBIDDEN_COOLDOWN_SECONDS", "300")))
_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS = max(30, int(os.getenv("PUBLIC_MARKET_PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS", "300")))
_PROVIDER_5XX_COOLDOWN_SECONDS = max(10, int(os.getenv("PUBLIC_MARKET_PROVIDER_5XX_COOLDOWN_SECONDS", "60")))
_SYMBOL_PROVIDER_MISS_TTL_SECONDS = max(60, int(os.getenv("PUBLIC_MARKET_SYMBOL_PROVIDER_MISS_TTL_SECONDS", "900")))

_PROVIDER_HEALTH: Dict[str, Dict[str, Any]] = {}
_PROVIDER_HEALTH_LOCK = threading.Lock()
_SYMBOL_PROVIDER_MISSES: Dict[Tuple[str, str, str], float] = {}
_SYMBOL_PROVIDER_MISSES_LOCK = threading.Lock()


class PublicMarketDataError(RuntimeError):
    pass


class ProviderCircuitOpen(PublicMarketDataError):
    pass


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    payload: Any


def _cache_get(key: str) -> Any | None:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return None
    now = time.time()
    with _CACHE_LOCK:
        item = _CACHE.get(key)
        if not item:
            return None
        expires_at, value = item
        if now >= expires_at:
            _CACHE.pop(key, None)
            return None
        return value


def _cache_set(key: str, value: Any, ttl_seconds: int) -> None:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    with _CACHE_LOCK:
        _CACHE[key] = (time.time() + max(1, int(ttl_seconds)), value)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _infer_provider_from_url(url: str) -> str:
    value = str(url or "").lower()
    if "bybit" in value:
        return "bybit"
    if "okx" in value:
        return "okx"
    if "binance" in value:
        return "binance"
    return "unknown"


def _provider_health_snapshot() -> Dict[str, Dict[str, Any]]:
    now = time.time()
    with _PROVIDER_HEALTH_LOCK:
        return {
            provider: dict(state)
            for provider, state in _PROVIDER_HEALTH.items()
            if float(state.get("cooldown_until", 0.0) or 0.0) > now
        }


def get_public_provider_health() -> Dict[str, Dict[str, Any]]:
    """Return currently open provider circuits for diagnostics/admin logs."""
    snapshot = _provider_health_snapshot()
    now = time.time()
    for state in snapshot.values():
        state["remaining_seconds"] = max(0.0, float(state.get("cooldown_until", 0.0) or 0.0) - now)
    return snapshot


def _provider_cooldown_remaining(provider: str) -> float:
    if not _PROVIDER_HEALTH_ENABLED:
        return 0.0
    provider = str(provider or "unknown").lower().strip()
    now = time.time()
    with _PROVIDER_HEALTH_LOCK:
        state = _PROVIDER_HEALTH.get(provider)
        if not state:
            return 0.0
        until = float(state.get("cooldown_until", 0.0) or 0.0)
        if until <= now:
            _PROVIDER_HEALTH.pop(provider, None)
            return 0.0
        return until - now


def _provider_available(provider: str) -> bool:
    return _provider_cooldown_remaining(provider) <= 0.0


def _provider_unavailable_error(provider: str) -> ProviderCircuitOpen:
    state = _provider_health_snapshot().get(str(provider or "unknown").lower().strip(), {})
    remaining = max(0.0, float(state.get("cooldown_until", 0.0) or 0.0) - time.time())
    reason = str(state.get("reason") or "provider_circuit_open")
    return ProviderCircuitOpen(f"{provider} circuit_open remaining={remaining:.1f}s reason={reason[:120]}")


def _mark_provider_unhealthy(provider: str, reason: str, cooldown_seconds: int, *, status_code: Optional[int] = None) -> None:
    if not _PROVIDER_HEALTH_ENABLED:
        return
    provider = str(provider or "unknown").lower().strip()
    if provider not in {"bybit", "okx", "binance"}:
        return
    cooldown_seconds = max(1, int(cooldown_seconds))
    until = time.time() + cooldown_seconds
    with _PROVIDER_HEALTH_LOCK:
        previous = _PROVIDER_HEALTH.get(provider) or {}
        previous_until = float(previous.get("cooldown_until", 0.0) or 0.0)
        if previous_until >= until:
            return
        _PROVIDER_HEALTH[provider] = {
            "cooldown_until": until,
            "reason": str(reason or "provider_unhealthy"),
            "status_code": status_code,
            "marked_at": time.time(),
        }
    logger.warning(
        "⛔ Public market provider circuit opened | provider=%s | cooldown=%ss | status=%s | reason=%s",
        provider,
        cooldown_seconds,
        status_code,
        str(reason or "")[:180],
    )


def _clear_provider_health(provider: str) -> None:
    if not _PROVIDER_HEALTH_ENABLED:
        return
    provider = str(provider or "unknown").lower().strip()
    with _PROVIDER_HEALTH_LOCK:
        _PROVIDER_HEALTH.pop(provider, None)


def _retry_after_seconds(response: requests.Response, default: int) -> int:
    try:
        retry_after = response.headers.get("Retry-After") if getattr(response, "headers", None) else None
        if retry_after is not None:
            return max(1, int(float(retry_after)))
    except Exception:
        pass
    return int(default)


def _public_get_json(
    url: str,
    *,
    params: Optional[dict] = None,
    timeout: int = PUBLIC_MARKET_TIMEOUT_SECONDS,
    provider: Optional[str] = None,
) -> Any:
    provider_name = str(provider or _infer_provider_from_url(url)).lower().strip()
    if provider_name in {"bybit", "okx", "binance"} and not _provider_available(provider_name):
        raise _provider_unavailable_error(provider_name)

    response = requests.get(url, params=params, timeout=timeout)
    status = int(getattr(response, "status_code", 0) or 0)
    body = str(getattr(response, "text", "") or "")

    if status == 451:
        _mark_provider_unhealthy(
            provider_name,
            f"restricted_location status=451 url={url} body={body[:160]}",
            _PROVIDER_RESTRICTED_COOLDOWN_SECONDS,
            status_code=status,
        )
        raise PublicMarketDataError(f"restricted_location status=451 provider={provider_name} url={url} body={body[:160]}")

    if status == 403:
        _mark_provider_unhealthy(
            provider_name,
            f"forbidden status=403 url={url} body={body[:160]}",
            _PROVIDER_FORBIDDEN_COOLDOWN_SECONDS,
            status_code=status,
        )
        raise PublicMarketDataError(f"forbidden status=403 provider={provider_name} url={url} body={body[:160]}")

    if status in (418, 429):
        cooldown = _retry_after_seconds(response, _PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS)
        _mark_provider_unhealthy(
            provider_name,
            f"rate_limited status={status} url={url} body={body[:160]}",
            cooldown,
            status_code=status,
        )
        raise PublicMarketDataError(f"rate_limited status={status} provider={provider_name} url={url} body={body[:160]}")

    if 500 <= status < 600:
        _mark_provider_unhealthy(
            provider_name,
            f"upstream_5xx status={status} url={url} body={body[:160]}",
            _PROVIDER_5XX_COOLDOWN_SECONDS,
            status_code=status,
        )
        raise PublicMarketDataError(f"upstream_5xx status={status} provider={provider_name} url={url} body={body[:160]}")

    response.raise_for_status()
    payload = response.json()
    if provider_name in {"bybit", "okx", "binance"}:
        _clear_provider_health(provider_name)
    return payload


def _provider_order() -> List[str]:
    supported = {"bybit", "okx", "binance"}
    ordered: List[str] = []
    for provider in PUBLIC_MARKET_PROVIDERS:
        if provider in supported and provider not in ordered:
            ordered.append(provider)
    for provider in ("bybit", "okx", "binance"):
        if provider not in ordered:
            ordered.append(provider)
    return ordered


def _available_provider_order() -> List[str]:
    return [provider for provider in _provider_order() if _provider_available(provider)]


def _provider_skip_reason(provider: str) -> Optional[str]:
    remaining = _provider_cooldown_remaining(provider)
    if remaining <= 0.0:
        return None
    state = _provider_health_snapshot().get(str(provider or "").lower().strip(), {})
    return f"{provider}: circuit_open remaining={remaining:.1f}s reason={str(state.get('reason') or '')[:120]}"


def _mark_symbol_provider_miss(provider: str, symbol: str, data_kind: str, reason: str = "not_listed") -> None:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    provider = str(provider or "").lower().strip()
    symbol = str(symbol or "").upper().strip()
    data_kind = str(data_kind or "ticker").lower().strip()
    if not provider or not symbol:
        return
    with _SYMBOL_PROVIDER_MISSES_LOCK:
        _SYMBOL_PROVIDER_MISSES[(provider, symbol, data_kind)] = time.time() + _SYMBOL_PROVIDER_MISS_TTL_SECONDS


def _symbol_provider_recently_missed(provider: str, symbol: str, data_kind: str) -> bool:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False
    provider = str(provider or "").lower().strip()
    symbol = str(symbol or "").upper().strip()
    data_kind = str(data_kind or "ticker").lower().strip()
    key = (provider, symbol, data_kind)
    now = time.time()
    with _SYMBOL_PROVIDER_MISSES_LOCK:
        expires_at = _SYMBOL_PROVIDER_MISSES.get(key)
        if not expires_at:
            return False
        if now >= expires_at:
            _SYMBOL_PROVIDER_MISSES.pop(key, None)
            return False
        return True


def get_public_provider_order() -> List[str]:
    """Return the effective public provider order used by the app.

    Kept tiny and cache-free so UI/service layers can label market data without
    hardcoding Binance as the source.
    """
    return list(_provider_order())


def public_provider_label(provider: Any = None) -> str:
    value = str(provider or "").lower().strip()
    labels = {
        "bybit": "Bybit",
        "okx": "OKX",
        "binance": "Binance",
        "fallback": "proveedores públicos",
    }
    if value:
        return labels.get(value, value.upper())
    order = [labels.get(p, p.upper()) for p in _provider_order()]
    return " / ".join(order[:2]) if order else "proveedores públicos"


def _symbol_to_okx_inst_id(symbol: str) -> str:
    s = str(symbol or "").upper().strip().replace("-", "").replace("/", "")
    if not s.endswith("USDT"):
        s = f"{s}USDT"
    base = s[:-4]
    return f"{base}-USDT-SWAP"


def _okx_inst_id_to_symbol(inst_id: str) -> str:
    return str(inst_id or "").upper().replace("-USDT-SWAP", "USDT").replace("-", "")


def _interval_to_bybit(interval: str) -> str:
    key = str(interval or "").strip().lower()
    return {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "2h": "120",
        "4h": "240",
        "1d": "D",
    }.get(key, key.rstrip("m"))


def _interval_to_okx(interval: str) -> str:
    key = str(interval or "").strip().lower()
    return {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "1d": "1D",
    }.get(key, key)


def _fallback_24h_tickers() -> List[Dict[str, Any]]:
    return [
        {
            "symbol": symbol,
            "priceChangePercent": "0",
            "quoteVolume": "0",
            "lastPrice": "0",
            "count": "0",
            "provider": "fallback",
        }
        for symbol in ACTIVE_SYMBOLS_FALLBACK
    ]


# --------------------------
# Tickers / active symbols
# --------------------------


def _fetch_bybit_tickers() -> List[Dict[str, Any]]:
    data = _public_get_json("https://api.bybit.com/v5/market/tickers", params={"category": "linear"}, provider="bybit")
    rows = (((data or {}).get("result") or {}).get("list") or [])
    normalized: List[Dict[str, Any]] = []
    for item in rows:
        symbol = str(item.get("symbol", "")).upper().strip()
        if not symbol.endswith("USDT"):
            continue
        last_price = _safe_float(item.get("lastPrice"))
        prev_price = _safe_float(item.get("prevPrice24h"))
        high_price = _safe_float(item.get("highPrice24h"), last_price)
        low_price = _safe_float(item.get("lowPrice24h"), last_price)
        change_pct = _safe_float(item.get("price24hPcnt")) * 100.0
        price_change = last_price - prev_price if prev_price > 0 else 0.0
        normalized.append(
            {
                "symbol": symbol,
                "priceChangePercent": str(change_pct),
                "priceChange": str(price_change),
                "quoteVolume": str(_safe_float(item.get("turnover24h"))),
                "lastPrice": str(last_price),
                "highPrice": str(high_price),
                "lowPrice": str(low_price),
                "count": "0",
                "openInterest": str(_safe_float(item.get("openInterest"))),
                "lastFundingRate": str(_safe_float(item.get("fundingRate"))),
                "provider": "bybit",
            }
        )
    if not normalized:
        raise PublicMarketDataError("bybit returned no linear USDT tickers")
    return normalized


def _fetch_okx_tickers() -> List[Dict[str, Any]]:
    data = _public_get_json("https://www.okx.com/api/v5/market/tickers", params={"instType": "SWAP"}, provider="okx")
    rows = (data or {}).get("data") or []
    normalized: List[Dict[str, Any]] = []
    for item in rows:
        inst_id = str(item.get("instId", "")).upper().strip()
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        last_price = _safe_float(item.get("last"))
        open_24h = _safe_float(item.get("open24h"))
        high_price = _safe_float(item.get("high24h"), last_price)
        low_price = _safe_float(item.get("low24h"), last_price)
        price_change = last_price - open_24h if open_24h > 0 else 0.0
        change_pct = ((last_price - open_24h) / open_24h * 100.0) if open_24h > 0 else 0.0
        normalized.append(
            {
                "symbol": _okx_inst_id_to_symbol(inst_id),
                "priceChangePercent": str(change_pct),
                "priceChange": str(price_change),
                "quoteVolume": str(_safe_float(item.get("volCcy24h"))),
                "lastPrice": str(last_price),
                "highPrice": str(high_price),
                "lowPrice": str(low_price),
                "count": "0",
                "provider": "okx",
            }
        )
    if not normalized:
        raise PublicMarketDataError("okx returned no USDT swap tickers")
    return normalized


def _fetch_binance_tickers() -> List[Dict[str, Any]]:
    data = _public_get_json("https://fapi.binance.com/fapi/v1/ticker/24hr", provider="binance")
    if not isinstance(data, list) or not data:
        raise PublicMarketDataError("binance returned no futures tickers")
    for row in data:
        if isinstance(row, dict):
            row.setdefault("provider", "binance")
    return data


def _merge_ticker_payloads(provider_payloads: Sequence[ProviderResult]) -> List[Dict[str, Any]]:
    """Merge 24h ticker universes across public providers.

    Previous behavior returned the first successful provider. That was safe for
    latency, but it hid symbols that exist only on a later provider. The market
    page, watchlist and radar need coverage from the complete configured public
    provider chain while still preserving priority for duplicate symbols.

    Provider priority rule:
      - first provider in MARKET_DATA_PROVIDERS wins for duplicated symbols;
      - later providers fill coverage gaps only;
      - each row keeps its real source in ``provider`` and gets a tiny
        ``providers_available`` provenance list for diagnostics/UI.
    """
    by_symbol: Dict[str, Dict[str, Any]] = {}
    providers_by_symbol: Dict[str, List[str]] = {}

    for result in provider_payloads:
        provider = str(result.provider or "").lower().strip()
        payload = result.payload if isinstance(result.payload, list) else []
        for item in payload:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).upper().strip()
            if not symbol.endswith("USDT") or symbol.endswith("BUSD"):
                continue

            providers = providers_by_symbol.setdefault(symbol, [])
            if provider and provider not in providers:
                providers.append(provider)

            # Keep the highest priority provider row as the canonical row.
            if symbol in by_symbol:
                continue

            row = dict(item)
            row["symbol"] = symbol
            row.setdefault("provider", provider or "unknown")
            by_symbol[symbol] = row

    merged: List[Dict[str, Any]] = []
    for symbol, row in by_symbol.items():
        enriched = dict(row)
        enriched["providers_available"] = list(providers_by_symbol.get(symbol) or [])
        merged.append(enriched)

    # Deterministic and useful ordering: strongest quote volume first, then symbol.
    merged.sort(key=lambda item: (_safe_float(item.get("quoteVolume")), str(item.get("symbol", ""))), reverse=True)
    return merged


def get_public_24h_tickers(*, allow_fallback: bool = True) -> List[Dict[str, Any]]:
    key = "public:24h_tickers:merged:" + ",".join(_provider_order())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    errors: List[str] = []
    provider_payloads: List[ProviderResult] = []
    provider_counts: Dict[str, int] = {}

    for provider in _provider_order():
        skip_reason = _provider_skip_reason(provider)
        if skip_reason:
            errors.append(skip_reason)
            continue
        try:
            if provider == "bybit":
                payload = _fetch_bybit_tickers()
            elif provider == "okx":
                payload = _fetch_okx_tickers()
            elif provider == "binance":
                payload = _fetch_binance_tickers()
            else:
                continue
            provider_payloads.append(ProviderResult(provider=provider, payload=payload))
            provider_counts[provider] = len(payload) if isinstance(payload, list) else 0
        except Exception as exc:
            errors.append(f"{provider}: {exc}")
            logger.warning("⚠️ Market data provider falló en tickers | provider=%s error=%s", provider, exc)

    merged = _merge_ticker_payloads(provider_payloads)
    if merged:
        _cache_set(key, merged, _TTL_TICKERS)
        logger.info(
            "✅ Market data tickers merged | providers=%s | total=%s | counts=%s",
            ",".join([r.provider for r in provider_payloads]),
            len(merged),
            provider_counts,
        )
        return merged

    if allow_fallback:
        fallback = _fallback_24h_tickers()
        _cache_set(key, fallback, _TTL_TICKERS)
        logger.warning("⚠️ Usando fallback local de tickers | errors=%s", errors[:3])
        return fallback
    raise PublicMarketDataError("all ticker providers failed: " + " | ".join(errors))


def get_public_active_symbols(*, min_quote_volume: float = 0.0, allow_fallback: bool = True) -> List[str]:
    tickers = get_public_24h_tickers(allow_fallback=allow_fallback)
    rows: List[Tuple[str, float]] = []
    for item in tickers:
        symbol = str(item.get("symbol", "")).upper().strip()
        if not symbol.endswith("USDT") or symbol.endswith("BUSD"):
            continue
        quote_volume = _safe_float(item.get("quoteVolume"))
        if quote_volume >= float(min_quote_volume):
            rows.append((symbol, quote_volume))
    rows.sort(key=lambda x: x[1], reverse=True)
    symbols = [symbol for symbol, _ in rows]
    if symbols:
        return symbols
    return list(ACTIVE_SYMBOLS_FALLBACK) if allow_fallback else []


# --------------------------
# Symbol details
# --------------------------


def _find_cached_ticker(symbol: str) -> Dict[str, Any]:
    symbol = str(symbol or "").upper().strip()
    for item in get_public_24h_tickers(allow_fallback=True):
        if str(item.get("symbol", "")).upper().strip() == symbol:
            return dict(item)
    return {}


def _fetch_bybit_symbol_ticker(symbol: str) -> Dict[str, Any]:
    data = _public_get_json(
        "https://api.bybit.com/v5/market/tickers",
        params={"category": "linear", "symbol": symbol.upper()},
        provider="bybit",
    )
    rows = (((data or {}).get("result") or {}).get("list") or [])
    if not rows:
        raise PublicMarketDataError(f"bybit returned no ticker for {symbol}")
    return rows[0]


def _fetch_okx_funding(symbol: str) -> Dict[str, Any]:
    inst_id = _symbol_to_okx_inst_id(symbol)
    data = _public_get_json("https://www.okx.com/api/v5/public/funding-rate", params={"instId": inst_id}, provider="okx")
    rows = (data or {}).get("data") or []
    if not rows:
        raise PublicMarketDataError(f"okx returned no funding for {symbol}")
    return rows[0]


def _fetch_okx_open_interest(symbol: str) -> Dict[str, Any]:
    inst_id = _symbol_to_okx_inst_id(symbol)
    data = _public_get_json("https://www.okx.com/api/v5/public/open-interest", params={"instType": "SWAP", "instId": inst_id}, provider="okx")
    rows = (data or {}).get("data") or []
    if not rows:
        raise PublicMarketDataError(f"okx returned no open interest for {symbol}")
    return rows[0]


def get_public_premium_index(symbol: str) -> Dict[str, Any]:
    symbol = str(symbol or "").upper().strip()
    key = f"public:premium:{symbol}:" + ",".join(_provider_order())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    errors: List[str] = []
    for provider in _provider_order():
        skip_reason = _provider_skip_reason(provider)
        if skip_reason:
            errors.append(skip_reason)
            continue
        try:
            if provider == "bybit":
                item = _fetch_bybit_symbol_ticker(symbol)
                payload = {"symbol": symbol, "lastFundingRate": str(_safe_float(item.get("fundingRate"))), "provider": "bybit"}
            elif provider == "okx":
                item = _fetch_okx_funding(symbol)
                payload = {"symbol": symbol, "lastFundingRate": str(_safe_float(item.get("fundingRate"))), "provider": "okx"}
            elif provider == "binance":
                payload = _public_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": symbol}, provider="binance")
                if isinstance(payload, dict):
                    payload.setdefault("provider", "binance")
                else:
                    raise PublicMarketDataError("invalid binance premium payload")
            else:
                continue
            _cache_set(key, payload, _TTL_SYMBOL_DETAILS)
            return payload
        except Exception as exc:
            errors.append(f"{provider}: {exc}")
            logger.debug("Market premium provider failed | provider=%s symbol=%s error=%s", provider, symbol, exc)

    ticker = _find_cached_ticker(symbol)
    payload = {"symbol": symbol, "lastFundingRate": str(_safe_float(ticker.get("lastFundingRate"))), "provider": ticker.get("provider", "fallback")}
    _cache_set(key, payload, _TTL_SYMBOL_DETAILS)
    return payload


def get_public_open_interest(symbol: str) -> Dict[str, Any]:
    symbol = str(symbol or "").upper().strip()
    key = f"public:open_interest:{symbol}:" + ",".join(_provider_order())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    errors: List[str] = []
    for provider in _provider_order():
        skip_reason = _provider_skip_reason(provider)
        if skip_reason:
            errors.append(skip_reason)
            continue
        try:
            if provider == "bybit":
                item = _fetch_bybit_symbol_ticker(symbol)
                payload = {"symbol": symbol, "openInterest": str(_safe_float(item.get("openInterest"))), "provider": "bybit"}
            elif provider == "okx":
                item = _fetch_okx_open_interest(symbol)
                payload = {"symbol": symbol, "openInterest": str(_safe_float(item.get("oi"))), "provider": "okx"}
            elif provider == "binance":
                payload = _public_get_json("https://fapi.binance.com/fapi/v1/openInterest", params={"symbol": symbol}, provider="binance")
                if isinstance(payload, dict):
                    payload.setdefault("provider", "binance")
                else:
                    raise PublicMarketDataError("invalid binance open interest payload")
            else:
                continue
            _cache_set(key, payload, _TTL_SYMBOL_DETAILS)
            return payload
        except Exception as exc:
            errors.append(f"{provider}: {exc}")
            logger.debug("Market open-interest provider failed | provider=%s symbol=%s error=%s", provider, symbol, exc)

    ticker = _find_cached_ticker(symbol)
    payload = {"symbol": symbol, "openInterest": str(_safe_float(ticker.get("openInterest"))), "provider": ticker.get("provider", "fallback")}
    _cache_set(key, payload, _TTL_SYMBOL_DETAILS)
    return payload


# --------------------------
# Klines
# --------------------------


def _klines_to_df(rows: Sequence[Sequence[Any]], *, provider: str) -> pd.DataFrame:
    parsed: List[Dict[str, Any]] = []
    for row in rows:
        try:
            if provider == "okx":
                # [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
                ts = int(float(row[0]))
                open_v, high_v, low_v, close_v, volume_v = row[1], row[2], row[3], row[4], row[5]
            else:
                # Binance: [open_time,o,h,l,c,v,close_time,...]
                # Bybit:  [startTime,o,h,l,c,volume,turnover]
                ts = int(float(row[0]))
                open_v, high_v, low_v, close_v, volume_v = row[1], row[2], row[3], row[4], row[5]
            parsed.append(
                {
                    "open_time": pd.to_datetime(ts, unit="ms", utc=True),
                    "open": float(open_v),
                    "high": float(high_v),
                    "low": float(low_v),
                    "close": float(close_v),
                    "volume": float(volume_v),
                }
            )
        except Exception:
            continue

    if not parsed:
        raise PublicMarketDataError(f"{provider} returned no parseable klines")

    df = pd.DataFrame(parsed).sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last").reset_index(drop=True)
    if len(df) > 1:
        # close_time is inferred from the next open. For the final candle use the previous interval length.
        next_open = df["open_time"].shift(-1)
        inferred_delta = df["open_time"].diff().median()
        if pd.isna(inferred_delta):
            inferred_delta = pd.Timedelta(minutes=5)
        df["close_time"] = next_open.fillna(df["open_time"] + inferred_delta)
    else:
        df["close_time"] = df["open_time"]
    return df[["open_time", "close_time", "open", "high", "low", "close", "volume"]]


def _fetch_bybit_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    data = _public_get_json(
        "https://api.bybit.com/v5/market/kline",
        params={"category": "linear", "symbol": symbol.upper(), "interval": _interval_to_bybit(interval), "limit": int(limit)},
        provider="bybit",
    )
    rows = (((data or {}).get("result") or {}).get("list") or [])
    if not rows:
        raise PublicMarketDataError(f"bybit returned no klines for {symbol} {interval}")
    return _klines_to_df(rows, provider="bybit")


def _fetch_okx_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    data = _public_get_json(
        "https://www.okx.com/api/v5/market/candles",
        params={"instId": _symbol_to_okx_inst_id(symbol), "bar": _interval_to_okx(interval), "limit": int(limit)},
        provider="okx",
    )
    rows = (data or {}).get("data") or []
    if not rows:
        raise PublicMarketDataError(f"okx returned no klines for {symbol} {interval}")
    return _klines_to_df(rows, provider="okx")


def _fetch_binance_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    data = _public_get_json(
        "https://fapi.binance.com/fapi/v1/klines",
        params={"symbol": symbol.upper(), "interval": str(interval).lower(), "limit": int(limit)},
        provider="binance",
    )
    if not isinstance(data, list) or not data:
        raise PublicMarketDataError(f"binance returned no klines for {symbol} {interval}")
    return _klines_to_df(data, provider="binance")


def get_public_klines_df(symbol: str, interval: str, limit: int = 220) -> pd.DataFrame:
    symbol = str(symbol or "").upper().strip()
    interval_key = str(interval or "").strip().lower()
    key = f"public:klines:{symbol}:{interval_key}:{int(limit)}:" + ",".join(_provider_order())
    cached = _cache_get(key)
    if cached is not None:
        return cached.copy(deep=False)

    errors: List[str] = []
    for provider in _provider_order():
        skip_reason = _provider_skip_reason(provider)
        if skip_reason:
            errors.append(skip_reason)
            continue
        try:
            if provider == "bybit":
                df = _fetch_bybit_klines(symbol, interval_key, limit)
            elif provider == "okx":
                df = _fetch_okx_klines(symbol, interval_key, limit)
            elif provider == "binance":
                df = _fetch_binance_klines(symbol, interval_key, limit)
            else:
                continue
            _cache_set(key, df, _TTL_KLINES)
            return df.copy(deep=False)
        except Exception as exc:
            errors.append(f"{provider}: {exc}")
            logger.debug("Market kline provider failed | provider=%s symbol=%s interval=%s error=%s", provider, symbol, interval_key, exc)
    raise PublicMarketDataError(f"all kline providers failed for {symbol} {interval_key}: " + " | ".join(errors))



def get_public_current_price(symbol: str) -> float:
    """Return the latest public price without calling Binance directly.

    Uses the same provider priority as the scanner. This is intentionally backed
    by the cached 24h ticker payload, so signal creation/evaluation does not add
    extra network pressure per symbol when the scanner already warmed the cache.
    """
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        raise PublicMarketDataError("missing symbol for current price")
    ticker = _find_cached_ticker(symbol)
    price = _safe_float(ticker.get("lastPrice"))
    if price > 0:
        return price
    errors: List[str] = []
    for provider in _provider_order():
        skip_reason = _provider_skip_reason(provider)
        if skip_reason:
            errors.append(skip_reason)
            continue
        try:
            if provider == "bybit":
                item = _fetch_bybit_symbol_ticker(symbol)
                price = _safe_float(item.get("lastPrice"))
            elif provider == "okx":
                data = _public_get_json("https://www.okx.com/api/v5/market/ticker", params={"instId": _symbol_to_okx_inst_id(symbol)}, provider="okx")
                rows = (data or {}).get("data") or []
                if not rows:
                    raise PublicMarketDataError(f"okx returned no ticker for {symbol}")
                price = _safe_float(rows[0].get("last"))
            elif provider == "binance":
                data = _public_get_json("https://fapi.binance.com/fapi/v1/ticker/price", params={"symbol": symbol}, provider="binance")
                price = _safe_float((data or {}).get("price"))
            else:
                continue
            if price > 0:
                return float(price)
            raise PublicMarketDataError(f"{provider} returned invalid price for {symbol}")
        except Exception as exc:
            errors.append(f"{provider}: {exc}")
            logger.debug("Market price provider failed | provider=%s symbol=%s error=%s", provider, symbol, exc)
    raise PublicMarketDataError(f"all price providers failed for {symbol}: " + " | ".join(errors))


def _interval_ms(interval: str) -> int:
    key = str(interval or "").strip().lower()
    mapping = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "2h": 7_200_000,
        "4h": 14_400_000,
        "1d": 86_400_000,
    }
    return mapping.get(key, 60_000)


def _df_to_binance_like_rows(df: pd.DataFrame) -> List[List[Any]]:
    if df is None or df.empty:
        return []
    rows: List[List[Any]] = []
    for _, item in df.sort_values("open_time").iterrows():
        open_time = int(pd.Timestamp(item["open_time"]).timestamp() * 1000)
        close_time = int(pd.Timestamp(item.get("close_time", item["open_time"])).timestamp() * 1000)
        rows.append([
            open_time,
            str(float(item["open"])),
            str(float(item["high"])),
            str(float(item["low"])),
            str(float(item["close"])),
            str(float(item["volume"])),
            close_time,
        ])
    return rows


def _filter_df_between(df: pd.DataFrame, start_ms: int, end_ms: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    open_ms = (pd.to_datetime(df["open_time"], utc=True).astype("int64") // 1_000_000)
    return df[(open_ms >= int(start_ms)) & (open_ms <= int(end_ms))].copy()


def _fetch_bybit_klines_between(symbol: str, interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    interval_key = str(interval or "1m").lower()
    step_ms = _interval_ms(interval_key)
    cursor = int(start_ms)
    chunks: List[pd.DataFrame] = []
    safety = 0
    while cursor < int(end_ms) and safety < 20:
        safety += 1
        remaining = max(1, int((int(end_ms) - cursor) / step_ms) + 2)
        limit = min(1000, remaining)
        chunk_end = min(int(end_ms), cursor + (limit * step_ms))
        data = _public_get_json(
            "https://api.bybit.com/v5/market/kline",
            params={
                "category": "linear",
                "symbol": symbol.upper(),
                "interval": _interval_to_bybit(interval_key),
                "start": cursor,
                "end": chunk_end,
                "limit": limit,
            },
            provider="bybit",
        )
        rows = (((data or {}).get("result") or {}).get("list") or [])
        if not rows:
            break
        df = _filter_df_between(_klines_to_df(rows, provider="bybit"), start_ms, end_ms)
        if not df.empty:
            chunks.append(df)
            last_ms = int(pd.Timestamp(df["open_time"].max()).timestamp() * 1000)
            next_cursor = last_ms + step_ms
        else:
            next_cursor = chunk_end + step_ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(rows) < limit:
            break
    if not chunks:
        raise PublicMarketDataError(f"bybit returned no klines between for {symbol} {interval_key}")
    return pd.concat(chunks, ignore_index=True).sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last").reset_index(drop=True)


def _fetch_recent_klines_between(symbol: str, interval: str, start_ms: int, end_ms: int, *, provider: str) -> pd.DataFrame:
    interval_key = str(interval or "1m").lower()
    needed = max(2, int((int(end_ms) - int(start_ms)) / _interval_ms(interval_key)) + 5)
    limit = min(1000 if provider != "okx" else 300, needed)
    if provider == "okx":
        df = _fetch_okx_klines(symbol, interval_key, limit)
    elif provider == "binance":
        df = _fetch_binance_klines(symbol, interval_key, limit)
    else:
        df = _fetch_bybit_klines(symbol, interval_key, limit)
    filtered = _filter_df_between(df, start_ms, end_ms)
    if filtered.empty:
        raise PublicMarketDataError(f"{provider} returned no recent klines in requested window for {symbol} {interval_key}")
    return filtered


def get_public_klines_between_rows(symbol: str, start_dt: Any, end_dt: Any, interval: str = "1m") -> List[List[Any]]:
    """Return Binance-like kline rows for signal lifecycle evaluation.

    This replaces the old app.signals direct Binance /fapi/v1/klines call. It is
    optimized for short lifecycle windows (pending entry / TP / SL checks) and
    uses provider failover without adding calls to the scanner path.
    """
    symbol = str(symbol or "").upper().strip()
    start_ts = pd.Timestamp(start_dt)
    end_ts = pd.Timestamp(end_dt)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    start_ms = int(start_ts.timestamp() * 1000)
    end_ms = int(end_ts.timestamp() * 1000)
    interval_key = str(interval or "1m").strip().lower()
    if not symbol or start_ms >= end_ms:
        return []

    bucket_start = start_ms - (start_ms % _interval_ms(interval_key))
    bucket_end = end_ms - (end_ms % _interval_ms(interval_key))
    key = f"public:klines_between:{symbol}:{interval_key}:{bucket_start}:{bucket_end}:" + ",".join(_provider_order())
    cached = _cache_get(key)
    if cached is not None:
        return list(cached)

    errors: List[str] = []
    for provider in _provider_order():
        skip_reason = _provider_skip_reason(provider)
        if skip_reason:
            errors.append(skip_reason)
            continue
        try:
            if provider == "bybit":
                df = _fetch_bybit_klines_between(symbol, interval_key, start_ms, end_ms)
            elif provider in {"okx", "binance"}:
                df = _fetch_recent_klines_between(symbol, interval_key, start_ms, end_ms, provider=provider)
            else:
                continue
            rows = _df_to_binance_like_rows(_filter_df_between(df, start_ms, end_ms))
            if not rows:
                raise PublicMarketDataError(f"{provider} returned no rows after filtering")
            # Short TTL: lifecycle checks are time-sensitive but repeated often by scheduler.
            _cache_set(key, rows, max(5, min(_TTL_KLINES, 20)))
            return rows
        except Exception as exc:
            errors.append(f"{provider}: {exc}")
            logger.debug("Market kline-range provider failed | provider=%s symbol=%s interval=%s error=%s", provider, symbol, interval_key, exc)
    raise PublicMarketDataError(f"all kline-range providers failed for {symbol} {interval_key}: " + " | ".join(errors))

def get_valid_public_symbols() -> set[str]:
    return set(get_public_active_symbols(min_quote_volume=0.0, allow_fallback=True))
