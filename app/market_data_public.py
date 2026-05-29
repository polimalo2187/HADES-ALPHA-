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


class PublicMarketDataError(RuntimeError):
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


def _public_get_json(url: str, *, params: Optional[dict] = None, timeout: int = PUBLIC_MARKET_TIMEOUT_SECONDS) -> Any:
    response = requests.get(url, params=params, timeout=timeout)
    if response.status_code == 451:
        raise PublicMarketDataError(f"restricted_location status=451 url={url} body={response.text[:160]}")
    response.raise_for_status()
    return response.json()


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
    data = _public_get_json("https://api.bybit.com/v5/market/tickers", params={"category": "linear"})
    rows = (((data or {}).get("result") or {}).get("list") or [])
    normalized: List[Dict[str, Any]] = []
    for item in rows:
        symbol = str(item.get("symbol", "")).upper().strip()
        if not symbol.endswith("USDT"):
            continue
        change_pct = _safe_float(item.get("price24hPcnt")) * 100.0
        normalized.append(
            {
                "symbol": symbol,
                "priceChangePercent": str(change_pct),
                "quoteVolume": str(_safe_float(item.get("turnover24h"))),
                "lastPrice": str(_safe_float(item.get("lastPrice"))),
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
    data = _public_get_json("https://www.okx.com/api/v5/market/tickers", params={"instType": "SWAP"})
    rows = (data or {}).get("data") or []
    normalized: List[Dict[str, Any]] = []
    for item in rows:
        inst_id = str(item.get("instId", "")).upper().strip()
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        last_price = _safe_float(item.get("last"))
        open_24h = _safe_float(item.get("open24h"))
        change_pct = ((last_price - open_24h) / open_24h * 100.0) if open_24h > 0 else 0.0
        normalized.append(
            {
                "symbol": _okx_inst_id_to_symbol(inst_id),
                "priceChangePercent": str(change_pct),
                "quoteVolume": str(_safe_float(item.get("volCcy24h"))),
                "lastPrice": str(last_price),
                "count": "0",
                "provider": "okx",
            }
        )
    if not normalized:
        raise PublicMarketDataError("okx returned no USDT swap tickers")
    return normalized


def _fetch_binance_tickers() -> List[Dict[str, Any]]:
    data = _public_get_json("https://fapi.binance.com/fapi/v1/ticker/24hr")
    if not isinstance(data, list) or not data:
        raise PublicMarketDataError("binance returned no futures tickers")
    for row in data:
        if isinstance(row, dict):
            row.setdefault("provider", "binance")
    return data


def get_public_24h_tickers(*, allow_fallback: bool = True) -> List[Dict[str, Any]]:
    key = "public:24h_tickers:" + ",".join(_provider_order())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    errors: List[str] = []
    for provider in _provider_order():
        try:
            if provider == "bybit":
                payload = _fetch_bybit_tickers()
            elif provider == "okx":
                payload = _fetch_okx_tickers()
            elif provider == "binance":
                payload = _fetch_binance_tickers()
            else:
                continue
            _cache_set(key, payload, _TTL_TICKERS)
            logger.info("✅ Market data tickers provider=%s count=%s", provider, len(payload))
            return payload
        except Exception as exc:
            errors.append(f"{provider}: {exc}")
            logger.warning("⚠️ Market data provider falló en tickers | provider=%s error=%s", provider, exc)

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
    )
    rows = (((data or {}).get("result") or {}).get("list") or [])
    if not rows:
        raise PublicMarketDataError(f"bybit returned no ticker for {symbol}")
    return rows[0]


def _fetch_okx_funding(symbol: str) -> Dict[str, Any]:
    inst_id = _symbol_to_okx_inst_id(symbol)
    data = _public_get_json("https://www.okx.com/api/v5/public/funding-rate", params={"instId": inst_id})
    rows = (data or {}).get("data") or []
    if not rows:
        raise PublicMarketDataError(f"okx returned no funding for {symbol}")
    return rows[0]


def _fetch_okx_open_interest(symbol: str) -> Dict[str, Any]:
    inst_id = _symbol_to_okx_inst_id(symbol)
    data = _public_get_json("https://www.okx.com/api/v5/public/open-interest", params={"instType": "SWAP", "instId": inst_id})
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
        try:
            if provider == "bybit":
                item = _fetch_bybit_symbol_ticker(symbol)
                payload = {"symbol": symbol, "lastFundingRate": str(_safe_float(item.get("fundingRate"))), "provider": "bybit"}
            elif provider == "okx":
                item = _fetch_okx_funding(symbol)
                payload = {"symbol": symbol, "lastFundingRate": str(_safe_float(item.get("fundingRate"))), "provider": "okx"}
            elif provider == "binance":
                payload = _public_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": symbol})
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
        try:
            if provider == "bybit":
                item = _fetch_bybit_symbol_ticker(symbol)
                payload = {"symbol": symbol, "openInterest": str(_safe_float(item.get("openInterest"))), "provider": "bybit"}
            elif provider == "okx":
                item = _fetch_okx_open_interest(symbol)
                payload = {"symbol": symbol, "openInterest": str(_safe_float(item.get("oi"))), "provider": "okx"}
            elif provider == "binance":
                payload = _public_get_json("https://fapi.binance.com/fapi/v1/openInterest", params={"symbol": symbol})
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
    )
    rows = (((data or {}).get("result") or {}).get("list") or [])
    if not rows:
        raise PublicMarketDataError(f"bybit returned no klines for {symbol} {interval}")
    return _klines_to_df(rows, provider="bybit")


def _fetch_okx_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    data = _public_get_json(
        "https://www.okx.com/api/v5/market/candles",
        params={"instId": _symbol_to_okx_inst_id(symbol), "bar": _interval_to_okx(interval), "limit": int(limit)},
    )
    rows = (data or {}).get("data") or []
    if not rows:
        raise PublicMarketDataError(f"okx returned no klines for {symbol} {interval}")
    return _klines_to_df(rows, provider="okx")


def _fetch_binance_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    data = _public_get_json(
        "https://fapi.binance.com/fapi/v1/klines",
        params={"symbol": symbol.upper(), "interval": str(interval).lower(), "limit": int(limit)},
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


def get_valid_public_symbols() -> set[str]:
    return set(get_public_active_symbols(min_quote_volume=0.0, allow_fallback=True))
