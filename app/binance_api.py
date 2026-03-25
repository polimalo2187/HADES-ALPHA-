# app/binance_api.py
# Utilidades para consumir endpoints públicos de Binance Futures (USDT-M)
# Enfocado en estabilidad: timeouts + cache simple en memoria.

from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Tuple

import requests


# Endpoints públicos (USDT-M Futures)
FAPI_24H_TICKER = "https://fapi.binance.com/fapi/v1/ticker/24hr"
FAPI_PREMIUM_INDEX = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
FAPI_OPEN_INTEREST = "https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"

# Cache en memoria (por proceso). Evita spamear Binance si muchos usuarios tocan botones a la vez.
_CACHE: Dict[str, Tuple[float, Any]] = {}

# TTLs (segundos)
_TTL_TICKERS = 20          # datos generales 24h
_TTL_SYMBOL_DETAILS = 60   # funding / open interest por símbolo

# Radar cooldown / rotación
_RADAR_RECENT_SYMBOLS: Dict[str, float] = {}
_RADAR_SYMBOL_COOLDOWN_SECONDS = 1800  # 30 minutos


def _get_json(url: str, timeout: int = 10) -> Any:
    """GET JSON con tolerancia a fallos.
    - Devuelve [] o {} si falla la petición.
    - Evita que un fallo de red tumbe el bot.
    """
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        # devolver un tipo seguro; la mayoría de endpoints aquí devuelven lista/dict
        return []


def _cache_get(key: str) -> Any | None:
    now = time.time()
    item = _CACHE.get(key)
    if not item:
        return None
    expires_at, value = item
    if now >= expires_at:
        _CACHE.pop(key, None)
        return None
    return value


def _cache_set(key: str, value: Any, ttl_seconds: int) -> None:
    _CACHE[key] = (time.time() + ttl_seconds, value)


def _prune_radar_recent_symbols() -> None:
    now = time.time()
    expired = [sym for sym, ts in _RADAR_RECENT_SYMBOLS.items() if (now - ts) >= _RADAR_SYMBOL_COOLDOWN_SECONDS]
    for sym in expired:
        _RADAR_RECENT_SYMBOLS.pop(sym, None)


def _mark_radar_symbols(symbols: List[str]) -> None:
    now = time.time()
    for sym in symbols:
        _RADAR_RECENT_SYMBOLS[sym] = now


def _symbol_in_radar_cooldown(symbol: str) -> bool:
    _prune_radar_recent_symbols()
    ts = _RADAR_RECENT_SYMBOLS.get(symbol)
    if ts is None:
        return False
    return (time.time() - ts) < _RADAR_SYMBOL_COOLDOWN_SECONDS


def get_futures_24h_tickers() -> List[Dict[str, Any]]:
    """Devuelve el listado completo de tickers 24h (cached)."""
    key = "futures_24h_tickers"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data = _get_json(FAPI_24H_TICKER, timeout=10)
    if not isinstance(data, list):
        return []
    _cache_set(key, data, _TTL_TICKERS)
    return data


def _is_usdt_symbol(symbol: str) -> bool:
    # USDT-M: pares que terminan en USDT (ignoramos BUSD por si acaso)
    return symbol.endswith("USDT") and not symbol.endswith("BUSD")


def get_top_movers_usdtm(limit: int = 10, *, kind: str = "gainers") -> List[Dict[str, Any]]:
    """
    kind:
      - 'gainers' (mayores subidas 24h)
      - 'losers' (mayores caídas 24h)
      - 'absolute' (mayor movimiento absoluto 24h)
    """
    tickers = [t for t in get_futures_24h_tickers() if _is_usdt_symbol(t.get("symbol", ""))]

    def pct(x: Dict[str, Any]) -> float:
        try:
            return float(x.get("priceChangePercent", 0.0))
        except Exception:
            return 0.0

    if kind == "losers":
        tickers.sort(key=pct)  # más negativo primero
    elif kind == "absolute":
        tickers.sort(key=lambda x: abs(pct(x)), reverse=True)
    else:
        tickers.sort(key=pct, reverse=True)

    return tickers[: max(1, int(limit))]


def get_premium_index(symbol: str) -> Dict[str, Any]:
    """Datos de premiumIndex (incluye lastFundingRate). Cached por símbolo."""
    symbol = symbol.upper().strip()
    key = f"premium_index:{symbol}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data = _get_json(FAPI_PREMIUM_INDEX.format(symbol=symbol), timeout=10)
    _cache_set(key, data, _TTL_SYMBOL_DETAILS)
    return data


def get_open_interest(symbol: str) -> Dict[str, Any]:
    """Open interest actual. Cached por símbolo."""
    symbol = symbol.upper().strip()
    key = f"open_interest:{symbol}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data = _get_json(FAPI_OPEN_INTEREST.format(symbol=symbol), timeout=10)
    _cache_set(key, data, _TTL_SYMBOL_DETAILS)
    return data


def get_radar_opportunities(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Radar mejorado y seguro:
    - sigue usando tickers 24h (estable)
    - calcula un score por movimiento + volumen + actividad
    - evita repetir siempre los mismos símbolos con cooldown
    - añade momentum textual
    Devuelve lista de dicts con campos extra:
      score, direction, change_pct, quote_volume, trades, momentum
    """
    tickers = [t for t in get_futures_24h_tickers() if _is_usdt_symbol(t.get("symbol", ""))]

    rows: List[Dict[str, Any]] = []
    for t in tickers:
        try:
            change_pct = float(t.get("priceChangePercent", 0.0))
        except Exception:
            change_pct = 0.0
        try:
            quote_volume = float(t.get("quoteVolume", 0.0))
        except Exception:
            quote_volume = 0.0
        try:
            trades = float(t.get("count", 0.0))
        except Exception:
            trades = 0.0
        try:
            last_price = float(t.get("lastPrice", 0.0))
        except Exception:
            last_price = 0.0

        symbol = t.get("symbol")
        if not symbol:
            continue

        rows.append(
            {
                "symbol": symbol,
                "change_pct": change_pct,
                "abs_change_pct": abs(change_pct),
                "quote_volume": quote_volume,
                "trades": trades,
                "last_price": last_price,
            }
        )

    if not rows:
        return []

    # ranks (0..1)
    def _rank(values: List[float]) -> Dict[int, float]:
        sorted_idx = sorted(range(len(values)), key=lambda i: values[i])
        ranks = [0.0] * len(values)
        n = max(1, len(values) - 1)
        for r, i in enumerate(sorted_idx):
            ranks[i] = r / n
        return {i: ranks[i] for i in range(len(values))}

    abs_changes = [r["abs_change_pct"] for r in rows]
    volumes = [r["quote_volume"] for r in rows]
    trades = [r["trades"] for r in rows]

    r_abs = _rank(abs_changes)
    r_vol = _rank(volumes)
    r_trd = _rank(trades)

    # score base
    for i, r in enumerate(rows):
        combined = (0.45 * r_abs[i]) + (0.35 * r_vol[i]) + (0.20 * r_trd[i])
        score = int(round(100 * combined))
        r["score"] = max(1, min(100, score))
        r["direction"] = "LONG" if r["change_pct"] >= 0 else "SHORT"

        strength = r["abs_change_pct"]
        if strength >= 12:
            r["momentum"] = "Muy alto"
        elif strength >= 7:
            r["momentum"] = "Alto"
        elif strength >= 3:
            r["momentum"] = "Medio"
        else:
            r["momentum"] = "Bajo"

        # penalización por repetición reciente en radar
        penalty = 18 if _symbol_in_radar_cooldown(r["symbol"]) else 0
        r["final_score"] = max(1, r["score"] - penalty)

    # ordenar por final_score
    rows.sort(
        key=lambda x: (x["final_score"], x["abs_change_pct"], x["quote_volume"], x["trades"]),
        reverse=True,
    )

    # selecciona con mezcla LONG/SHORT para que no se vea fijo
    longs = [r for r in rows if r["direction"] == "LONG"]
    shorts = [r for r in rows if r["direction"] == "SHORT"]

    selected: List[Dict[str, Any]] = []
    used = set()

    def _take_from(pool: List[Dict[str, Any]], max_items: int) -> None:
        taken = 0
        for row in pool:
            if len(selected) >= limit or taken >= max_items:
                return
            sym = row["symbol"]
            if sym in used:
                continue
            selected.append(row)
            used.add(sym)
            taken += 1

    # mitad general, luego mezcla long/short
    half = max(1, limit // 2)
    _take_from(rows, half)
    _take_from(longs, max(1, limit // 4))
    _take_from(shorts, max(1, limit // 4))

    # completar si faltan
    if len(selected) < limit:
        for row in rows:
            if len(selected) >= limit:
                break
            sym = row["symbol"]
            if sym in used:
                continue
            selected.append(row)
            used.add(sym)

    # devolver score final ya ajustado
    for r in selected:
        r["score"] = r["final_score"]
        r["quoteVolume"] = r["quote_volume"]
        r["lastPrice"] = r["last_price"]
        r.pop("final_score", None)

    _mark_radar_symbols([r["symbol"] for r in selected])

    return selected[: max(1, int(limit))]
