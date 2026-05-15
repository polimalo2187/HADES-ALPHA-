"""
Rutas de Mercado - Datos de Exchanges en Tiempo Real
Sin servicios de terceros, conexión directa a APIs públicas
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
import aiohttp

from backend.database import get_database
from backend.config import settings

router = APIRouter(prefix="/market", tags=["market"])


# =========================
# CONFIGURACIÓN DE EXCHANGES
# =========================
EXCHANGE_ENDPOINTS = {
    "binance": "https://api.binance.com/api/v3",
    "bybit": "https://api.bybit.com/v5",
    "okx": "https://www.okx.com/api/v5",
    "kucoin": "https://api.kucoin.com/api/v1",
}

DEFAULT_EXCHANGE = "binance"
DEFAULT_TIMEOUT = 10  # segundos


async def fetch_from_exchange(
    session: aiohttp.ClientSession,
    exchange: str,
    endpoint: str,
    params: Optional[Dict] = None,
) -> Optional[Dict]:
    """Obtiene datos de un exchange específico"""
    
    base_url = EXCHANGE_ENDPOINTS.get(exchange)
    if not base_url:
        return None
    
    url = f"{base_url}{endpoint}"
    
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)) as response:
            if response.status == 200:
                data = await response.json()
                
                # Normalizar respuesta según exchange
                if exchange == "binance":
                    return data
                elif exchange == "bybit":
                    return data.get("result", {})
                elif exchange == "okx":
                    result = data.get("data", [])
                    return result[0] if result else {}
                elif exchange == "kucoin":
                    return data.get("data", {})
                    
    except Exception as e:
        print(f"Error fetching from {exchange}: {e}")
    
    return None


async def fetch_all_exchanges(endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Obtiene datos de todos los exchanges simultáneamente"""
    
    async with aiohttp.ClientSession() as session:
        tasks = {
            exchange: fetch_from_exchange(session, exchange, endpoint, params)
            for exchange in EXCHANGE_ENDPOINTS.keys()
        }
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        return dict(zip(tasks.keys(), results))


# =========================
# RUTAS DE MERCADO
# =========================

@router.get("/price/{symbol}")
async def get_price(
    symbol: str,
    exchange: str = Query(default=DEFAULT_EXCHANGE, description="Exchange a consultar"),
    db=Depends(get_database),
):
    """Obtiene el precio actual de un símbolo"""
    
    # Normalizar símbolo (ej: BTCUSDT)
    symbol = symbol.upper().replace("/", "").replace("-", "")
    
    if exchange not in EXCHANGE_ENDPOINTS:
        raise HTTPException(status_code=400, detail=f"Exchange no soportado: {exchange}")
    
    async with aiohttp.ClientSession() as session:
        # Endpoint específico por exchange
        if exchange == "binance":
            endpoint = "/ticker/price"
            params = {"symbol": symbol}
        elif exchange == "bybit":
            endpoint = "/market/tickers"
            params = {"category": "linear", "symbol": symbol}
        elif exchange == "okx":
            endpoint = "/market/ticker"
            params = {"instId": symbol}
        elif exchange == "kucoin":
            endpoint = f"/market/orderbook/level1?symbol={symbol}"
            params = {}
        else:
            raise HTTPException(status_code=400, detail="Exchange no implementado")
        
        data = await fetch_from_exchange(session, exchange, endpoint, params)
        
        if not data:
            raise HTTPException(status_code=404, detail=f"No se pudo obtener precio de {exchange}")
        
        # Extraer precio según formato del exchange
        price = None
        if exchange == "binance":
            price = float(data.get("price", 0))
        elif exchange == "bybit":
            ticker_list = data.get("list", [])
            if ticker_list:
                price = float(ticker_list[0].get("lastPrice", 0))
        elif exchange == "okx":
            price = float(data.get("last", 0))
        elif exchange == "kucoin":
            price = float(data.get("price", 0))
        
        if not price or price <= 0:
            raise HTTPException(status_code=404, detail=f"Símbolo no encontrado: {symbol}")
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "price": price,
            "timestamp": datetime.utcnow().isoformat(),
        }


@router.get("/prices/{symbol}")
async def get_prices_all_exchanges(
    symbol: str,
    db=Depends(get_database),
):
    """Obtiene el precio de un símbolo en todos los exchanges"""
    
    symbol = symbol.upper().replace("/", "").replace("-", "")
    
    results = {}
    errors = []
    
    async with aiohttp.ClientSession() as session:
        for exchange in EXCHANGE_ENDPOINTS.keys():
            try:
                if exchange == "binance":
                    endpoint = "/ticker/price"
                    params = {"symbol": symbol}
                elif exchange == "bybit":
                    endpoint = "/market/tickers"
                    params = {"category": "linear", "symbol": symbol}
                elif exchange == "okx":
                    endpoint = "/market/ticker"
                    params = {"instId": symbol}
                elif exchange == "kucoin":
                    endpoint = f"/market/orderbook/level1?symbol={symbol}"
                    params = {}
                else:
                    continue
                
                data = await fetch_from_exchange(session, exchange, endpoint, params)
                
                if data:
                    price = None
                    if exchange == "binance":
                        price = float(data.get("price", 0))
                    elif exchange == "bybit":
                        ticker_list = data.get("list", [])
                        if ticker_list:
                            price = float(ticker_list[0].get("lastPrice", 0))
                    elif exchange == "okx":
                        price = float(data.get("last", 0))
                    elif exchange == "kucoin":
                        price = float(data.get("price", 0))
                    
                    if price and price > 0:
                        results[exchange] = price
                else:
                    errors.append(exchange)
                    
            except Exception as e:
                errors.append(f"{exchange}: {str(e)}")
    
    if not results:
        raise HTTPException(status_code=404, detail=f"No se encontró precio para {symbol} en ningún exchange")
    
    # Calcular estadísticas
    prices = list(results.values())
    avg_price = sum(prices) / len(prices)
    min_price = min(prices)
    max_price = max(prices)
    spread = ((max_price - min_price) / avg_price) * 100
    
    return {
        "symbol": symbol,
        "prices": results,
        "statistics": {
            "average": avg_price,
            "min": min_price,
            "max": max_price,
            "spread_percent": round(spread, 4),
        },
        "errors": errors if errors else None,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/orderbook/{symbol}")
async def get_orderbook(
    symbol: str,
    exchange: str = Query(default=DEFAULT_EXCHANGE),
    limit: int = Query(default=20, ge=1, le=100),
    db=Depends(get_database),
):
    """Obtiene el libro de órdenes de un símbolo"""
    
    symbol = symbol.upper().replace("/", "").replace("-", "")
    
    if exchange not in EXCHANGE_ENDPOINTS:
        raise HTTPException(status_code=400, detail=f"Exchange no soportado: {exchange}")
    
    async with aiohttp.ClientSession() as session:
        if exchange == "binance":
            endpoint = "/depth"
            params = {"symbol": symbol, "limit": limit}
        elif exchange == "bybit":
            endpoint = "/market/orderbook"
            params = {"category": "linear", "symbol": symbol, "limit": limit}
        elif exchange == "okx":
            endpoint = "/market/books"
            params = {"instId": symbol, "sz": limit}
        elif exchange == "kucoin":
            endpoint = f"/market/orderbook/level2_20?symbol={symbol}"
            params = {}
        else:
            raise HTTPException(status_code=400, detail="Exchange no implementado")
        
        data = await fetch_from_exchange(session, exchange, endpoint, params)
        
        if not data:
            raise HTTPException(status_code=404, detail=f"No se pudo obtener orderbook de {exchange}")
        
        # Normalizar respuesta
        bids = []
        asks = []
        
        if exchange == "binance":
            bids = [[float(p), float(q)] for p, q in data.get("bids", [])[:limit]]
            asks = [[float(p), float(q)] for p, q in data.get("asks", [])[:limit]]
        elif exchange == "bybit":
            orderbook = data.get("bids", []) + data.get("asks", [])
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", [])[:limit]]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", [])[:limit]]
        elif exchange == "okx":
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", [])[:limit]]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", [])[:limit]]
        elif exchange == "kucoin":
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", [])[:limit]]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", [])[:limit]]
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "bids": bids,
            "asks": asks,
            "spread": bids[0][0] - asks[0][0] if bids and asks else None,
            "mid_price": (bids[0][0] + asks[0][0]) / 2 if bids and asks else None,
            "timestamp": datetime.utcnow().isoformat(),
        }


@router.get("/klines/{symbol}")
async def get_klines(
    symbol: str,
    interval: str = Query(default="1h", description="Intervalo: 1m, 5m, 15m, 1h, 4h, 1d"),
    limit: int = Query(default=100, ge=1, le=1000),
    exchange: str = Query(default=DEFAULT_EXCHANGE),
    db=Depends(get_database),
):
    """Obtiene velas japonesas (klines/candlesticks)"""
    
    symbol = symbol.upper().replace("/", "").replace("-", "")
    
    if exchange not in EXCHANGE_ENDPOINTS:
        raise HTTPException(status_code=400, detail=f"Exchange no soportado: {exchange}")
    
    # Mapear intervalos
    interval_map = {
        "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m",
        "30m": "30m", "1h": "1h", "2h": "2h", "4h": "4h",
        "6h": "6h", "12h": "12h", "1d": "1d", "3d": "3d",
        "1w": "1w", "1M": "1M",
    }
    
    tf = interval_map.get(interval, "1h")
    
    async with aiohttp.ClientSession() as session:
        if exchange == "binance":
            endpoint = "/klines"
            params = {"symbol": symbol, "interval": tf, "limit": limit}
        elif exchange == "bybit":
            endpoint = "/market/kline"
            params = {"category": "linear", "symbol": symbol, "interval": tf, "limit": limit}
        elif exchange == "okx":
            endpoint = "/market/candles"
            params = {"instId": symbol, "bar": tf, "limit": limit}
        elif exchange == "kucoin":
            endpoint = f"/market/candles?symbol={symbol}&type={tf}"
            params = {}
        else:
            raise HTTPException(status_code=400, detail="Exchange no implementado")
        
        data = await fetch_from_exchange(session, exchange, endpoint, params)
        
        if not data:
            raise HTTPException(status_code=404, detail=f"No se pudo obtener klines de {exchange}")
        
        # Normalizar formato de velas
        candles = []
        
        if exchange == "binance":
            # [open_time, open, high, low, close, volume, ...]
            for candle in data[:limit]:
                candles.append({
                    "timestamp": datetime.utcfromtimestamp(candle[0] / 1000).isoformat(),
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5]),
                })
        elif exchange == "bybit":
            candle_list = data.get("list", [])
            for candle in candle_list[:limit]:
                candles.append({
                    "timestamp": datetime.utcfromtimestamp(int(candle[0]) / 1000).isoformat(),
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5]),
                })
        elif exchange == "okx":
            for candle in data[:limit]:
                candles.append({
                    "timestamp": datetime.utcfromtimestamp(int(candle[0]) / 1000).isoformat(),
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5]),
                })
        elif exchange == "kucoin":
            for candle in data[:limit]:
                candles.append({
                    "timestamp": datetime.utcfromtimestamp(int(candle[0]) / 1000).isoformat(),
                    "open": float(candle[1]),
                    "close": float(candle[2]),
                    "high": float(candle[3]),
                    "low": float(candle[4]),
                    "volume": float(candle[5]),
                })
        
        return {
            "symbol": symbol,
            "exchange": exchange,
            "interval": tf,
            "candles": candles,
            "count": len(candles),
            "timestamp": datetime.utcnow().isoformat(),
        }


@router.get("/exchanges")
async def list_exchanges(db=Depends(get_database)):
    """Lista todos los exchanges disponibles"""
    
    return {
        "exchanges": [
            {
                "id": ex_id,
                "name": ex_id.capitalize(),
                "endpoint": EXCHANGE_ENDPOINTS[ex_id],
                "status": "active",
            }
            for ex_id in EXCHANGE_ENDPOINTS.keys()
        ],
        "default": DEFAULT_EXCHANGE,
    }


@router.get("/symbols")
async def list_symbols(
    exchange: str = Query(default=DEFAULT_EXCHANGE),
    quote_asset: str = Query(default="USDT", description="Filtrar por asset de cotización"),
    db=Depends(get_database),
):
    """Lista todos los símbolos disponibles en un exchange"""
    
    if exchange not in EXCHANGE_ENDPOINTS:
        raise HTTPException(status_code=400, detail=f"Exchange no soportado: {exchange}")
    
    async with aiohttp.ClientSession() as session:
        if exchange == "binance":
            endpoint = "/exchangeInfo"
            params = {}
        elif exchange == "bybit":
            endpoint = "/market/instruments-info"
            params = {"category": "linear"}
        elif exchange == "okx":
            endpoint = "/public/instruments"
            params = {"instType": "SPOT"}
        elif exchange == "kucoin":
            endpoint = "/symbols"
            params = {}
        else:
            raise HTTPException(status_code=400, detail="Exchange no implementado")
        
        data = await fetch_from_exchange(session, exchange, endpoint, params)
        
        if not data:
            raise HTTPException(status_code=404, detail=f"No se pudo obtener información de {exchange}")
        
        symbols = []
        
        if exchange == "binance":
            for s in data.get("symbols", []):
                if quote_asset and s.get("quoteAsset") != quote_asset:
                    continue
                symbols.append({
                    "symbol": s["symbol"],
                    "base": s["baseAsset"],
                    "quote": s["quoteAsset"],
                    "status": s["status"],
                })
        elif exchange == "bybit":
            for s in data.get("list", []):
                if quote_asset and quote_asset not in s.get("symbol", ""):
                    continue
                symbols.append({
                    "symbol": s["symbol"],
                    "base": s["baseCoin"],
                    "quote": s["quoteCoin"],
                    "status": s["status"],
                })
        elif exchange == "okx":
            for s in data:
                if quote_asset and quote_asset not in s.get("instId", ""):
                    continue
                symbols.append({
                    "symbol": s["instId"],
                    "base": s["baseCcy"],
                    "quote": s["quoteCcy"],
                    "status": s["state"],
                })
        elif exchange == "kucoin":
            for s in data:
                if quote_asset and quote_asset not in s.get("symbol", ""):
                    continue
                symbols.append({
                    "symbol": s["symbol"],
                    "base": s["baseCurrency"],
                    "quote": s["quoteCurrency"],
                    "status": s["enableTrading"],
                })
        
        return {
            "exchange": exchange,
            "symbols": symbols[:1000],  # Limitar a 1000 símbolos
            "total": len(symbols),
            "timestamp": datetime.utcnow().isoformat(),
        }


__all__ = ["router"]
