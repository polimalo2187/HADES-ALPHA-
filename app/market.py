
import requests
from datetime import datetime, timezone

BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"

def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def get_market_snapshot():

    try:
        r = requests.get(BINANCE_TICKER_URL, timeout=10)
        if r.status_code != 200:
            return None

        data = r.json()

    except Exception:
        return None

    btc = None
    eth = None

    gainers = []
    losers = []

    for item in data:
        symbol = item.get("symbol")

        if not symbol.endswith("USDT"):
            continue

        change = _safe_float(item.get("priceChangePercent"))
        volume = _safe_float(item.get("quoteVolume"))

        entry = {
            "symbol": symbol,
            "change": change,
            "volume": volume
        }

        if symbol == "BTCUSDT":
            btc = entry

        if symbol == "ETHUSDT":
            eth = entry

        gainers.append(entry)
        losers.append(entry)

    gainers.sort(key=lambda x: x["change"], reverse=True)
    losers.sort(key=lambda x: x["change"])

    return {
        "btc": btc,
        "eth": eth,
        "gainers": gainers[:5],
        "losers": losers[:5],
        "time": _now()
    }
