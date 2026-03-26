from __future__ import annotations

from datetime import datetime, timezone
from statistics import median
from typing import Any, Dict, List, Tuple

from app.binance_api import get_futures_24h_tickers, get_open_interest, get_premium_index


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _fmt_compact_number(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.0f}"


def _normalize_symbol_row(item: Dict[str, Any]) -> Dict[str, Any] | None:
    symbol = str(item.get("symbol", "")).upper()
    if not symbol.endswith("USDT") or symbol.endswith("BUSD"):
        return None

    return {
        "symbol": symbol,
        "change": _safe_float(item.get("priceChangePercent")),
        "quote_volume": _safe_float(item.get("quoteVolume")),
        "last_price": _safe_float(item.get("lastPrice")),
        "trades": _safe_float(item.get("count")),
    }


def _classify_bias(adv_ratio: float, avg_change: float, btc_change: float, eth_change: float) -> Tuple[str, str]:
    majors_avg = (btc_change + eth_change) / 2.0

    if adv_ratio >= 0.62 and avg_change >= 0.75 and majors_avg >= 0.75:
        return "Alcista", "LONGS"
    if adv_ratio <= 0.38 and avg_change <= -0.75 and majors_avg <= -0.75:
        return "Bajista", "SHORTS"
    if adv_ratio >= 0.56 and avg_change >= 0.20:
        return "Alcista leve", "LONGS selectivos"
    if adv_ratio <= 0.44 and avg_change <= -0.20:
        return "Bajista leve", "SHORTS selectivos"
    return "Neutral", "Selectivo / mixto"


def _classify_regime(adv_ratio: float, median_abs_change: float, top_abs_change: float) -> str:
    if median_abs_change < 0.90:
        return "Compresión"
    if 0.45 <= adv_ratio <= 0.55 and median_abs_change < 1.70:
        return "Lateral"
    if median_abs_change >= 3.20 or top_abs_change >= 12.0:
        return "Expansión"
    if adv_ratio >= 0.58 or adv_ratio <= 0.42:
        return "Tendencial"
    return "Mixto"


def _classify_volatility(median_abs_change: float, top_abs_change: float) -> str:
    if median_abs_change >= 3.20 or top_abs_change >= 12.0:
        return "Alta"
    if median_abs_change >= 1.60 or top_abs_change >= 7.0:
        return "Media"
    return "Baja"


def _classify_participation(active_ratio: float) -> str:
    if active_ratio >= 0.45:
        return "Amplia"
    if active_ratio >= 0.25:
        return "Media"
    return "Reducida"


def _classify_environment(bias: str, regime: str, volatility: str, participation: str) -> Tuple[str, str]:
    if bias in {"Alcista", "Bajista"} and regime in {"Tendencial", "Expansión"} and participation != "Reducida":
        return "Favorable", (
            "Favorece seguir la dirección dominante en retrocesos limpios."
            if bias == "Alcista"
            else "Favorece buscar shorts en rebotes y rupturas confirmadas."
        )

    if regime in {"Compresión", "Lateral"} and volatility == "Baja":
        return "Selectivo", "Reduce exposición y exige confirmación extra antes de entrar."

    if regime == "Expansión" and bias == "Neutral":
        return "Peligroso", "Hay movimiento fuerte pero sin lectura clara; evita entradas agresivas."

    return "Mixto", "Opera selectivo: prioriza estructura limpia y evita perseguir precio."


def _major_symbol_block(symbol: str, rows_by_symbol: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    row = rows_by_symbol.get(symbol, {"symbol": symbol, "change": 0.0, "quote_volume": 0.0})
    premium = get_premium_index(symbol) or {}
    oi = get_open_interest(symbol) or {}

    funding_rate = _safe_float(premium.get("lastFundingRate")) * 100.0
    open_interest = _safe_float(oi.get("openInterest"))

    return {
        "symbol": symbol,
        "change": row.get("change", 0.0),
        "quote_volume": row.get("quote_volume", 0.0),
        "funding_rate_pct": funding_rate,
        "open_interest": open_interest,
    }


def get_market_state_snapshot() -> Dict[str, Any] | None:
    raw = get_futures_24h_tickers()
    if not raw:
        return None

    parsed: List[Dict[str, Any]] = []
    for item in raw:
        row = _normalize_symbol_row(item)
        if row:
            parsed.append(row)

    if not parsed:
        return None

    rows_by_symbol = {row["symbol"]: row for row in parsed}
    universe = len(parsed)

    advancers = sum(1 for row in parsed if row["change"] > 0.15)
    decliners = sum(1 for row in parsed if row["change"] < -0.15)
    flat = max(0, universe - advancers - decliners)
    adv_ratio = advancers / universe if universe else 0.0

    abs_changes = [abs(row["change"]) for row in parsed]
    avg_change = sum(row["change"] for row in parsed) / universe if universe else 0.0
    median_abs_change = median(abs_changes) if abs_changes else 0.0
    top_abs_change = max(abs_changes) if abs_changes else 0.0
    active_ratio = sum(1 for value in abs_changes if value >= 2.0) / universe if universe else 0.0

    top_gainers = sorted(parsed, key=lambda x: x["change"], reverse=True)[:4]
    top_losers = sorted(parsed, key=lambda x: x["change"])[:4]
    top_volume = sorted(parsed, key=lambda x: x["quote_volume"], reverse=True)[:5]

    oi_rows = []
    for row in top_volume[:12]:
        try:
            oi_data = get_open_interest(row["symbol"]) or {}
            oi_value = _safe_float(oi_data.get("openInterest"))
        except Exception:
            oi_value = 0.0
        oi_rows.append({
            "symbol": row["symbol"],
            "open_interest": oi_value,
            "change": row["change"],
        })
    top_open_interest = sorted(oi_rows, key=lambda x: x["open_interest"], reverse=True)[:4]

    btc = _major_symbol_block("BTCUSDT", rows_by_symbol)
    eth = _major_symbol_block("ETHUSDT", rows_by_symbol)

    bias, preferred_side = _classify_bias(adv_ratio, avg_change, btc["change"], eth["change"])
    regime = _classify_regime(adv_ratio, median_abs_change, top_abs_change)
    volatility = _classify_volatility(median_abs_change, top_abs_change)
    participation = _classify_participation(active_ratio)
    environment, recommendation = _classify_environment(bias, regime, volatility, participation)

    return {
        "time": _now_utc(),
        "universe": universe,
        "advancers": advancers,
        "decliners": decliners,
        "flat": flat,
        "adv_ratio_pct": adv_ratio * 100.0,
        "avg_change": avg_change,
        "median_abs_change": median_abs_change,
        "top_abs_change": top_abs_change,
        "active_ratio_pct": active_ratio * 100.0,
        "bias": bias,
        "regime": regime,
        "volatility": volatility,
        "participation": participation,
        "environment": environment,
        "preferred_side": preferred_side,
        "recommendation": recommendation,
        "btc": btc,
        "eth": eth,
        "top_gainers": top_gainers,
        "top_losers": top_losers,
        "top_volume": top_volume,
        "top_open_interest": top_open_interest,
    }


def format_volume(value: float) -> str:
    return f"{_fmt_compact_number(value)} USDT"


def format_open_interest(value: float) -> str:
    return _fmt_compact_number(value)
