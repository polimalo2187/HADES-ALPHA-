from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict
import requests

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.watchlist import format_watchlist

CB_WL_REFRESH = "wl_refresh"
CB_WL_CLEAR = "wl_clear"
CB_WL_REMOVE_PREFIX = "wl_rm:"
CB_BACK_MENU = "back_menu"

BINANCE_FUTURES_24H = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_PREMIUM_INDEX = "https://fapi.binance.com/fapi/v1/premiumIndex"
BINANCE_OPEN_INTEREST = "https://fapi.binance.com/fapi/v1/openInterest"
BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/klines"


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _fetch_24h(symbol: str) -> dict:
    try:
        r = requests.get(BINANCE_FUTURES_24H, params={"symbol": symbol}, timeout=8)
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception:
        return {}


def _fetch_premium_index(symbol: str) -> dict:
    try:
        r = requests.get(BINANCE_PREMIUM_INDEX, params={"symbol": symbol}, timeout=8)
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception:
        return {}


def _fetch_open_interest(symbol: str) -> dict:
    try:
        r = requests.get(BINANCE_OPEN_INTEREST, params={"symbol": symbol}, timeout=8)
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception:
        return {}


def _fetch_change(symbol: str, interval: str) -> float | None:
    try:
        r = requests.get(
            BINANCE_KLINES,
            params={"symbol": symbol, "interval": interval, "limit": 2},
            timeout=8,
        )
        if r.status_code != 200:
            return None

        data = r.json()
        if not isinstance(data, list) or len(data) < 2:
            return None

        prev_close = _safe_float(data[-2][4])
        last_close = _safe_float(data[-1][4])

        if prev_close <= 0:
            return None

        return ((last_close - prev_close) / prev_close) * 100.0
    except Exception:
        return None


def _trend_label(chg_4h: float | None, chg_1h: float | None) -> str:
    c4 = chg_4h if chg_4h is not None else 0.0
    c1 = chg_1h if chg_1h is not None else 0.0

    if c4 > 1.0 and c1 >= 0:
        return "Alcista"
    if c4 < -1.0 and c1 <= 0:
        return "Bajista"
    return "Mixta"


def _momentum_label(chg_1h: float | None, chg_24h: float) -> str:
    c1 = chg_1h if chg_1h is not None else 0.0
    abs_mix = abs(c1) + abs(chg_24h) / 4.0

    if abs_mix >= 4.0:
        return "Fuerte"
    if abs_mix >= 1.5:
        return "Medio"
    return "Débil"


def _fetch_symbol_panel(symbol: str) -> dict:
    ticker = _fetch_24h(symbol)
    if not ticker:
        return {}

    premium = _fetch_premium_index(symbol)
    oi = _fetch_open_interest(symbol)
    chg_1h = _fetch_change(symbol, "1h")
    chg_4h = _fetch_change(symbol, "4h")

    price = _safe_float(ticker.get("lastPrice"))
    chg_24h = _safe_float(ticker.get("priceChangePercent"))
    volume = _safe_float(ticker.get("quoteVolume"))
    funding = _safe_float(premium.get("lastFundingRate")) * 100.0
    open_interest = _safe_float(oi.get("openInterest"))

    return {
        "price": price,
        "chg_24h": chg_24h,
        "chg_1h": chg_1h,
        "chg_4h": chg_4h,
        "volume": volume,
        "funding": funding,
        "open_interest": open_interest,
        "trend": _trend_label(chg_4h, chg_1h),
        "momentum": _momentum_label(chg_1h, chg_24h),
    }


def fetch_watchlist_snapshot(symbols):
    data: Dict[str, dict] = {}
    for s in symbols[:10]:
        panel = _fetch_symbol_panel(s)
        if panel:
            data[s] = panel
    return data


def watchlist_keyboard(symbols):
    rows = [
        [
            InlineKeyboardButton("🔄 Actualizar", callback_data=CB_WL_REFRESH),
            InlineKeyboardButton("🧹 Limpiar", callback_data=CB_WL_CLEAR),
        ]
    ]

    for s in symbols[:6]:
        rows.append(
            [InlineKeyboardButton(f"❌ Quitar {s}", callback_data=f"{CB_WL_REMOVE_PREFIX}{s}")]
        )

    rows.append([InlineKeyboardButton("⬅️ Volver", callback_data=CB_BACK_MENU)])
    return InlineKeyboardMarkup(rows)


def _fmt_price(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def _fmt_vol(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    return f"{v:,.0f}"


def _fmt_oi(v: float) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:.2f}K"
    return f"{v:,.0f}"


def render_watchlist_view(symbols):
    base = format_watchlist(symbols)
    snapshot = fetch_watchlist_snapshot(symbols)

    lines = [base]

    if snapshot:
        lines.append("\n📊 Watchlist PRO (Futures):")
        for s in symbols[:10]:
            if s not in snapshot:
                continue

            d = snapshot[s]
            lines.append(
                f"\n{s}\n"
                f"Precio: {_fmt_price(d['price'])}\n"
                f"24h: {d['chg_24h']:+.2f}% | 1h: {(d['chg_1h'] if d['chg_1h'] is not None else 0):+.2f}% | 4h: {(d['chg_4h'] if d['chg_4h'] is not None else 0):+.2f}%\n"
                f"Volumen 24h: {_fmt_vol(d['volume'])}\n"
                f"Funding: {d['funding']:+.4f}%\n"
                f"Open Interest: {_fmt_oi(d['open_interest'])}\n"
                f"Tendencia: {d['trend']} | Momentum: {d['momentum']}"
            )
    elif symbols:
        lines.append("\nℹ️ No pude cargar datos de mercado ahora mismo.")

    lines.append(f"\n🕒 Actualizado: {_now()}")

    kb = watchlist_keyboard(symbols)
    return "\n".join(lines), kb
