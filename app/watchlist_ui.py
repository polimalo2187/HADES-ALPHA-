from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict
import requests

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.watchlist import format_watchlist, get_symbols
from app.i18n import normalize_language, tr

CB_WL_REFRESH = "wl_refresh"
CB_WL_CLEAR = "wl_clear"
CB_WL_REMOVE_PREFIX = "wl_rm:"
CB_BACK_MENU = "back_menu"

BINANCE_FUTURES_24H = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_PREMIUM_INDEX = "https://fapi.binance.com/fapi/v1/premiumIndex"
BINANCE_OPEN_INTEREST = "https://fapi.binance.com/fapi/v1/openInterest"
BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/klines"



def _t(lang: str | None, key: str, **kwargs) -> str:
    return tr(normalize_language(lang), f"watchlist.{key}", **kwargs)


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


def _trend_label(chg_4h: float | None, chg_1h: float | None, lang: str | None) -> str:
    c4 = chg_4h if chg_4h is not None else 0.0
    c1 = chg_1h if chg_1h is not None else 0.0

    if c4 > 1.0 and c1 >= 0:
        return _t(lang, "bullish")
    if c4 < -1.0 and c1 <= 0:
        return _t(lang, "bearish")
    return _t(lang, "mixed")


def _momentum_label(chg_1h: float | None, chg_24h: float, lang: str | None) -> str:
    c1 = chg_1h if chg_1h is not None else 0.0
    abs_mix = abs(c1) + abs(chg_24h) / 4.0

    if abs_mix >= 4.0:
        return _t(lang, "strong")
    if abs_mix >= 1.5:
        return _t(lang, "medium")
    return _t(lang, "weak")


def _fetch_symbol_panel(symbol: str, lang: str | None = None) -> dict:
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
        "trend": _trend_label(chg_4h, chg_1h, lang),
        "momentum": _momentum_label(chg_1h, chg_24h, lang),
    }


def fetch_watchlist_snapshot(symbols, lang: str | None = None):
    data: Dict[str, dict] = {}
    for s in symbols[:10]:
        panel = _fetch_symbol_panel(s, lang=lang)
        if panel:
            data[s] = panel
    return data


def watchlist_keyboard(symbols, lang: str | None = None):
    rows = [[
        InlineKeyboardButton(_t(lang, "refresh"), callback_data=CB_WL_REFRESH),
        InlineKeyboardButton(_t(lang, "clear"), callback_data=CB_WL_CLEAR),
    ]]

    for s in symbols[:6]:
        rows.append([InlineKeyboardButton(_t(lang, "remove", symbol=s), callback_data=f"{CB_WL_REMOVE_PREFIX}{s}")])

    rows.append([InlineKeyboardButton(_t(lang, "back"), callback_data=CB_BACK_MENU)])
    return InlineKeyboardMarkup(rows)


def _price_digits(v: float) -> int:
    try:
        number = abs(float(v))
    except Exception:
        return 4
    if number == 0:
        return 4
    if number >= 1000:
        return 2
    if number >= 100:
        return 3
    if number >= 1:
        return 4
    if number >= 0.1:
        return 5
    if number >= 0.01:
        return 7
    if number >= 0.001:
        return 8
    if number >= 0.0001:
        return 10
    return 12


def _fmt_price(v: float) -> str:
    digits = _price_digits(v)
    formatted = f"{float(v):,.{digits}f}"
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted


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


def _format_watchlist(symbols, lang: str | None = None) -> str:
    if not symbols:
        return (
            f"{_t(lang, 'empty_title')}\n\n"
            f"{_t(lang, 'empty_hint')}\n"
            f"{_t(lang, 'examples')}"
        )

    lines = [_t(lang, "title"), ""]
    for i, s in enumerate(symbols, 1):
        lines.append(f"{i}) {s}")
    lines.append("")
    lines.append(_t(lang, "tip"))
    return "\n".join(lines)


def render_watchlist_view(symbols, lang: str | None = None):
    base = _format_watchlist(symbols, lang=lang)
    snapshot = fetch_watchlist_snapshot(symbols, lang=lang)

    lines = [base]

    if snapshot:
        lines.append(f"\n{_t(lang, 'pro_title')}")
        for s in symbols[:10]:
            if s not in snapshot:
                continue
            d = snapshot[s]
            lines.append(
                f"\n{s}\n"
                f"{_t(lang, 'price')}: {_fmt_price(d['price'])}\n"
                f"24h: {d['chg_24h']:+.2f}% | 1h: {(d['chg_1h'] if d['chg_1h'] is not None else 0):+.2f}% | 4h: {(d['chg_4h'] if d['chg_4h'] is not None else 0):+.2f}%\n"
                f"{_t(lang, 'volume_24h')}: {_fmt_vol(d['volume'])}\n"
                f"{_t(lang, 'funding')}: {d['funding']:+.4f}%\n"
                f"{_t(lang, 'open_interest')}: {_fmt_oi(d['open_interest'])}\n"
                f"{_t(lang, 'trend')}: {d['trend']} | {_t(lang, 'momentum')}: {d['momentum']}"
            )
    elif symbols:
        lines.append(f"\n{_t(lang, 'load_error')}")

    lines.append(f"\n{_t(lang, 'updated_at')}: {_now()}")

    kb = watchlist_keyboard(symbols, lang=lang)
    return "\n".join(lines), kb
