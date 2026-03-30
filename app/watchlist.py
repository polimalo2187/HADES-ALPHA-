from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Optional, Set
import requests

from app.database import watchlists_collection
from app.models import WATCHLIST_SCHEMA_VERSION, new_watchlist

BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

collection = watchlists_collection()


def _now():
    return datetime.now(timezone.utc)


def _safe_json_get(url: str, timeout: int = 10):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def get_valid_symbols() -> Set[str]:
    data = _safe_json_get(BINANCE_INFO_URL, timeout=10)
    if not data or "symbols" not in data:
        return set()

    symbols = set()
    for item in data.get("symbols", []):
        try:
            if item.get("contractType") == "PERPETUAL" and item.get("status") == "TRADING":
                sym = str(item.get("symbol", "")).upper().strip()
                if sym.endswith("USDT"):
                    symbols.add(sym)
        except Exception:
            continue
    return symbols


def normalize_symbol(symbol: str) -> Optional[str]:
    if not symbol:
        return None

    s = str(symbol).upper().strip()
    s = s.replace(" ", "").replace("/", "").replace("-", "")
    s = "".join(ch for ch in s if ch.isalnum())

    if not s:
        return None

    if s.endswith("USDT"):
        base = s[:-4]
        if 2 <= len(base) <= 20:
            return f"{base}USDT"
        return None

    if 2 <= len(s) <= 20:
        return f"{s}USDT"

    return None


def normalize_many(raw: str) -> List[str]:
    if not raw:
        return []

    raw = raw.replace("\n", ",").replace(";", ",")
    tokens = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        for sub in part.split():
            sub = sub.strip()
            if sub:
                tokens.append(sub)

    result: List[str] = []
    seen = set()
    for token in tokens:
        sym = normalize_symbol(token)
        if sym and sym not in seen:
            result.append(sym)
            seen.add(sym)
    return result


def get_symbols(user_id: int) -> List[str]:
    doc = collection.find_one({"user_id": int(user_id)}, {"symbols": 1})
    if not doc:
        return []
    return list(doc.get("symbols", []))


def get_watchlist(user_id: int) -> List[str]:
    return get_symbols(user_id)


def _ensure_doc(user_id: int):
    collection.update_one(
        {"user_id": int(user_id)},
        {
            "$setOnInsert": new_watchlist(int(user_id), []),
            "$set": {"updated_at": _now(), "schema_version": WATCHLIST_SCHEMA_VERSION},
        },
        upsert=True,
    )


def get_watchlist_limit_for_plan(plan: str) -> Optional[int]:
    p = (plan or "FREE").upper().strip()
    if p == "PREMIUM":
        return None
    if p == "PLUS":
        return 10
    return 2


def add_symbol(user_id: int, symbol: str, plan: str = "FREE"):
    sym = normalize_symbol(symbol)
    if not sym:
        return False, "❌ Símbolo inválido. Ejemplos válidos: BTCUSDT, ETHUSDT, SOLUSDT"

    valid = get_valid_symbols()
    if valid and sym not in valid:
        return False, f"❌ {sym} no es un contrato válido de Binance Futures USDT-M."

    current = get_symbols(int(user_id))
    if sym in current:
        return True, f"✅ {sym} ya está en tu Watchlist."

    limit = get_watchlist_limit_for_plan(plan)
    if limit is not None and len(current) >= limit:
        return False, f"🔒 Tu plan permite hasta {limit} símbolos en Watchlist."

    _ensure_doc(int(user_id))
    collection.update_one(
        {"user_id": int(user_id)},
        {
            "$addToSet": {"symbols": sym},
            "$set": {"updated_at": _now(), "schema_version": WATCHLIST_SCHEMA_VERSION},
        },
        upsert=True,
    )
    return True, f"✅ {sym} añadido a tu Watchlist."


def set_symbols(user_id: int, symbols: Iterable[str], plan: str = "FREE"):
    normalized = []
    seen = set()
    valid = get_valid_symbols()

    for symbol in symbols:
        sym = normalize_symbol(symbol)
        if not sym:
            continue
        if valid and sym not in valid:
            continue
        if sym not in seen:
            normalized.append(sym)
            seen.add(sym)

    limit = get_watchlist_limit_for_plan(plan)
    if limit is not None and len(normalized) > limit:
        return False, f"🔒 Tu plan permite hasta {limit} símbolos en Watchlist."

    collection.update_one(
        {"user_id": int(user_id)},
        {
            "$set": {
                "symbols": normalized,
                "updated_at": _now(),
                "schema_version": WATCHLIST_SCHEMA_VERSION,
            },
            "$setOnInsert": new_watchlist(int(user_id), normalized),
        },
        upsert=True,
    )
    return True, "✅ Watchlist actualizada."


def remove_symbol(user_id: int, symbol: str):
    sym = normalize_symbol(symbol)
    if not sym:
        return False, "❌ Símbolo inválido."

    _ensure_doc(int(user_id))
    collection.update_one(
        {"user_id": int(user_id)},
        {
            "$pull": {"symbols": sym},
            "$set": {"updated_at": _now(), "schema_version": WATCHLIST_SCHEMA_VERSION},
        },
        upsert=True,
    )
    return True, f"✅ {sym} eliminado de tu Watchlist."


def clear(user_id: int):
    _ensure_doc(int(user_id))
    collection.update_one(
        {"user_id": int(user_id)},
        {
            "$set": {
                "symbols": [],
                "updated_at": _now(),
                "schema_version": WATCHLIST_SCHEMA_VERSION,
            }
        },
        upsert=True,
    )
    return True, "✅ Watchlist limpiada."


def clear_watchlist(user_id: int):
    return clear(user_id)


def format_watchlist(symbols: List[str]) -> str:
    if not symbols:
        return (
            "⭐ Watchlist vacía.\n\n"
            "Escribe un símbolo para añadir.\n"
            "Ejemplos válidos: BTCUSDT, ETHUSDT, SOLUSDT"
        )

    lines = ["⭐ WATCHLIST\n"]
    for i, s in enumerate(symbols, 1):
        lines.append(f"{i}) {s}")
    lines.append("\nTip: puedes escribir varios separados por coma. Ej: BTC, ETH, SOL")
    return "\n".join(lines)
