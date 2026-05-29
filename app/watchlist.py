from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Optional, Set, Tuple
from app.market_data_public import get_public_24h_ticker_for_symbol, get_valid_public_symbols

from app.database import watchlists_collection
from app.models import WATCHLIST_SCHEMA_VERSION, new_watchlist

PUBLIC_MARKET_INFO_SOURCE = "public-provider://symbols"


collection = watchlists_collection()


def _now():
    return datetime.now(timezone.utc)




def get_valid_symbols() -> Set[str]:
    try:
        return get_valid_public_symbols()
    except Exception:
        return set()


def _symbol_support_status(sym: str) -> Tuple[bool, str]:
    """Validate a watchlist symbol against the complete public provider chain.

    Important: watchlist validation must not be tied to the scanner Top-N cache.
    A user may add a Binance-only/OKX-only/Bybit-only symbol that is not part of
    the highest-volume scanner universe. We first use the cheap merged symbol set,
    then do one cached direct lookup for the explicit symbol before rejecting it.
    """
    symbol = normalize_symbol(sym)
    if not symbol:
        return False, "invalid_format"

    valid = get_valid_symbols()
    if symbol in valid:
        return True, "bulk_provider_universe"

    # If the provider universe could not be built or only contains the small local
    # fallback, do not hard-reject. Let the card show temporary coverage status.
    if not valid or len(valid) <= 50:
        return True, "provider_universe_unreliable"

    # Direct rescue path: asks the configured providers for this specific symbol,
    # with per-symbol caching and circuit-breaker protection in market_data_public.
    try:
        ticker = get_public_24h_ticker_for_symbol(symbol, allow_direct_fetch=True)
        if ticker and str(ticker.get("symbol") or "").upper() == symbol:
            return True, str(ticker.get("provider") or "direct_provider_lookup")
    except Exception:
        pass

    return False, "not_supported_by_public_providers"


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


def _watchlist_insert_seed(user_id: int, symbols: Optional[List[str]] = None) -> dict:
    doc = new_watchlist(int(user_id), list(symbols or []))
    doc.pop("updated_at", None)
    doc.pop("schema_version", None)
    doc.pop("symbols", None)
    return doc


def _ensure_doc(user_id: int):
    collection.update_one(
        {"user_id": int(user_id)},
        {
            "$setOnInsert": {**_watchlist_insert_seed(int(user_id)), "symbols": []},
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

    supported, support_source = _symbol_support_status(sym)
    if not supported:
        return False, (
            f"❌ {sym} no está disponible como contrato USDT perpetuo en los proveedores públicos activos "
            "(Bybit, OKX o Binance). Verifica el símbolo e intenta de nuevo."
        )

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
    rejected: List[str] = []

    for symbol in symbols:
        sym = normalize_symbol(symbol)
        if not sym:
            continue
        supported, _support_source = _symbol_support_status(sym)
        if not supported:
            rejected.append(sym)
            continue
        if sym not in seen:
            normalized.append(sym)
            seen.add(sym)

    if rejected and not normalized:
        return False, "❌ Ninguno de esos pares está disponible como contrato USDT perpetuo en los proveedores públicos activos."

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
            "$setOnInsert": _watchlist_insert_seed(int(user_id), normalized),
        },
        upsert=True,
    )
    if rejected:
        preview = ", ".join(rejected[:5])
        more = f" y {len(rejected) - 5} más" if len(rejected) > 5 else ""
        return True, f"✅ Watchlist actualizada. No añadí {preview}{more} porque no tienen cobertura USDT perpetua pública."
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
