
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.market import format_open_interest, format_volume, get_market_state_snapshot
from app.plans import PLAN_FREE, PLAN_PREMIUM

CB_MARKET_REFRESH = "market_refresh"
CB_BACK_MENU = "back_menu"
CB_PLANS = "plans"


def _fmt_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def _norm_lang(language: str | None) -> str:
    return "en" if str(language or "es").lower().startswith("en") else "es"


ES_EN = {
    "Alcista": "Bullish",
    "Bajista": "Bearish",
    "Alcista leve": "Mild bullish",
    "Bajista leve": "Mild bearish",
    "Neutral": "Neutral",
    "LONGS": "LONGS",
    "SHORTS": "SHORTS",
    "LONGS selectivos": "Selective LONGS",
    "SHORTS selectivos": "Selective SHORTS",
    "Selectivo / mixto": "Selective / mixed",
    "Compresión": "Compression",
    "Lateral": "Sideways",
    "Expansión": "Expansion",
    "Tendencial": "Trending",
    "Mixto": "Mixed",
    "Alta": "High",
    "Media": "Medium",
    "Baja": "Low",
    "Amplia": "Broad",
    "Reducida": "Reduced",
    "Favorable": "Favorable",
    "Selectivo": "Selective",
    "Peligroso": "Dangerous",
}

RECOMMENDATION_EN = {
    "Favorece seguir la dirección dominante en retrocesos limpios.": "Favors following the dominant direction on clean pullbacks.",
    "Favorece buscar shorts en rebotes y rupturas confirmadas.": "Favors looking for shorts on rebounds and confirmed breakdowns.",
    "Reduce exposición y exige confirmación extra antes de entrar.": "Reduce exposure and demand extra confirmation before entering.",
    "Hay movimiento fuerte pero sin lectura clara; evita entradas agresivas.": "There is strong movement but no clear read; avoid aggressive entries.",
    "Opera selectivo: prioriza estructura limpia y evita perseguir precio.": "Trade selectively: prioritize clean structure and avoid chasing price.",
    "El régimen favorece continuidad": "The regime favors continuation",
    "La lectura exige más selectividad": "The read requires more selectivity",
    "Hay riesgo de ruido o manipulación; baja agresividad": "There is risk of noise or manipulation; reduce aggressiveness",
}


def _tx(value: str, language: str | None) -> str:
    if _norm_lang(language) == "es":
        return value
    return ES_EN.get(value, RECOMMENDATION_EN.get(value, value))


def _build_market_state_text_free(snapshot: dict, language: str | None = None) -> str:
    en = _norm_lang(language) == "en"
    lines = [
        "🌐 MARKET STATE" if en else "🌐 ESTADO DE MERCADO",
        "",
        "*FREE Summary*" if en else "*Resumen FREE*",
        f"🕒 {'Updated' if en else 'Actualizado'}: {snapshot['time']}",
        "",
        "*Main read*" if en else "*Lectura principal*",
        f"• {'Bias' if en else 'Sesgo'}: *{_tx(snapshot['bias'], language)}*",
        f"• {'Regime' if en else 'Régimen'}: *{_tx(snapshot['regime'], language)}*",
        f"• {'Volatility' if en else 'Volatilidad'}: *{_tx(snapshot['volatility'], language)}*",
        f"• {'Environment' if en else 'Entorno'}: *{_tx(snapshot['environment'], language)}*",
        f"• {'Favored side' if en else 'Favorece'}: *{_tx(snapshot['preferred_side'], language)}*",
        "",
        "*General pulse*" if en else "*Pulso general*",
        (
            f"• {'Universe' if en else 'Universo'}: {snapshot['universe']} {'pairs' if en else 'pares'} | "
            f"{'Up' if en else 'Suben'}: {snapshot['advancers']} | {'Down' if en else 'Caen'}: {snapshot['decliners']}"
        ),
        f"• {'Bullish breadth' if en else 'Breadth alcista'}: {_fmt_pct(snapshot['adv_ratio_pct'])}",
        f"• {'Average 24h change' if en else 'Cambio medio 24h'}: {_fmt_pct(snapshot['avg_change'])}",
        f"• {'Broad activity' if en else 'Actividad amplia'}: {snapshot['active_ratio_pct']:.2f}% {'of pairs with |Δ| >= 2%' if en else 'de pares con |Δ| >= 2%'}",
        "",
        "*Majors*",
        f"• BTCUSDT: {_fmt_pct(snapshot['btc']['change'])}",
        f"• ETHUSDT: {_fmt_pct(snapshot['eth']['change'])}",
        "",
        "*Recommendation*" if en else "*Recomendación*",
        f"• {_tx(snapshot['recommendation'], language)}",
        "",
        "🔒 *PLUS/PREMIUM*: unlock funding, open interest, extremes and the full market read." if en else "🔒 *PLUS/PREMIUM*: desbloquea funding, open interest, extremos y lectura completa del mercado.",
    ]
    return "\n".join(lines)


def _build_market_state_text_plus(snapshot: dict, premium: bool = False, language: str | None = None) -> str:
    en = _norm_lang(language) == "en"
    lines = [
        "🌐 MARKET STATE" if en else "🌐 ESTADO DE MERCADO",
        "",
        "*Binance USDT-M Futures*",
        f"🕒 {'Updated' if en else 'Actualizado'}: {snapshot['time']}",
        "",
        "*Main read*" if en else "*Lectura principal*",
        f"• {'Bias' if en else 'Sesgo'}: *{_tx(snapshot['bias'], language)}*",
        f"• {'Regime' if en else 'Régimen'}: *{_tx(snapshot['regime'], language)}*",
        f"• {'Volatility' if en else 'Volatilidad'}: *{_tx(snapshot['volatility'], language)}*",
        f"• {'Participation' if en else 'Participación'}: *{_tx(snapshot['participation'], language)}*",
        f"• {'Environment' if en else 'Entorno'}: *{_tx(snapshot['environment'], language)}*",
        f"• {'Favored side' if en else 'Favorece'}: *{_tx(snapshot['preferred_side'], language)}*",
        "",
        "*Breadth & activity*" if en else "*Breadth y actividad*",
        (
            f"• {'Universe' if en else 'Universo'}: {snapshot['universe']} {'pairs' if en else 'pares'} | "
            f"{'Up' if en else 'Suben'}: {snapshot['advancers']} | "
            f"{'Down' if en else 'Caen'}: {snapshot['decliners']} | "
            f"{'Flat' if en else 'Planos'}: {snapshot['flat']}"
        ),
        f"• {'Bullish breadth' if en else 'Breadth alcista'}: {_fmt_pct(snapshot['adv_ratio_pct'])}",
        f"• {'Average 24h change' if en else 'Cambio medio 24h'}: {_fmt_pct(snapshot['avg_change'])}",
        f"• {'Median volatility' if en else 'Volatilidad mediana'}: {snapshot['median_abs_change']:.2f}%",
        f"• {'Extreme move' if en else 'Movimiento extremo'}: {snapshot['top_abs_change']:.2f}%",
        f"• {'Broad activity' if en else 'Actividad amplia'}: {snapshot['active_ratio_pct']:.2f}% {'of pairs with |Δ| >= 2%' if en else 'de pares con |Δ| >= 2%'}",
        "",
        "*Majors*",
        (
            f"• BTCUSDT: {_fmt_pct(snapshot['btc']['change'])} | "
            f"{'Funding' if en else 'Funding'}: {_fmt_pct(snapshot['btc']['funding_rate_pct'])} | "
            f"OI: {format_open_interest(snapshot['btc']['open_interest'])}"
        ),
        (
            f"• ETHUSDT: {_fmt_pct(snapshot['eth']['change'])} | "
            f"{'Funding' if en else 'Funding'}: {_fmt_pct(snapshot['eth']['funding_rate_pct'])} | "
            f"OI: {format_open_interest(snapshot['eth']['open_interest'])}"
        ),
        "",
        "*Top volume*" if en else "*Mayor volumen*",
    ]

    for row in snapshot['top_volume']:
        lines.append(f"• *{row['symbol']}* — {format_volume(row['quote_volume'])} | {_fmt_pct(row['change'])}")

    lines.extend(["", "*Top Open Interest*" if en else "*Mayor Open Interest*"])
    for row in snapshot['top_open_interest']:
        lines.append(f"• *{row['symbol']}* — {format_open_interest(row['open_interest'])} | {_fmt_pct(row['change'])}")

    lines.extend([
        "",
        "*24h extremes*" if en else "*Extremos 24h*",
        ("• Gainers: " if en else "• Gainers: ") + ", ".join(f"{row['symbol']} ({_fmt_pct(row['change'])})" for row in snapshot['top_gainers']),
        ("• Losers: " if en else "• Losers: ") + ", ".join(f"{row['symbol']} ({_fmt_pct(row['change'])})" for row in snapshot['top_losers']),
        "",
        "*Recommendation*" if en else "*Recomendación*",
        f"• {_tx(snapshot['recommendation'], language)}",
    ])

    if premium:
        premium_note = (
            "The regime favors continuation" if _tx(snapshot['environment'], language) == ('Favorable' if en else 'Favorable') and _tx(snapshot['regime'], language) in ({'Trending', 'Expansion'} if en else {'Tendencial', 'Expansión'})
            else "The read requires more selectivity" if _tx(snapshot['environment'], language) in ({'Mixed', 'Selective'} if en else {'Mixto', 'Selectivo'})
            else "There is risk of noise or manipulation; reduce aggressiveness" if en else "Hay riesgo de ruido o manipulación; baja agresividad"
        )
        lines.extend([
            "",
            "*Premium read*" if en else "*Lectura Premium*",
            f"• BTC funding + OI: {_fmt_pct(snapshot['btc']['funding_rate_pct'])} / {format_open_interest(snapshot['btc']['open_interest'])}",
            f"• ETH funding + OI: {_fmt_pct(snapshot['eth']['funding_rate_pct'])} / {format_open_interest(snapshot['eth']['open_interest'])}",
            f"• {'Premium note' if en else 'Nota premium'}: {premium_note}",
        ])

    return "\n".join(lines)


def build_market_state_text(snapshot: dict, plan: str | None = None, language: str | None = None) -> str:
    if plan == PLAN_FREE:
        return _build_market_state_text_free(snapshot, language=language)
    if plan == PLAN_PREMIUM:
        return _build_market_state_text_plus(snapshot, premium=True, language=language)
    return _build_market_state_text_plus(snapshot, premium=False, language=language)


def build_market_state_keyboard(plan: str | None = None, language: str | None = None) -> InlineKeyboardMarkup:
    en = _norm_lang(language) == "en"
    rows = [[InlineKeyboardButton("🔄 Update" if en else "🔄 Actualizar", callback_data=CB_MARKET_REFRESH)]]
    if plan == PLAN_FREE:
        rows.append([InlineKeyboardButton("💼 View plans" if en else "💼 Ver planes", callback_data=CB_PLANS)])
    rows.append([InlineKeyboardButton("⬅️ Back" if en else "⬅️ Volver", callback_data=CB_BACK_MENU)])
    return InlineKeyboardMarkup(rows)


def render_market_state(plan: str | None = None, language: str | None = None):
    en = _norm_lang(language) == "en"
    snapshot = get_market_state_snapshot()
    if not snapshot:
        rows = []
        if plan == PLAN_FREE:
            rows.append([InlineKeyboardButton("💼 View plans" if en else "💼 Ver planes", callback_data=CB_PLANS)])
        rows.append([InlineKeyboardButton("⬅️ Back" if en else "⬅️ Volver", callback_data=CB_BACK_MENU)])
        return (
            "❌ I could not load market state right now." if en else "❌ No pude cargar el estado de mercado ahora mismo.",
            InlineKeyboardMarkup(rows),
        )
    return build_market_state_text(snapshot, plan=plan, language=language), build_market_state_keyboard(plan=plan, language=language)
