from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.market import format_open_interest, format_volume, get_market_state_snapshot
from app.plans import PLAN_FREE, PLAN_PREMIUM

CB_MARKET_REFRESH = "market_refresh"
CB_BACK_MENU = "back_menu"
CB_PLANS = "plans"


def _fmt_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def _build_market_state_text_free(snapshot: dict) -> str:
    lines = [
        "🌐 ESTADO DE MERCADO",
        "",
        "*Resumen FREE*",
        f"🕒 Actualizado: {snapshot['time']}",
        "",
        "*Lectura principal*",
        f"• Sesgo: *{snapshot['bias']}*",
        f"• Régimen: *{snapshot['regime']}*",
        f"• Volatilidad: *{snapshot['volatility']}*",
        f"• Entorno: *{snapshot['environment']}*",
        f"• Favorece: *{snapshot['preferred_side']}*",
        "",
        "*Pulso general*",
        (
            f"• Universo: {snapshot['universe']} pares | "
            f"Suben: {snapshot['advancers']} | Caen: {snapshot['decliners']}"
        ),
        f"• Breadth alcista: {_fmt_pct(snapshot['adv_ratio_pct'])}",
        f"• Cambio medio 24h: {_fmt_pct(snapshot['avg_change'])}",
        f"• Actividad amplia: {snapshot['active_ratio_pct']:.2f}% de pares con |Δ| >= 2%",
        "",
        "*Majors*",
        f"• BTCUSDT: {_fmt_pct(snapshot['btc']['change'])}",
        f"• ETHUSDT: {_fmt_pct(snapshot['eth']['change'])}",
        "",
        "*Recomendación*",
        f"• {snapshot['recommendation']}",
        "",
        "🔒 *PLUS/PREMIUM*: desbloquea funding, open interest, extremos y lectura completa del mercado.",
    ]
    return "\n".join(lines)


def _build_market_state_text_plus(snapshot: dict, premium: bool = False) -> str:
    lines = [
        "🌐 ESTADO DE MERCADO",
        "",
        "*Binance USDT-M Futures*",
        f"🕒 Actualizado: {snapshot['time']}",
        "",
        "*Lectura principal*",
        f"• Sesgo: *{snapshot['bias']}*",
        f"• Régimen: *{snapshot['regime']}*",
        f"• Volatilidad: *{snapshot['volatility']}*",
        f"• Participación: *{snapshot['participation']}*",
        f"• Entorno: *{snapshot['environment']}*",
        f"• Favorece: *{snapshot['preferred_side']}*",
        "",
        "*Breadth y actividad*",
        (
            f"• Universo: {snapshot['universe']} pares | "
            f"Suben: {snapshot['advancers']} | Caen: {snapshot['decliners']} | Planos: {snapshot['flat']}"
        ),
        f"• Breadth alcista: {_fmt_pct(snapshot['adv_ratio_pct'])}",
        f"• Cambio medio 24h: {_fmt_pct(snapshot['avg_change'])}",
        f"• Volatilidad mediana: {snapshot['median_abs_change']:.2f}%",
        f"• Movimiento extremo: {snapshot['top_abs_change']:.2f}%",
        f"• Actividad amplia: {snapshot['active_ratio_pct']:.2f}% de pares con |Δ| >= 2%",
        "",
        "*Majors*",
        (
            f"• BTCUSDT: {_fmt_pct(snapshot['btc']['change'])} | "
            f"Funding: {_fmt_pct(snapshot['btc']['funding_rate_pct'])} | "
            f"OI: {format_open_interest(snapshot['btc']['open_interest'])}"
        ),
        (
            f"• ETHUSDT: {_fmt_pct(snapshot['eth']['change'])} | "
            f"Funding: {_fmt_pct(snapshot['eth']['funding_rate_pct'])} | "
            f"OI: {format_open_interest(snapshot['eth']['open_interest'])}"
        ),
        "",
        "*Mayor volumen*",
    ]

    for row in snapshot['top_volume']:
        lines.append(f"• *{row['symbol']}* — {format_volume(row['quote_volume'])} | {_fmt_pct(row['change'])}")

    lines.extend(["", "*Mayor Open Interest*"])
    for row in snapshot['top_open_interest']:
        lines.append(f"• *{row['symbol']}* — {format_open_interest(row['open_interest'])} | {_fmt_pct(row['change'])}")

    lines.extend([
        "",
        "*Extremos 24h*",
        "• Gainers: " + ", ".join(f"{row['symbol']} ({_fmt_pct(row['change'])})" for row in snapshot['top_gainers']),
        "• Losers: " + ", ".join(f"{row['symbol']} ({_fmt_pct(row['change'])})" for row in snapshot['top_losers']),
        "",
        "*Recomendación*",
        f"• {snapshot['recommendation']}",
    ])

    if premium:
        premium_note = (
            "El régimen favorece continuidad" if snapshot['environment'] == 'Favorable' and snapshot['regime'] in {'Tendencial', 'Expansión'}
            else "La lectura exige más selectividad" if snapshot['environment'] in {'Mixto', 'Selectivo'}
            else "Hay riesgo de ruido o manipulación; baja agresividad"
        )
        lines.extend([
            "",
            "*Lectura Premium*",
            f"• BTC funding + OI: {_fmt_pct(snapshot['btc']['funding_rate_pct'])} / {format_open_interest(snapshot['btc']['open_interest'])}",
            f"• ETH funding + OI: {_fmt_pct(snapshot['eth']['funding_rate_pct'])} / {format_open_interest(snapshot['eth']['open_interest'])}",
            f"• Nota premium: {premium_note}",
        ])

    return "\n".join(lines)


def build_market_state_text(snapshot: dict, plan: str | None = None) -> str:
    if plan == PLAN_FREE:
        return _build_market_state_text_free(snapshot)
    if plan == PLAN_PREMIUM:
        return _build_market_state_text_plus(snapshot, premium=True)
    return _build_market_state_text_plus(snapshot, premium=False)


def build_market_state_keyboard(plan: str | None = None) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("🔄 Actualizar", callback_data=CB_MARKET_REFRESH)]]
    if plan == PLAN_FREE:
        rows.append([InlineKeyboardButton("💼 Ver planes", callback_data=CB_PLANS)])
    rows.append([InlineKeyboardButton("⬅️ Volver", callback_data=CB_BACK_MENU)])
    return InlineKeyboardMarkup(rows)


def render_market_state(plan: str | None = None):
    snapshot = get_market_state_snapshot()
    if not snapshot:
        rows = []
        if plan == PLAN_FREE:
            rows.append([InlineKeyboardButton("💼 Ver planes", callback_data=CB_PLANS)])
        rows.append([InlineKeyboardButton("⬅️ Volver", callback_data=CB_BACK_MENU)])
        return (
            "❌ No pude cargar el estado de mercado ahora mismo.",
            InlineKeyboardMarkup(rows),
        )
    return build_market_state_text(snapshot, plan=plan), build_market_state_keyboard(plan=plan)
