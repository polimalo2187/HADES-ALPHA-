from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.market import format_open_interest, format_volume, get_market_state_snapshot

CB_MARKET_REFRESH = "market_refresh"
CB_BACK_MENU = "back_menu"


def _fmt_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def build_market_state_text(snapshot: dict) -> str:
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

    lines.extend(["", "*Extremos 24h*", "• Gainers: " + ", ".join(f"{row['symbol']} ({_fmt_pct(row['change'])})" for row in snapshot['top_gainers'])])
    lines.append("• Losers: " + ", ".join(f"{row['symbol']} ({_fmt_pct(row['change'])})" for row in snapshot['top_losers']))

    lines.extend(["", "*Recomendación*", f"• {snapshot['recommendation']}"])
    return "\n".join(lines)


def build_market_state_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Actualizar", callback_data=CB_MARKET_REFRESH)],
        [InlineKeyboardButton("⬅️ Volver", callback_data=CB_BACK_MENU)],
    ])


def render_market_state():
    snapshot = get_market_state_snapshot()
    if not snapshot:
        return (
            "❌ No pude cargar el estado de mercado ahora mismo.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data=CB_BACK_MENU)]])
        )
    return build_market_state_text(snapshot), build_market_state_keyboard()
