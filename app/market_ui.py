
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from app.market import get_market_snapshot

CB_MARKET_REFRESH = "market_refresh"
CB_BACK_MENU = "back_menu"

def render_market():

    snap = get_market_snapshot()

    if not snap:
        return (
            "❌ No pude cargar datos del mercado ahora mismo.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data=CB_BACK_MENU)]])
        )

    text = "📊 MERCADO FUTURES\n\n"

    if snap["btc"]:
        text += f"BTCUSDT: {snap['btc']['change']:+.2f}%\n"

    if snap["eth"]:
        text += f"ETHUSDT: {snap['eth']['change']:+.2f}%\n"

    text += "\n🔥 Top Gainers:\n"

    for g in snap["gainers"]:
        text += f"{g['symbol']} {g['change']:+.2f}%\n"

    text += "\n📉 Top Losers:\n"

    for l in snap["losers"]:
        text += f"{l['symbol']} {l['change']:+.2f}%\n"

    text += f"\n🕒 Actualizado: {snap['time']}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Actualizar", callback_data=CB_MARKET_REFRESH)],
        [InlineKeyboardButton("⬅️ Volver", callback_data=CB_BACK_MENU)]
    ])

    return text, keyboard
