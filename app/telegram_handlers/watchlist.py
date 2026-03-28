from __future__ import annotations

import asyncio
from telegram import Update
from telegram.ext import ContextTypes
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.database import users_collection
from app.menus import main_menu
from app.telegram_handlers.common import _get_user_language, _tr

logger = logging.getLogger(__name__)

async def handle_watchlist_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Mientras el usuario está en ⭐ Watchlist, cualquier texto se interpreta como símbolos
    para añadir (ej: BTC, ETHUSDT, SOL/USDT, BTC,ETH,SOL).
    """
    try:
        msg = update.effective_message
        user = update.effective_user
        if not msg or not user:
            return

        raw = (msg.text or "").strip()
        if not raw:
            return

        lang = _get_user_language(users_collection().find_one({"user_id": int(user.id)}) or {})

        from app.watchlist import normalize_many, add_symbol, get_symbols
        from app.watchlist_ui import render_watchlist_view

        symbols = normalize_many(raw)
        if not symbols:
            await msg.reply_text(_tr(lang, "❌ Símbolo inválido. Ej: BTCUSDT", "❌ Invalid symbol. Example: BTCUSDT"))
            return

        try:
            udoc = users_collection().find_one({"user_id": int(user.id)}) or {}
        except Exception:
            udoc = {}
        plan = (udoc.get("plan") or "FREE").upper()

        last_res = None
        for s in symbols:
            last_res = add_symbol(int(user.id), s, plan=plan)

        current = get_symbols(int(user.id))
        text, kb = render_watchlist_view(current, lang=lang)

        prefix = ""
        if last_res:
            if isinstance(last_res, tuple):
                ok = bool(last_res[0])
                msg_text = str(last_res[1]) if len(last_res) > 1 else ""
                prefix = (msg_text + "\n\n") if msg_text else ""
                if not ok and not msg_text:
                    prefix = _tr(lang, "❌ No pude añadir ese símbolo.\n\n", "❌ I could not add that symbol.\n\n")
            else:
                msg_text = getattr(last_res, "message", "")
                prefix = (msg_text + "\n\n") if msg_text else ""

        await msg.reply_text(prefix + text, reply_markup=kb)

    except Exception:
        logging.exception("Watchlist text input error")
        try:
            lang = _get_user_language(users_collection().find_one({"user_id": int(update.effective_user.id)}) or {})
            await update.effective_message.reply_text(_tr(lang, "❌ No pude añadir ese símbolo. Intenta de nuevo.", "❌ I could not add that symbol. Try again."))
        except Exception:
            pass

async def handle_exchange_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["awaiting_exchange"] = False
        exchange_name = update.message.text.strip()
        users_col = users_collection()
        user_id = update.effective_user.id
        
        loop = asyncio.get_event_loop()
        user = await loop.run_in_executor(
            None,
            lambda: users_col.find_one({"user_id": user_id})
        )

        if not user:
            await update.message.reply_text("❌ Usuario no encontrado.")
            return

        await loop.run_in_executor(
            None,
            lambda: users_col.update_one(
                {"user_id": user_id},
                {"$set": {"exchange": exchange_name}}
            )
        )

        await update.message.reply_text(
            f"✅ Exchange confirmado: {exchange_name}\nMenú principal:",
            reply_markup=main_menu(language=_get_user_language(user)),
        )

    except Exception as e:
        logger.error(f"Error en handle_exchange_text: {e}", exc_info=True)
        await update.message.reply_text("❌ Error al registrar exchange.")
        context.user_data["awaiting_exchange"] = False


async def handle_watchlist_callback(query, context, user, action: str, admin: bool) -> bool:
    user_id = user["user_id"]
    language = _get_user_language(user)

    if action == "watchlist":
        try:
            from app.watchlist import get_symbols
            from app.watchlist_ui import render_watchlist_view
            symbols = get_symbols(int(user_id))
            text, kb = render_watchlist_view(symbols, lang=language)
            context.user_data["watchlist_active"] = True
            await query.edit_message_text(text, reply_markup=kb)
        except Exception:
            logging.exception("Watchlist open error")
            await query.edit_message_text(
                _tr(language, "❌ No pude abrir Watchlist ahora mismo.", "❌ I could not open Watchlist right now."),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]),
            )
        return True

    if action == "wl_refresh":
        try:
            from app.watchlist import get_symbols
            from app.watchlist_ui import render_watchlist_view
            symbols = get_symbols(int(user_id))
            text, kb = render_watchlist_view(symbols, lang=language)
            context.user_data["watchlist_active"] = True
            await query.edit_message_text(text, reply_markup=kb)
        except Exception:
            logging.exception("Watchlist refresh error")
        return True

    if action == "wl_clear":
        try:
            from app.watchlist import clear, get_symbols
            from app.watchlist_ui import render_watchlist_view
            clear(int(user_id))
            symbols = get_symbols(int(user_id))
            text, kb = render_watchlist_view(symbols, lang=language)
            context.user_data["watchlist_active"] = True
            await query.edit_message_text(_tr(language, "🧹 Watchlist limpiada.\n\n", "🧹 Watchlist cleared.\n\n") + text, reply_markup=kb)
        except Exception:
            logging.exception("Watchlist clear error")
            await query.answer(_tr(language, "No pude limpiar.", "I could not clear it."), show_alert=False)
        return True

    if action.startswith("wl_rm:"):
        try:
            from app.watchlist import get_symbols, remove_symbol
            from app.watchlist_ui import render_watchlist_view
            sym = action.split(":", 1)[1]
            remove_symbol(int(user_id), sym)
            symbols = get_symbols(int(user_id))
            text, kb = render_watchlist_view(symbols, lang=language)
            context.user_data["watchlist_active"] = True
            await query.edit_message_text(text, reply_markup=kb)
        except Exception:
            logging.exception("Watchlist remove error")
            await query.answer(_tr(language, "No pude quitar.", "I could not remove it."), show_alert=False)
        return True

    return False
