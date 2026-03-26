import logging
import asyncio
from datetime import datetime, date
from functools import partial
from urllib.parse import quote
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters

from app.database import users_collection
from app.models import is_trial_active, is_plan_active, update_timestamp
from app.binance_api import get_top_movers_usdtm, get_radar_opportunities, get_premium_index, get_open_interest
from app.market_ui import render_market_state
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM, activate_plus, activate_premium, extend_current_plan
from app.signals import get_latest_base_signal_for_plan, format_user_signal, get_user_signal_by_signal_id, get_recent_user_signals_for_user, get_signal_analysis_for_user, get_signal_tracking_for_user
from app.config import is_admin, get_admin_whatsapps
from app.menus import main_menu, back_to_menu, my_account_menu
from app.referrals import get_user_referral_stats, get_referral_link, register_valid_referral, check_ref_rewards
from app.risk import (
    RiskConfigurationError,
    SignalProfileError,
    SignalRiskError,
    build_risk_preview_from_user_signal,
    get_exchange_fee_preset,
    get_user_risk_profile,
    save_user_risk_profile,
)
from app.risk_ui import (
    PROFILE_CODE_TO_NAME,
    build_active_signals_list_keyboard,
    build_active_signals_list_text,
    build_default_profile_selection_keyboard,
    build_default_profile_selection_text,
    build_exchange_selection_keyboard,
    build_exchange_selection_text,
    build_history_list_keyboard,
    build_history_list_text,
    build_risk_management_keyboard,
    build_risk_management_text,
    build_risk_result_keyboard,
    build_risk_result_text,
    build_signal_detail_keyboard,
    build_signal_detail_text,
    build_signal_profile_picker_keyboard,
    build_signal_profile_picker_text,
)

from app.analysis_ui import (
    build_signal_analysis_keyboard,
    build_signal_analysis_text,
)
from app.tracking_ui import (
    build_signal_tracking_keyboard,
    build_signal_tracking_text,
)

try:
    from app.statistics import (
        get_last_days_stats,
        get_last_days_stats_by_plan,
        get_performance_snapshot,
        get_signal_activity_stats,
        get_signal_activity_stats_by_plan,
        get_winrate_by_score,
        reset_statistics,
    )
except ImportError:  # compat
    get_last_days_stats = None
    get_last_days_stats_by_plan = None
    get_performance_snapshot = None
    get_signal_activity_stats = None
    get_signal_activity_stats_by_plan = None
    get_winrate_by_score = None
    reset_statistics = None


logger = logging.getLogger(__name__)

DAILY_LIMITS = {
    PLAN_FREE: 3,
    PLAN_PLUS: 5,
    PLAN_PREMIUM: 7,
}

RISK_INPUT_FIELDS = {
    "capital_usdt": {
        "label": "capital",
        "prompt": "💰 Envía tu capital disponible en USDT.\nEjemplo: 500",
    },
    "risk_percent": {
        "label": "riesgo por trade",
        "prompt": "🎯 Envía el porcentaje que arriesgas por trade.\nEjemplo: 1 o 1.5",
    },
    "fee_percent_per_side": {
        "label": "fee por lado",
        "prompt": "💸 Envía la fee por lado en porcentaje.\nEjemplo: 0.02",
    },
    "slippage_percent": {
        "label": "slippage",
        "prompt": "📉 Envía el slippage estimado en porcentaje.\nEjemplo: 0.03",
    },
    "default_leverage": {
        "label": "apalancamiento por defecto",
        "prompt": "📈 Envía el apalancamiento por defecto.\nEjemplo: 20 o 35",
    },
}

# ======================================================
# FUNCIONES AUXILIARES
# ======================================================

def format_whatsapp_contacts():
    whatsapps = get_admin_whatsapps()
    if not whatsapps:
        return "WhatsApp: (no configurado)"
    if len(whatsapps) == 1:
        return f"WhatsApp: {whatsapps[0]}"
    return "WhatsApps:\n- " + "\n- ".join(whatsapps)

def _wa_link(phone: str, message: str) -> str:
    # Si ya es un enlace completo de WhatsApp, usarlo directamente
    phone_str = str(phone)
    if phone_str.startswith("http://") or phone_str.startswith("https://"):
        if "text=" in phone_str:
            return phone_str
        sep = "&" if "?" in phone_str else "?"
        return f"{phone_str}{sep}text={quote(message)}"

    # Si es número, construir enlace wa.me
    clean = "".join(ch for ch in phone_str if ch.isdigit())
    return f"https://wa.me/{clean}?text={quote(message)}"


def parse_ref_code(start_param: str) -> int | None:
    """Extrae user_id del referidor desde start parameter de Telegram"""
    if not start_param:
        return None
    if start_param.startswith("ref_"):
        try:
            return int(start_param.split("_")[1])
        except ValueError:
            return None
    return None

# ======================================================
# HANDLER /start (inserta referidos)
# ======================================================

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja /start y captura referidos"""
    try:
        user_id = update.effective_user.id
        start_param = context.args[0] if context.args else None
        referrer_id = parse_ref_code(start_param)

        users_col = users_collection()
        user = users_col.find_one({"user_id": user_id})

        if user and user.get("banned"):
            await update.message.reply_text("🚫 Tu acceso al bot ha sido revocado.")
            return

        if not user:
            doc = {"user_id": user_id, "plan": PLAN_FREE, "ref_plus_valid": 0,
                   "ref_premium_valid": 0, "ref_plus_total": 0, "ref_premium_total": 0}
            if referrer_id and referrer_id != user_id:
                doc["referred_by"] = referrer_id
            users_col.insert_one(doc)
        else:
            if referrer_id and referrer_id != user_id and "referred_by" not in user:
                users_col.update_one({"user_id": user_id}, {"$set": {"referred_by": referrer_id}})

        await update.message.reply_text(
            "👋 Bienvenido al bot de señales.\nMenú principal:",
                reply_markup=main_menu(is_admin=is_admin(user_id)),
        )

    except Exception as e:
        logger.error(f"Error en handle_start: {e}", exc_info=True)
        await update.message.reply_text("❌ Error al iniciar el bot.")

# ======================================================
# HANDLER MENÚ PRINCIPAL
# ======================================================

async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    try:
        user_id = query.from_user.id
        users_col = users_collection()
        user = users_col.find_one({"user_id": user_id})

        if user and user.get("banned"):
            await query.edit_message_text("🚫 Tu acceso al bot ha sido revocado.")
            return

        if not user:
            await query.edit_message_text(
                "Usuario no encontrado. Usa /start nuevamente.",
                reply_markup=main_menu(),
            )
            return

        action = query.data
        admin = is_admin(user_id)
        plan = (user.get("plan") or PLAN_FREE)

        if await _handle_risk_dynamic_callbacks(query, context, user, action):
            return

        if action == "admin_panel" and admin:
            await query.edit_message_text(
                "👑 PANEL ADMINISTRADOR",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Activar plan", callback_data="admin_activate_plan")],
                    [InlineKeyboardButton("🗑 Eliminar usuario", callback_data="admin_delete_user")],
                    [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
                ])
            )
            return

        if action == "admin_delete_user" and admin:
            context.user_data["awaiting_delete_user_id"] = True
            await query.edit_message_text("🆔 Envía el User ID del usuario a eliminar:")
            return

        if action == "admin_activate_plan" and admin:
            context.user_data["awaiting_user_id"] = True
            await query.edit_message_text("🆔 Envía el User ID del usuario:")
            return

        if action == "view_signals":
            await handle_view_signals(query, user, admin, users_col)
            return

        if action == "plans":
            await handle_plans(query, user)
            return

        if action == "my_account":
            await handle_my_account(query, user, admin)
            return

        if action == "referrals":
            await handle_referrals(query, user)
            return


        # Nuevos módulos (Menú PRO)
        if action == "performance":
            await handle_performance(query, user)
            return

        if action == "reset_stats":
            await handle_reset_stats(query, user)
            return

        if action == "radar":
            plan = (user.get("plan") or PLAN_FREE)
            await handle_radar(query, user, plan)
            return

        if action == "radar_refresh":
            plan = (user.get("plan") or PLAN_FREE)
            await handle_radar(query, user, plan)
            return

        if action == "movers":
            await handle_movers(query, user)
            return

        if action == "market":
            await handle_market(query, user)
            return

        if action == "market_refresh":
            await handle_market(query, user)
            return

        if action == "watchlist":
            # Mostrar Watchlist (modo activo para capturar texto del usuario)
            try:
                from app.watchlist import get_symbols
                from app.watchlist_ui import render_watchlist_view
                symbols = get_symbols(int(user_id))
                text, kb = render_watchlist_view(symbols)
                context.user_data["watchlist_active"] = True
                await query.edit_message_text(text, reply_markup=kb)
            except Exception:
                logging.exception("Watchlist open error")
                await query.edit_message_text("❌ No pude abrir Watchlist ahora mismo.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")]]))
            
        # Watchlist callbacks
        if action == "wl_refresh":
            try:
                from app.watchlist import get_symbols
                from app.watchlist_ui import render_watchlist_view
                symbols = get_symbols(int(user_id))
                text, kb = render_watchlist_view(symbols)
                context.user_data["watchlist_active"] = True
                await query.edit_message_text(text, reply_markup=kb)
            except Exception:
                logging.exception("Watchlist refresh error")
                pass
            return

        if action == "wl_clear":
            try:
                from app.watchlist import clear, get_symbols
                from app.watchlist_ui import render_watchlist_view
                clear(int(user_id))
                symbols = get_symbols(int(user_id))
                text, kb = render_watchlist_view(symbols)
                context.user_data["watchlist_active"] = True
                await query.edit_message_text("🧹 Watchlist limpiada.\n\n" + text, reply_markup=kb)
            except Exception:
                logging.exception("Watchlist clear error")
                await query.answer("No pude limpiar.", show_alert=False)
            return

        if action.startswith("wl_rm:"):
            try:
                from app.watchlist import remove_symbol, get_symbols
                from app.watchlist_ui import render_watchlist_view
                sym = action.split(":", 1)[1]
                remove_symbol(int(user_id), sym)
                symbols = get_symbols(int(user_id))
                text, kb = render_watchlist_view(symbols)
                context.user_data["watchlist_active"] = True
                await query.edit_message_text(text, reply_markup=kb)
            except Exception:
                logging.exception("Watchlist remove error")
                await query.answer("No pude quitar.", show_alert=False)
            return


        if action == "alerts":
            await handle_alerts(query, user)
            return

        if action == "alerts_refresh":
            await handle_alerts(query, user)
            return

        if action == "history":
            await handle_history(query, user)
            return

        if action == "history_refresh":
            await handle_history(query, user)
            return
        if action == "support":
            await handle_support(query)
            return

        if action == "register_exchange":
            context.user_data["awaiting_exchange"] = True
            await query.edit_message_text(
                "🌐 Envía el nombre de tu exchange (ej: Binance, CoinEx, KuCoin):"
            )
            return

        if action == "back_menu":
            context.user_data["watchlist_active"] = False
            await query.edit_message_text(
"🏠 MENÚ PRINCIPAL — Selecciona una opción abajo",
                reply_markup=main_menu(is_admin=admin),
            )
            return

        if action in ["choose_plus_plan", "choose_premium_plan"]:
            target_user_id = context.user_data.get("target_user_id")
            if target_user_id:
                loop = asyncio.get_event_loop()
                if action == "choose_plus_plan":
                    success = await loop.run_in_executor(
                        None,
                        partial(activate_plus, target_user_id)
                    )
                    plan_name = "PLUS"
                else:
                    success = await loop.run_in_executor(
                        None,
                        partial(activate_premium, target_user_id)
                    )
                    plan_name = "PREMIUM"

                if success:
                    register_valid_referral(target_user_id, plan_name)
                    await query.edit_message_text(f"✅ Plan {plan_name} activado correctamente.")
                else:
                    await query.edit_message_text(f"❌ No se pudo activar el plan {plan_name}.")

                context.user_data.pop("awaiting_plan_choice", None)
                context.user_data.pop("target_user_id", None)
            return

    except Exception as e:
        logger.error(f"Error en handle_menu: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Ocurrió un error inesperado.",
            reply_markup=main_menu(),
        )

# ======================================================
# HANDLER REFERRALS
# ======================================================

async def handle_referrals(query, user):
    try:
        user_id = user["user_id"]
        stats = get_user_referral_stats(user_id)
        if not stats:
            await query.edit_message_text(
                "❌ No se pudo cargar la información de referidos.",
                reply_markup=back_to_menu(),
            )
            return

        ref_link = get_referral_link(user_id)

        valid_total = stats.get("valid_referrals_total")
        if valid_total is None:
            valid_total = stats.get("current_plus", 0) + stats.get("current_premium", 0)

        reward_days_total = stats.get("reward_days_total", valid_total * 7)

        message = "👥 SISTEMA DE REFERIDOS\n\n"
        message += f"🔗 Tu enlace de referido:\n{ref_link}\n\n"

        message += "📊 ESTADÍSTICAS:\n"
        message += f"• Referidos válidos totales: {valid_total}\n"
        message += f"• Referidos PLUS: {stats.get('plus_referred', 0)}\n"
        message += f"• Referidos PREMIUM: {stats.get('premium_referred', 0)}\n\n"

        message += "🎁 RECOMPENSAS:\n"
        message += f"• Días acumulados por referidos: +{reward_days_total} días\n"
        message += "• Cada referido válido suma +7 días a tu plan actual\n\n"

        message += "📢 CÓMO REFERIR:\n"
        message += "1. Comparte tu enlace\n"
        message += "2. Ellos entran al bot\n"
        message += "3. Activan un plan\n\n"

        message += "📌 REGLA ACTUAL:\n"
        message += "• Cada referido válido agrega +7 días a tu plan actual\n"
        message += "• El tipo de referido (PLUS/PREMIUM) se muestra solo como estadística\n"

        keyboard = [
            [InlineKeyboardButton("📋 Copiar enlace", callback_data="copy_ref_code")],
            [InlineKeyboardButton("⬅️ Volver al menú", callback_data="back_menu")]
        ]

        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    except Exception as e:
        logger.error(f"Error en handle_referrals: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Error al cargar información de referidos.",
            reply_markup=back_to_menu(),
        )

  # ======================================================
# HANDLER COPIAR ENLACE DE REFERIDO
# ======================================================

async def handle_copy_ref_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        user_id = query.from_user.id
        ref_link = get_referral_link(user_id)

        await query.edit_message_text(
            text=(
                f"📋 Tu enlace de referido es:\n\n{ref_link}\n\nCópialo y compártelo."
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Volver a referidos", callback_data="referrals")],
                [InlineKeyboardButton("🏠 Menú principal", callback_data="back_menu")]
            ]),
        )

    except Exception as e:
        logger.error(f"Error en handle_copy_ref_code: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Error al copiar enlace.",
            reply_markup=main_menu(),
        )

# ======================================================
# HANDLER DE MENSAJES DE TEXTO COMBINADO
# ======================================================

async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja todos los mensajes de texto, decidiendo el flujo correcto"""

    try:
        user_id = update.effective_user.id
        user = users_collection().find_one({"user_id": user_id})
        if user and user.get("banned"):
            await update.message.reply_text("🚫 Tu acceso al bot ha sido revocado.")
            return
    except Exception:
        pass

    # Watchlist: capturar símbolos escritos por el usuario
    if context.user_data.get("watchlist_active"):
        await handle_watchlist_text_input(update, context)
        return

    if context.user_data.get("awaiting_user_id") or context.user_data.get("awaiting_delete_user_id"):
        await handle_admin_text_input(update, context)
        return
    
    if context.user_data.get("awaiting_exchange"):
        await handle_exchange_text_input(update, context)
        return

    if context.user_data.get("awaiting_risk_field"):
        await handle_risk_text_input(update, context)
        return


# ======================================================
# WATCHLIST (MENSAJES DE TEXTO)
# ======================================================

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

        from app.watchlist import normalize_many, add_symbol, get_symbols
        from app.watchlist_ui import render_watchlist_view

        symbols = normalize_many(raw)
        if not symbols:
            await msg.reply_text("❌ Símbolo inválido. Ej: BTCUSDT")
            return

        # Obtener plan (FREE/PLUS/PREMIUM) desde users_collection
        try:
            udoc = users_collection().find_one({"user_id": int(user.id)}) or {}
        except Exception:
            udoc = {}
        plan = (udoc.get("plan") or "FREE").upper()

        last_res = None
        for s in symbols:
            last_res = add_symbol(int(user.id), s, plan=plan)

        # Render actualizado
        current = get_symbols(int(user.id))
        text, kb = render_watchlist_view(current)
        prefix = (last_res.message + "\n\n") if last_res else ""
        await msg.reply_text(prefix + text, reply_markup=kb)

    except Exception:
        logging.exception("Watchlist text input error")
        try:
            await update.effective_message.reply_text("❌ No pude añadir ese símbolo. Intenta de nuevo.")
        except Exception:
            pass


# ======================================================
# HANDLER REGISTRAR EXCHANGE (MENSAJE CONFIRMACIÓN)
# ======================================================

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
            reply_markup=main_menu(),
        )

    except Exception as e:
        logger.error(f"Error en handle_exchange_text: {e}", exc_info=True)
        await update.message.reply_text("❌ Error al registrar exchange.")
        context.user_data["awaiting_exchange"] = False



async def _show_risk_management(query, user):
    profile = get_user_risk_profile(user["user_id"])
    plan = user.get("plan", PLAN_FREE)
    await query.edit_message_text(
        build_risk_management_text(profile, plan=plan),
        reply_markup=build_risk_management_keyboard(plan=plan),
    )


async def _show_signal_detail(query, user, signal_id: str, source: str = "live"):
    user_signal = get_user_signal_by_signal_id(user["user_id"], signal_id)
    if not user_signal:
        await query.edit_message_text(
            "❌ No pude encontrar esa señal en la base de datos.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="history" if source == "hist" else "view_signals")]]),
        )
        return

    await query.edit_message_text(
        build_signal_detail_text(user_signal, source=source),
        reply_markup=build_signal_detail_keyboard(signal_id, source=source, plan=user.get("plan", PLAN_FREE)),
    )


async def _show_signal_analysis(query, user, signal_id: str, source: str = "live"):
    profile_name = _effective_risk_profile_name(user)
    analysis = get_signal_analysis_for_user(user["user_id"], signal_id, profile_name=profile_name)
    if not analysis:
        await query.edit_message_text(
            "❌ No pude cargar el análisis de esa señal.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="history" if source == "hist" else "view_signals")]]),
        )
        return

    await query.edit_message_text(
        build_signal_analysis_text(analysis),
        reply_markup=build_signal_analysis_keyboard(signal_id, source=source),
    )




async def _show_signal_tracking(query, user, signal_id: str, source: str = "live"):
    profile_name = _effective_risk_profile_name(user)
    payload = get_signal_tracking_for_user(user["user_id"], signal_id, profile_name=profile_name)
    if not payload:
        await query.edit_message_text(
            "❌ No pude cargar el seguimiento de esa señal.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="history" if source == "hist" else "view_signals")]]),
        )
        return

    await query.edit_message_text(
        build_signal_tracking_text(payload),
        reply_markup=build_signal_tracking_keyboard(signal_id, source=source),
    )

async def _show_risk_result(query, user, signal_id: str, source: str = "live", profile_name: str | None = None):
    user_signal = get_user_signal_by_signal_id(user["user_id"], signal_id)
    if not user_signal:
        await query.edit_message_text(
            "❌ No pude cargar la señal para calcular el riesgo.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="history" if source == "hist" else "view_signals")]]),
        )
        return

    plan = user.get("plan", PLAN_FREE)
    effective_profile = _effective_risk_profile_name(user, requested=profile_name)

    try:
        calc = build_risk_preview_from_user_signal(
            user_signal,
            risk_profile=get_user_risk_profile(user["user_id"]),
            profile_name=effective_profile,
        )
    except RiskConfigurationError as exc:
        back_cb = f"sig_detail:{signal_id}" if source == "live" else (f"hist_detail:{signal_id}" if source == "hist" else "risk_menu")
        await query.edit_message_text(
            f"⚠️ No pude calcular el riesgo todavía.\n\n{exc}\n\nConfigura primero tu perfil de riesgo.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⚙️ Gestión de riesgo", callback_data="risk_menu")],
                [InlineKeyboardButton("⬅️ Volver", callback_data=back_cb)],
            ]),
        )
        return
    except (SignalProfileError, SignalRiskError) as exc:
        back_cb = f"sig_detail:{signal_id}" if source == "live" else (f"hist_detail:{signal_id}" if source == "hist" else "risk_menu")
        rows = []
        if signal_id and _risk_feature_tier(plan) == "full":
            rows.append([InlineKeyboardButton("🧭 Elegir perfil", callback_data=f"risk_pf:{source}:{signal_id}")])
        elif signal_id and _risk_feature_tier(plan) == "basic":
            rows.append([InlineKeyboardButton("💼 Ver planes", callback_data="plans")])
        rows.append([InlineKeyboardButton("⬅️ Volver", callback_data=back_cb)])
        await query.edit_message_text(
            f"⚠️ No pude calcular el riesgo para esta señal.\n\n{exc}\n\nLa configuración del usuario sí está cargada, pero esta señal o el perfil elegido no son coherentes para el cálculo.",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    await query.edit_message_text(
        build_risk_result_text(calc, plan=plan),
        reply_markup=build_risk_result_keyboard(signal_id, source=source, plan=plan),
    )


async def _handle_risk_dynamic_callbacks(query, context, user, action: str) -> bool:
    user_id = user["user_id"]

    if action == "risk_menu":
        await _show_risk_management(query, user)
        return True

    if action == "risk_set_capital":
        context.user_data["awaiting_risk_field"] = "capital_usdt"
        await query.edit_message_text(RISK_INPUT_FIELDS["capital_usdt"]["prompt"], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="risk_menu")]]))
        return True

    if action == "risk_set_risk":
        context.user_data["awaiting_risk_field"] = "risk_percent"
        await query.edit_message_text(RISK_INPUT_FIELDS["risk_percent"]["prompt"], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="risk_menu")]]))
        return True

    if action == "risk_set_fee":
        context.user_data["awaiting_risk_field"] = "fee_percent_per_side"
        await query.edit_message_text(RISK_INPUT_FIELDS["fee_percent_per_side"]["prompt"], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="risk_menu")]]))
        return True

    if action == "risk_set_slippage":
        context.user_data["awaiting_risk_field"] = "slippage_percent"
        await query.edit_message_text(RISK_INPUT_FIELDS["slippage_percent"]["prompt"], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="risk_menu")]]))
        return True

    if action == "risk_set_leverage":
        context.user_data["awaiting_risk_field"] = "default_leverage"
        await query.edit_message_text(RISK_INPUT_FIELDS["default_leverage"]["prompt"], reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="risk_menu")]]))
        return True

    if action == "risk_set_exchange":
        profile = get_user_risk_profile(user_id)
        await query.edit_message_text(
            build_exchange_selection_text(profile.get("exchange")),
            reply_markup=build_exchange_selection_keyboard(profile.get("exchange")),
        )
        return True

    if action.startswith("risk_pick_exchange:"):
        exchange = action.split(":", 1)[1]
        current = get_user_risk_profile(user_id)
        preset = get_exchange_fee_preset(exchange, current.get("entry_mode"))
        save_user_risk_profile(
            user_id,
            {
                "exchange": preset["exchange"],
                "fee_percent_per_side": preset["fee_percent_per_side"],
                "slippage_percent": preset["slippage_percent"],
            },
        )
        updated = get_user_risk_profile(user_id)
        await query.edit_message_text(
            "✅ Exchange actualizado. También recargué fee y slippage estimados.\n\n" + build_risk_management_text(updated),
            reply_markup=build_risk_management_keyboard(),
        )
        return True

    if action == "risk_set_profile":
        if _risk_feature_tier(user.get("plan", PLAN_FREE)) == "basic":
            await query.edit_message_text(
                "🔒 Perfil base avanzado\n\nEn FREE la calculadora usa el perfil Moderado.\n\nSube a PLUS o PREMIUM para elegir Conservador, Moderado o Agresivo.",
                reply_markup=_build_risk_plan_upgrade_markup("risk_menu"),
            )
            return True

        profile = get_user_risk_profile(user_id)
        await query.edit_message_text(
            build_default_profile_selection_text(profile.get("default_profile")),
            reply_markup=build_default_profile_selection_keyboard(profile.get("default_profile"), plan=user.get("plan", PLAN_FREE)),
        )
        return True

    if action.startswith("risk_pick_profile:"):
        profile_name = action.split(":", 1)[1]
        save_user_risk_profile(user_id, {"default_profile": profile_name})
        updated = get_user_risk_profile(user_id)
        await query.edit_message_text(
            "✅ Perfil base actualizado.\n\n" + build_risk_management_text(updated),
            reply_markup=build_risk_management_keyboard(),
        )
        return True

    if action == "risk_test":
        recent = get_recent_user_signals_for_user(user_id, limit=1, active_only=False)
        if not recent:
            await query.edit_message_text(
                "📭 No tienes señales todavía para probar la calculadora.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="risk_menu")]]),
            )
            return True
        signal_id = recent[0].get("signal_id") or str(recent[0].get("_id") or "")
        await _show_risk_result(query, user, signal_id, source="test")
        return True

    if action.startswith("sig_detail:"):
        signal_id = action.split(":", 1)[1]
        await _show_signal_detail(query, user, signal_id, source="live")
        return True

    if action.startswith("hist_detail:"):
        signal_id = action.split(":", 1)[1]
        await _show_signal_detail(query, user, signal_id, source="hist")
        return True

    if action.startswith("risk_calc:"):
        _, source, signal_id = action.split(":", 2)
        await _show_risk_result(query, user, signal_id, source=source)
        return True

    if action.startswith("sig_an:"):
        _, source, signal_id = action.split(":", 2)
        await _show_signal_analysis(query, user, signal_id, source=source)
        return True

    if action.startswith("sig_trk:"):
        _, source, signal_id = action.split(":", 2)
        await _show_signal_tracking(query, user, signal_id, source=source)
        return True

    if action.startswith("risk_pf:"):
        _, source, signal_id = action.split(":", 2)
        current = get_user_risk_profile(user_id)
        await query.edit_message_text(
            build_signal_profile_picker_text(current.get("default_profile"), source=source, plan=user.get("plan", PLAN_FREE)),
            reply_markup=build_signal_profile_picker_keyboard(signal_id, source=source, selected_profile=current.get("default_profile"), plan=user.get("plan", PLAN_FREE)),
        )
        return True

    if action.startswith("risk_cp:"):
        _, source, signal_id, profile_code = action.split(":", 3)
        if _risk_feature_tier(user.get("plan", PLAN_FREE)) == "basic":
            back_cb = f"sig_detail:{signal_id}" if source == "live" else (f"hist_detail:{signal_id}" if source == "hist" else "risk_menu")
            await query.edit_message_text(
                "🔒 Cambio de perfil\n\nEn FREE la calculadora usa el perfil Moderado.\n\nSube a PLUS o PREMIUM para elegir otros perfiles.",
                reply_markup=_build_risk_plan_upgrade_markup(back_cb),
            )
            return True
        profile_name = PROFILE_CODE_TO_NAME.get(profile_code)
        if not profile_name:
            await query.answer("Perfil inválido", show_alert=False)
            return True
        await _show_risk_result(query, user, signal_id, source=source, profile_name=profile_name)
        return True

    return False


async def handle_risk_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    field = context.user_data.get("awaiting_risk_field")
    if field not in RISK_INPUT_FIELDS:
        return

    raw = (update.effective_message.text or "").strip()
    user_id = update.effective_user.id

    try:
        if field == "default_leverage":
            value = float(raw.lower().replace("x", "").replace(",", "."))
            if value < 1:
                raise ValueError
        else:
            value = float(raw.replace(",", "."))
            if field in {"capital_usdt", "risk_percent"} and value <= 0:
                raise ValueError
            if field in {"fee_percent_per_side", "slippage_percent"} and value < 0:
                raise ValueError
    except Exception:
        await update.effective_message.reply_text(
            f"❌ Valor inválido para {RISK_INPUT_FIELDS[field]['label']}.\n\n{RISK_INPUT_FIELDS[field]['prompt']}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="risk_menu")]]),
        )
        return

    context.user_data["awaiting_risk_field"] = None
    try:
        profile = save_user_risk_profile(user_id, {field: value})
    except RiskConfigurationError as exc:
        await update.effective_message.reply_text(f"❌ {exc}")
        return

    await update.effective_message.reply_text(
        "✅ Ajuste guardado.\n\n" + build_risk_management_text(profile),
        reply_markup=build_risk_management_keyboard(),
    )


# ======================================================
# HANDLER VIEW SIGNALS (CORREGIDO SIN LÍMITE)
# ======================================================

async def handle_view_signals(query, user, admin, users_col):
    try:
        user_id = user["user_id"]
        plan = PLAN_PREMIUM if admin else user.get("plan", PLAN_FREE)

        if not admin and not (is_plan_active(user) or is_trial_active(user)):
            await query.edit_message_text(
                "⛔ Acceso expirado.",
                reply_markup=back_to_menu(),
            )
            return

        user_signals = get_latest_base_signal_for_plan(user_id, plan)
        if not user_signals:
            await query.edit_message_text(
                "📭 No hay señales activas disponibles.",
                reply_markup=back_to_menu(),
            )
            return

        await query.edit_message_text(
            build_active_signals_list_text(user_signals),
            reply_markup=build_active_signals_list_keyboard(user_signals),
        )

        users_col.update_one(
            {"user_id": user_id},
            {"$set": update_timestamp(user)}
        )

    except Exception as e:
        logger.error(f"Error en handle_view_signals: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Error al obtener señales.",
            reply_markup=back_to_menu(),
        )

# ======================================================
# HANDLER PLANS
# ======================================================

async def handle_plans(query, user):
    plan = str(user.get("plan", PLAN_FREE)).upper()
    user_id = user.get("user_id")
    whatsapps = get_admin_whatsapps()

    message = (
        "💼 PLANES HADES ALPHA\n\n"
        f"📊 Tu plan actual: {plan}\n\n"
        "🟢 FREE\n"
        "• Acceso básico al bot\n"
        "• Funciones limitadas\n\n"
        "🟡 PLUS\n"
        "• Señales completas\n"
        "• Radar Futures\n"
        "• Movers\n"
        "• Watchlist PRO\n"
        "• Historial\n\n"
        "🔴 PREMIUM\n"
        "• Todo lo de PLUS\n"
        "• 🔔 Alertas premium\n"
        "• Oportunidades antes de la señal\n"
        "• Acceso completo al ecosistema\n\n"
        "👇 Selecciona el plan que deseas activar:"
    )

    keyboard_rows = []

    plus_msg = f"Hola, quiero activar el plan PLUS de HADES ALPHA. Mi ID de Telegram es: {user_id}"
    premium_msg = f"Hola, quiero activar el plan PREMIUM de HADES ALPHA. Mi ID de Telegram es: {user_id}"

    if len(whatsapps) >= 1:
        keyboard_rows.append([
            InlineKeyboardButton("💬 PLUS · Admin 1", url=_wa_link(whatsapps[0], plus_msg))
        ])
        keyboard_rows.append([
            InlineKeyboardButton("💬 PREMIUM · Admin 1", url=_wa_link(whatsapps[0], premium_msg))
        ])

    if len(whatsapps) >= 2:
        keyboard_rows.append([
            InlineKeyboardButton("💬 PLUS · Admin 2", url=_wa_link(whatsapps[1], plus_msg))
        ])
        keyboard_rows.append([
            InlineKeyboardButton("💬 PREMIUM · Admin 2", url=_wa_link(whatsapps[1], premium_msg))
        ])

    if not keyboard_rows:
        message += "\n\n⚠️ No hay contactos de WhatsApp configurados todavía."

    keyboard_rows.append([InlineKeyboardButton("⬅️ Volver al menú", callback_data="back_menu")])

    await query.edit_message_text(
        message,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )

# ======================================================
# HANDLER MY ACCOUNT
# ======================================================


async def handle_my_account(query, user, admin=False):
    now = datetime.utcnow()

    plan = user.get("plan", PLAN_FREE)
    plan_end = user.get("plan_end")
    trial_end = user.get("trial_end")

    days_left = None
    expires_str = "—"

    if plan_end:
        try:
            delta = plan_end - now
            days_left = max(delta.days, 0)
            expires_str = plan_end.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass
    elif trial_end:
        try:
            delta = trial_end - now
            days_left = max(delta.days, 0)
            expires_str = trial_end.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass

    risk_profile = get_user_risk_profile(user["user_id"])
    message = (
        f"👤 MI CUENTA\n\n"
        f"ID: {user['user_id']}\n"
        f"Plan: {plan.upper()}\n"
    )

    if days_left is not None:
        message += f"📅 Días restantes: {days_left}\n"
        message += f"⏳ Expira: {expires_str}\n"

    message += "\nConfiguración de riesgo:\n"
    message += f"• Capital: {risk_profile.get('capital_usdt', 0):,.2f} USDT\n"
    message += f"• Riesgo/trade: {float(risk_profile.get('risk_percent') or 0):.2f}%\n"
    message += f"• Perfil base: {risk_profile.get('default_profile', 'moderado').title()}\n"
    message += f"• Exchange: {str(risk_profile.get('exchange') or '—').upper()}\n"

    await query.edit_message_text(
        text=message,
        reply_markup=my_account_menu(),
    )

# ======================================================
# HANDLER SUPPORT
# ======================================================


async def handle_history(query, user):
    plan = user.get("plan", PLAN_FREE)
    if _plan_rank(plan) < _plan_rank(PLAN_PLUS):
        await query.edit_message_text(
            "🔒 🧾 Historial\n\nDisponible para *PLUS* y *PREMIUM*.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💼 Planes", callback_data="plans")],
                [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    try:
        docs = get_recent_user_signals_for_user(user["user_id"], limit=10, active_only=False)

        if not docs:
            try:
                await query.edit_message_text(
                    "🧾 HISTORIAL\n\nAún no tienes señales registradas en tu historial.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Actualizar", callback_data="history_refresh")],
                        [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
                    ]),
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    await query.answer("✅ Actualizado", show_alert=False)
                else:
                    raise
            return

        try:
            await query.edit_message_text(
                build_history_list_text(docs),
                reply_markup=build_history_list_keyboard(docs),
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                await query.answer("✅ Actualizado", show_alert=False)
            else:
                raise

    except Exception as e:
        logger.error(f"Error en handle_history: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ No pude cargar tu historial ahora mismo.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
            ]),
        )


async def handle_alerts(query, user):
    plan = user.get("plan", PLAN_FREE)

    if _plan_rank(plan) < _plan_rank(PLAN_PREMIUM):
        await query.edit_message_text(
            "🔒 ALERTAS PREMIUM\n\nDisponible solo para *PREMIUM*.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💼 Planes", callback_data="plans")],
                [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    def _momentum_label(op):
        raw = op.get("momentum")
        if raw not in (None, "", "—", "-"):
            return str(raw)

        change = op.get("priceChangePercent", op.get("change_24h", op.get("change")))
        try:
            change = abs(float(change))
            if change >= 6:
                return "Muy alto"
            if change >= 3:
                return "Alto"
            if change >= 1.5:
                return "Medio"
            return "Bajo"
        except Exception:
            pass

        try:
            score_val = float(op.get("score", 0))
            if score_val >= 95:
                return "Muy alto"
            if score_val >= 85:
                return "Alto"
            if score_val >= 75:
                return "Medio"
            return "Bajo"
        except Exception:
            return "Medio"

    try:
        opportunities = get_radar_opportunities()

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Actualizar", callback_data="alerts_refresh")],
            [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
        ])

        if not opportunities:
            try:
                await query.edit_message_text(
                    "🔔 ALERTAS\n\nNo hay oportunidades detectadas ahora mismo.",
                    reply_markup=keyboard,
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    await query.answer("✅ Actualizado", show_alert=False)
                else:
                    raise
            return

        lines = ["🔔 ALERTAS PREMIUM", ""]

        for i, o in enumerate(opportunities[:5], 1):
            symbol = o.get("symbol", "—")
            score = o.get("score", "—")
            momentum = _momentum_label(o)
            lines.append(f"{i}. {symbol}")
            lines.append(f"   Score: {score} | Momentum: {momentum}")
            lines.append("")

        try:
            await query.edit_message_text(
                "\n".join(lines).strip(),
                reply_markup=keyboard,
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                await query.answer("✅ Actualizado", show_alert=False)
            else:
                raise

    except Exception as e:
        logger.error(f"Error en alerts: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ No pude cargar alertas ahora mismo.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
            ]),
        )

async def handle_support(query):
    await query.edit_message_text(
        f"📩 SOPORTE\n\n{format_whatsapp_contacts()}",
        reply_markup=back_to_menu(),
    )

# ======================================================
# HANDLER ADMIN TEXT INPUT
# ======================================================


# ======================================================
# MENÚ PRO - MÓDULOS NUEVOS
# ======================================================

def _plan_rank(plan: str) -> int:
    if plan == PLAN_PREMIUM:
        return 3
    if plan == PLAN_PLUS:
        return 2
    return 1



def _risk_feature_tier(plan: str) -> str:
    return "basic" if _plan_rank(plan) < _plan_rank(PLAN_PLUS) else "full"


def _effective_risk_profile_name(user: dict, requested: str | None = None) -> str:
    plan = user.get("plan", PLAN_FREE)
    if _risk_feature_tier(plan) == "basic":
        return "moderado"
    requested_value = str(requested or "").strip().lower()
    if requested_value in {"conservador", "moderado", "agresivo"}:
        return requested_value
    return get_user_risk_profile(user["user_id"]).get("default_profile") or "moderado"


def _build_risk_plan_upgrade_markup(back_cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💼 Ver planes", callback_data="plans")],
        [InlineKeyboardButton("⬅️ Volver", callback_data=back_cb)],
    ])


async def handle_locked_or_soon(query, user, feature: str, required_plan: str):
    """Muestra mensaje de bloqueado por plan o "próximamente" si aún no está implementado."""
    plan = user.get("plan", PLAN_FREE)

    # Bloqueo por plan (si requiere PLUS/PREMIUM)
    if _plan_rank(plan) < _plan_rank(required_plan):
        await query.edit_message_text(
            f"🔒 {feature}\n\nDisponible en plan {required_plan.upper()}.\n\nPulsa *Planes* para activar tu acceso.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💼 Planes", callback_data="plans")],
                [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    # Si el plan permite, pero aún no está implementado
    await query.edit_message_text(
        f"🚧 {feature}\n\nEsta función está en desarrollo y se activará muy pronto.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
        ]),
    )

async def handle_performance(query, user):
    plan = user.get("plan", PLAN_FREE)
    admin = is_admin(user.get("user_id"))

    if _plan_rank(plan) < _plan_rank(PLAN_PLUS):
        await query.edit_message_text(
            "🔒 🎯 Rendimiento\n\nDisponible para *PLUS* y *PREMIUM*.\n\nActiva tu plan para ver estadísticas reales del bot.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💼 Planes", callback_data="plans")],
                [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    snapshot = None
    try:
        if callable(get_performance_snapshot):
            snapshot = get_performance_snapshot(
                short_days=7,
                long_days=30,
                worst_symbols_limit=3,
                worst_symbols_min_resolved=3,
            )
    except Exception as e:
        logger.error(f"Error construyendo snapshot de performance: {e}", exc_info=True)
        snapshot = None

    if snapshot:
        s7 = snapshot.get("summary_7d", {})
        s30 = snapshot.get("summary_30d", {})
        by_plan_30 = snapshot.get("by_plan_30d", {})
        act7 = snapshot.get("activity_7d", {})
        act30 = snapshot.get("activity_30d", {})
        act_by_plan_30 = snapshot.get("activity_by_plan_30d", {})
        by_score_30 = snapshot.get("by_score_30d", {})
        diagnostics_30 = snapshot.get("diagnostics_30d", {})
        direction_30 = snapshot.get("direction_30d", [])
        worst_symbols_30 = snapshot.get("worst_symbols_30d", [])
        setup_groups_30 = snapshot.get("setup_groups_30d", [])
    else:
        s7 = s30 = act7 = act30 = None
        by_plan_30 = None
        act_by_plan_30 = None
        by_score_30 = None
        diagnostics_30 = None
        direction_30 = []
        worst_symbols_30 = []
        setup_groups_30 = []

        if callable(get_last_days_stats):
            s7 = get_last_days_stats(7)
            s30 = get_last_days_stats(30)
        else:
            from app.statistics import get_weekly_stats, get_monthly_stats
            s7 = get_weekly_stats()
            s30 = get_monthly_stats()

        if callable(get_last_days_stats_by_plan):
            try:
                by_plan_30 = get_last_days_stats_by_plan(30)
            except Exception:
                by_plan_30 = None

        if callable(get_signal_activity_stats):
            act7 = get_signal_activity_stats(7)
            act30 = get_signal_activity_stats(30)

        if callable(get_signal_activity_stats_by_plan):
            try:
                act_by_plan_30 = get_signal_activity_stats_by_plan(30)
            except Exception:
                act_by_plan_30 = None

        if callable(get_winrate_by_score):
            try:
                by_score_30 = get_winrate_by_score(30)
            except Exception:
                by_score_30 = None

    def _fmt_stats(label: str, s: dict) -> str:
        total = s.get("total", 0)
        won = s.get("won", 0)
        lost = s.get("lost", 0)
        expired = s.get("expired", 0)
        resolved = s.get("resolved", won + lost)
        winrate = s.get("winrate", 0.0)
        return (
            f"**{label}**\n"
            f"• Evaluadas: {total}\n"
            f"• Ganadas: {won} | Perdidas: {lost} | Expiradas: {expired}\n"
            f"• Resueltas: {resolved} | Win rate: {winrate}%\n"
        )

    def _plan_stats_line(title: str, emoji: str, key: str) -> list[str]:
        stats = (by_plan_30 or {}).get(key, {}) if by_plan_30 else {}
        act = (act_by_plan_30 or {}).get(key, {}) if act_by_plan_30 else {}
        return [
            f"{emoji} {title}",
            f"• Evaluadas: {stats.get('total', 0)}",
            f"• Ganadas: {stats.get('won', 0)} | Perdidas: {stats.get('lost', 0)} | Expiradas: {stats.get('expired', 0)}",
            f"• Resueltas: {stats.get('resolved', stats.get('won', 0) + stats.get('lost', 0))} | Win rate: {stats.get('winrate', 0.0)}%",
            f"• Señales scanner: {act.get('signals_total', 0)} | Score prom (raw): {act.get('avg_score', '—')}",
            "",
        ]

    parts = ["🎯 **RENDIMIENTO DEL BOT**\n"]
    parts.append(_fmt_stats("Últimos 7 días", s7 or {}))
    parts.append(_fmt_stats("Últimos 30 días", s30 or {}))

    if diagnostics_30:
        parts.append("🧪 **Diagnóstico rápido (30D)**")
        parts.append(
            f"• Evaluadas: {diagnostics_30.get('evaluated_total', 0)} | Resueltas: {diagnostics_30.get('resolved_total', 0)} | Pendientes: {diagnostics_30.get('pending_to_evaluate', 0)}"
        )
        parts.append(
            f"• Loss rate resuelto: {diagnostics_30.get('loss_rate', 0.0)}% | Expiry rate total: {diagnostics_30.get('expiry_rate', 0.0)}%"
        )
        parts.append(
            f"• Score prom resultados (raw): {diagnostics_30.get('avg_result_score', '—')} | Señales scanner: {diagnostics_30.get('scanner_signals_total', 0)}"
        )
        parts.append("")

    parts.append("📊 **Rendimiento por plan (30D)**")
    parts.extend(_plan_stats_line("FREE", "🟢", "free"))
    parts.extend(_plan_stats_line("PLUS", "🟡", "plus"))
    parts.extend(_plan_stats_line("PREMIUM", "🔴", "premium"))

    if direction_30:
        parts.append("🧭 **Rendimiento por dirección (30D)**")
        for row in direction_30:
            direction = row.get("direction", "—")
            wr = row.get("winrate", 0.0)
            resolved = row.get("resolved", 0)
            lost = row.get("lost", 0)
            expired = row.get("expired", 0)
            parts.append(f"• {direction}: {wr}% ({resolved}) | Losses: {lost} | Expiradas: {expired}")
        parts.append("")

    if worst_symbols_30:
        parts.append("🚨 **Símbolos más débiles (30D)**")
        for row in worst_symbols_30:
            symbol = row.get("symbol", "—")
            wr = row.get("winrate", 0.0)
            resolved = row.get("resolved", 0)
            lost = row.get("lost", 0)
            expired = row.get("expired", 0)
            parts.append(f"• {symbol}: {wr}% ({resolved}) | Losses: {lost} | Exp: {expired}")
        parts.append("")

    if setup_groups_30:
        parts.append("🧩 **Rendimiento por setup group (30D)**")
        for row in setup_groups_30:
            group_name = str(row.get("setup_group", "—")).upper()
            wr = row.get("winrate", 0.0)
            resolved = row.get("resolved", 0)
            parts.append(f"• {group_name}: {wr}% ({resolved})")
        parts.append("")

    parts.append("📈 **Actividad de señales (scanner)**")
    if act7:
        parts.append(f"• 7D: {act7.get('signals_total', 0)} señales | Score prom (raw): {act7.get('avg_score', '—')}")
    if act30:
        parts.append(f"• 30D: {act30.get('signals_total', 0)} señales | Score prom (raw): {act30.get('avg_score', '—')}")

    if by_score_30:
        buckets = by_score_30.get("buckets", [])
        if buckets:
            parts.append("")
            parts.append("🏷️ **Win rate por raw score (30D)**")
            for row in buckets:
                label = row.get("label", "—")
                wr = row.get("winrate", 0.0)
                n = row.get("n", 0)
                parts.append(f"• {label}: {wr}% ({n})")

    if (s7 or {}).get("total", 0) == 0 and (act7 or {}).get("signals_total", 0) > 0:
        parts.append("")
        parts.append("ℹ️ Aún no hay resultados evaluados en la base de estadísticas. Las señales del scanner sí están registradas.")

    parts.append("")
    parts.append("⬅️ Usa *Volver* para regresar al menú.")

    buttons = []
    if admin:
        buttons.append([InlineKeyboardButton("♻️ Restablecer todo", callback_data="reset_stats")])
    buttons.append([InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")])

    await query.edit_message_text(
        "\n".join(parts),
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown",
    )


async def handle_reset_stats(query, user):
    if not is_admin(user.get("user_id")):
        await query.answer("No autorizado", show_alert=True)
        return

    try:
        summary = reset_statistics(preserve_signals=False) if callable(reset_statistics) else None

        if isinstance(summary, dict):
            deleted_results = summary.get("deleted_results", 0)
            deleted_base = summary.get("deleted_base_signals", 0)
            deleted_user = summary.get("deleted_user_signals", 0)
            message = (
                "♻️ Reinicio total ejecutado correctamente.\n\n"
                f"• Señales base borradas: {deleted_base}\n"
                f"• Señales usuario borradas: {deleted_user}\n"
                f"• Resultados borrados: {deleted_results}\n\n"
                "Se eliminó todo el histórico del módulo de señales y rendimiento."
            )
        else:
            message = (
                "♻️ Reinicio total ejecutado correctamente.\n\n"
                "Se eliminó todo el histórico del módulo de señales y rendimiento."
            )

        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎯 Ver rendimiento", callback_data="performance")],
                [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
            ]),
        )
    except Exception as e:
        logger.error(f"Error reseteando estadísticas: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ No pude restablecer las estadísticas.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎯 Ver rendimiento", callback_data="performance")],
                [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
            ]),
        )

async def handle_market(query, user):
    """Estado de mercado futures: sesgo, régimen, volatilidad y lectura operativa."""
    try:
        text, keyboard = render_market_state()
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    except BadRequest as e:
        if "Message is not modified" in str(e):
            await query.answer("✅ Actualizado", show_alert=False)
        else:
            raise
    except Exception as e:
        logger.exception("Error construyendo estado de mercado: %s", e)
        await query.edit_message_text(
            "⚠️ No pude cargar Mercado ahora mismo. Intenta de nuevo en unos segundos.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")]]),
        )


async def handle_movers(query, user):
    """Muestra Top Movers 24h de Binance USDT-M Futures."""
    # Movers es info pública: disponible para todos (incluye free)
    try:
        movers = get_top_movers_usdtm(limit=10)
    except Exception as e:
        logger.exception("Error obteniendo movers de Binance: %s", e)
        await query.edit_message_text(
            "⚠️ No pude obtener los movers ahora mismo. Intenta de nuevo en unos segundos.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")]]),
        )
        return

    if not movers:
        await query.edit_message_text(
            "⚠️ No hay datos disponibles ahora mismo. Intenta de nuevo.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")]]),
        )
        return

    lines = ["🔥 TOP MOVERS FUTURES (24h)", "", "*Binance USDT-M*", f"🕒 Actualizado: {datetime.utcnow():%H:%M:%S} UTC", ""]

    for i, m in enumerate(movers, start=1):
        symbol = str(m.get("symbol", ""))
        try:
            change = float(m.get("priceChangePercent", 0.0))
        except Exception:
            change = 0.0
        try:
            qv = float(m.get("quoteVolume", 0.0))
        except Exception:
            qv = 0.0
        try:
            last = float(m.get("lastPrice", 0.0))
        except Exception:
            last = 0.0

        sign = "+" if change >= 0 else ""
        lines.append(f"{i}. *{symbol}*  —  {sign}{change:.2f}%")
        if last > 0:
            lines.append(f"   Precio: `{last}`")
        if qv > 0:
            lines.append(f"   Volumen (USDT): `{qv:,.0f}`")
        lines.append("")

    text_out = "\n".join(lines).strip()

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Actualizar", callback_data="movers")],
        [InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")],
    ])

    await query.edit_message_text(text_out, reply_markup=keyboard, parse_mode="Markdown")


async def handle_radar(query, user, plan: str):
    # Radar es PLUS/PREMIUM
    # PLUS: radar básico
    # PREMIUM: radar con funding + open interest para top oportunidades
    try:
        opportunities = get_radar_opportunities(limit=8)
    except Exception as e:
        logging.exception("Radar error")
        text = "📡 RADAR FUTURES\n\n❌ No pude cargar el radar ahora mismo. Intenta de nuevo."
        # Si eres admin, muestra un código corto del error
        if user and is_admin(user.get('user_id', 0)):
            text += f"\n\n(Admin) Detalle: {type(e).__name__}"
        keyboard = [[InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")]]
        try:
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        except BadRequest as be:
            if "Message is not modified" in str(be):
                return
            raise
        return
        return

    if not opportunities:
        text = "📡 RADAR FUTURES\n\nNo pude cargar oportunidades ahora mismo. Intenta de nuevo."
        keyboard = [[InlineKeyboardButton("⬅️ Volver", callback_data="back_menu")]]
        try:
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        except BadRequest as be:
            if "Message is not modified" in str(be):
                return
            raise
        return

    lines = ["📡 RADAR FUTURES", "", "Top oportunidades (USDT-M):", ""]

    for idx, o in enumerate(opportunities, start=1):
        sym = o["symbol"]
        score = o["score"]
        change = o["change_pct"]
        vol = o["quote_volume"]
        trades = o["trades"]
        direction = o["direction"]

        # formato compacto y claro
        lines.append(f"{idx}️⃣ {sym} — {direction}")
        lines.append(f"Score: {score} | 24h: {change:+.2f}%")
        lines.append(f"Volumen 24h: ${vol:,.0f} | Trades: {trades:,.0f}")

        if plan == PLAN_PREMIUM:
            # Enriquecemos con 2 llamadas por símbolo (cached)
            try:
                pi = get_premium_index(sym)
                fr = float(pi.get("lastFundingRate", 0.0))
            except Exception:
                fr = 0.0
            try:
                oi = get_open_interest(sym)
                oi_val = float(oi.get("openInterest", 0.0))
            except Exception:
                oi_val = 0.0

            lines.append(f"Funding: {fr:+.4f} | Open Interest: {oi_val:,.0f}")

        lines.append("")

    # Botones
    keyboard = [
        [
            InlineKeyboardButton("🔄 Actualizar", callback_data="radar_refresh"),
            InlineKeyboardButton("⬅️ Volver", callback_data="back_menu"),
        ]
    ]
    try:
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(keyboard))
    except BadRequest as e:
        # Evita crash al presionar "Actualizar" si el contenido no cambia
        if "Message is not modified" in str(e):
            return
        raise
async def handle_admin_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_user_id_str = update.message.text.strip()

        if context.user_data.get("awaiting_delete_user_id"):
            try:
                target_user_id = int(target_user_id_str)
            except ValueError:
                await update.message.reply_text("❌ ID inválido.")
                context.user_data["awaiting_delete_user_id"] = False
                return

            if not is_admin(update.effective_user.id):
                await update.message.reply_text("❌ Permisos revocados.")
                context.user_data["awaiting_delete_user_id"] = False
                return

            users_col = users_collection()
            loop = asyncio.get_event_loop()

            await loop.run_in_executor(
                None,
                lambda: users_col.update_one({"user_id": target_user_id}, {"$set": {"banned": True}})
            )

            context.user_data["awaiting_delete_user_id"] = False
            await update.message.reply_text("🚫 Usuario eliminado para siempre.")
            return
        logger.info(f"[ADMIN] Recibido User ID: {target_user_id_str}")
        
        try:
            target_user_id = int(target_user_id_str)
        except ValueError:
            await update.message.reply_text("❌ ID inválido. Debe ser un número.")
            context.user_data["awaiting_user_id"] = False
            return

        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Permisos revocados.")
            context.user_data["awaiting_user_id"] = False
            return

        users_col = users_collection()
        loop = asyncio.get_event_loop()
        
        target_user = await loop.run_in_executor(
            None,
            lambda: users_col.find_one({"user_id": target_user_id})
        )
        
        if not target_user:
            await update.message.reply_text("❌ Usuario no encontrado en la base de datos.")
            context.user_data["awaiting_user_id"] = False
            return

        context.user_data["awaiting_user_id"] = False
        context.user_data["awaiting_plan_choice"] = True
        context.user_data["target_user_id"] = target_user_id

        keyboard = [
            [InlineKeyboardButton("🟡 Activar PLAN PLUS", callback_data="choose_plus_plan")],
            [InlineKeyboardButton("🔴 Activar PLAN PREMIUM", callback_data="choose_premium_plan")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="back_menu")]
        ]

        await update.message.reply_text(
            f"✅ Usuario encontrado: {target_user_id}\nSeleccione el plan a activar:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    except Exception as e:
        logger.error(f"[ADMIN] Error en handle_admin_text: {e}", exc_info=True)
        await update.message.reply_text("❌ Error procesando la solicitud.")
        context.user_data["awaiting_user_id"] = False

# ======================================================
# REGISTRO DE HANDLERS
# ======================================================

def get_handlers():
    return [
        CallbackQueryHandler(
            handle_menu,
            pattern=r"^(view_signals|radar|radar_refresh|performance|reset_stats|movers|market|market_refresh|watchlist|wl_refresh|wl_clear|wl_rm:[A-Z0-9]+|alerts|alerts_refresh|history|history_refresh|plans|my_account|referrals|support|admin_panel|admin_activate_plan|admin_delete_user|back_menu|choose_plus_plan|choose_premium_plan|register_exchange|risk_menu|risk_set_capital|risk_set_risk|risk_set_exchange|risk_set_fee|risk_set_slippage|risk_set_leverage|risk_set_profile|risk_pick_exchange:[a-z]+|risk_pick_profile:[a-z]+|risk_test|sig_detail:[A-Za-z0-9]+|hist_detail:[A-Za-z0-9]+|risk_calc:(live|hist|test):[A-Za-z0-9]+|sig_an:(live|hist|test):[A-Za-z0-9]+|sig_trk:(live|hist|test):[A-Za-z0-9]+|risk_pf:(live|hist|test):[A-Za-z0-9]+|risk_cp:(live|hist|test):[A-Za-z0-9]+:[cma])$"
        ),
        CallbackQueryHandler(handle_copy_ref_code, pattern="^copy_ref_code$"),
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_messages),
          ]
