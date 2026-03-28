import asyncio
import logging
from datetime import datetime
from functools import partial

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest

from app.binance_api import get_open_interest, get_premium_index, get_radar_opportunities, get_top_movers_usdtm
from app.config import get_admin_whatsapps, is_admin
from app.market_ui import render_market_state
from app.menus import back_to_menu, my_account_menu, main_menu
from app.models import is_plan_active, is_trial_active, update_timestamp
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM, activate_plus, activate_premium
from app.risk import get_user_risk_profile
from app.risk_ui import (
    build_active_signals_list_keyboard,
    build_active_signals_list_text,
    build_history_list_keyboard,
    build_history_list_text,
)
from app.signals import get_latest_base_signal_for_plan
from app.history_service import get_history_entries_for_user
from app.statistics import (
    get_last_days_stats,
    get_last_days_stats_by_plan,
    get_performance_snapshot,
    get_signal_activity_stats,
    get_signal_activity_stats_by_plan,
    get_winrate_by_score,
    reset_statistics,
)
from app.telegram_handlers.common import _get_user_language, _plan_rank, _tr, _wa_link, format_whatsapp_contacts

logger = logging.getLogger(__name__)

async def handle_view_signals(query, user, admin, users_col):
    language = _get_user_language(user)
    try:
        user_id = user["user_id"]
        plan = PLAN_PREMIUM if admin else user.get("plan", PLAN_FREE)

        if not admin and not (is_plan_active(user) or is_trial_active(user)):
            await query.edit_message_text(
                _tr(language, "⛔ Acceso expirado.", "⛔ Access expired."),
                reply_markup=back_to_menu(language=language),
            )
            return

        user_signals = get_latest_base_signal_for_plan(user_id, plan)
        if not user_signals:
            await query.edit_message_text(
                _tr(language, "📭 No hay señales activas disponibles.", "📭 There are no active signals available."),
                reply_markup=back_to_menu(language=language),
            )
            return

        await query.edit_message_text(
            build_active_signals_list_text(user_signals, language=language),
            reply_markup=build_active_signals_list_keyboard(user_signals, language=language),
        )

        users_col.update_one({"user_id": user_id}, {"$set": update_timestamp(user)})

    except Exception as e:
        logger.error(f"Error en handle_view_signals: {e}", exc_info=True)
        await query.edit_message_text(
            _tr(language, "❌ Error al obtener señales.", "❌ Error while loading signals."),
            reply_markup=back_to_menu(language=language),
        )

async def handle_plans(query, user):
    language = _get_user_language(user)
    plan = str(user.get("plan", PLAN_FREE)).upper()
    user_id = user.get("user_id")
    whatsapps = get_admin_whatsapps()

    if language == "en":
        message = (
            "💼 HADES ALPHA PLANS\n\n"
            f"📊 Your current plan: {plan}\n\n"
            "🟢 FREE\n• Basic access to the bot\n• Limited features\n\n"
            "🟡 PLUS\n• Full signals\n• Futures Radar\n• Movers\n• PRO Watchlist\n• History\n\n"
            "🔴 PREMIUM\n• Everything in PLUS\n• 🔔 Premium alerts\n• Opportunities before the signal\n• Full ecosystem access\n\n"
            "👇 Select the plan you want to activate:"
        )
        plus_msg = f"Hello, I want to activate the PLUS plan of HADES ALPHA. My Telegram ID is: {user_id}"
        premium_msg = f"Hello, I want to activate the PREMIUM plan of HADES ALPHA. My Telegram ID is: {user_id}"
    else:
        message = (
            "💼 PLANES HADES ALPHA\n\n"
            f"📊 Tu plan actual: {plan}\n\n"
            "🟢 FREE\n• Acceso básico al bot\n• Funciones limitadas\n\n"
            "🟡 PLUS\n• Señales completas\n• Radar Futures\n• Movers\n• Watchlist PRO\n• Historial\n\n"
            "🔴 PREMIUM\n• Todo lo de PLUS\n• 🔔 Alertas premium\n• Oportunidades antes de la señal\n• Acceso completo al ecosistema\n\n"
            "👇 Selecciona el plan que deseas activar:"
        )
        plus_msg = f"Hola, quiero activar el plan PLUS de HADES ALPHA. Mi ID de Telegram es: {user_id}"
        premium_msg = f"Hola, quiero activar el plan PREMIUM de HADES ALPHA. Mi ID de Telegram es: {user_id}"

    keyboard_rows = []
    if len(whatsapps) >= 1:
        keyboard_rows.append([InlineKeyboardButton("💬 PLUS · Admin 1", url=_wa_link(whatsapps[0], plus_msg))])
        keyboard_rows.append([InlineKeyboardButton("💬 PREMIUM · Admin 1", url=_wa_link(whatsapps[0], premium_msg))])
    if len(whatsapps) >= 2:
        keyboard_rows.append([InlineKeyboardButton("💬 PLUS · Admin 2", url=_wa_link(whatsapps[1], plus_msg))])
        keyboard_rows.append([InlineKeyboardButton("💬 PREMIUM · Admin 2", url=_wa_link(whatsapps[1], premium_msg))])
    if not keyboard_rows:
        message += _tr(language, "\n\n⚠️ No hay contactos de WhatsApp configurados todavía.", "\n\n⚠️ No WhatsApp contacts are configured yet.")
    keyboard_rows.append([InlineKeyboardButton(_tr(language, "⬅️ Volver al menú", "⬅️ Back to menu"), callback_data="back_menu")])
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard_rows))

async def handle_my_account(query, user, admin=False):
    language = _get_user_language(user)
    now = datetime.utcnow()
    plan = user.get("plan", PLAN_FREE)
    plan_end = user.get("plan_end")
    trial_end = user.get("trial_end")
    days_left = None
    expires_str = "—"
    if plan_end:
        try:
            delta = plan_end - now; days_left = max(delta.days, 0); expires_str = plan_end.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass
    elif trial_end:
        try:
            delta = trial_end - now; days_left = max(delta.days, 0); expires_str = trial_end.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass
    risk_profile = get_user_risk_profile(user["user_id"])
    if language == "en":
        message = f"👤 MY ACCOUNT\n\nID: {user['user_id']}\nPlan: {str(plan).upper()}\n"
        if days_left is not None:
            message += f"📅 Days remaining: {days_left}\n⏳ Expires: {expires_str}\n"
        message += "\nRisk configuration:\n"
        message += f"• Capital: {risk_profile.get('capital_usdt', 0):,.2f} USDT\n"
        message += f"• Risk/trade: {float(risk_profile.get('risk_percent') or 0):.2f}%\n"
        message += f"• Default profile: {risk_profile.get('default_profile', 'moderado').title()}\n"
        message += f"• Exchange: {str(risk_profile.get('exchange') or '—').upper()}\n"
    else:
        message = f"👤 MI CUENTA\n\nID: {user['user_id']}\nPlan: {str(plan).upper()}\n"
        if days_left is not None:
            message += f"📅 Días restantes: {days_left}\n⏳ Expira: {expires_str}\n"
        message += "\nConfiguración de riesgo:\n"
        message += f"• Capital: {risk_profile.get('capital_usdt', 0):,.2f} USDT\n"
        message += f"• Riesgo/trade: {float(risk_profile.get('risk_percent') or 0):.2f}%\n"
        message += f"• Perfil base: {risk_profile.get('default_profile', 'moderado').title()}\n"
        message += f"• Exchange: {str(risk_profile.get('exchange') or '—').upper()}\n"
    await query.edit_message_text(text=message, reply_markup=my_account_menu(language=language))

async def handle_history(query, user):
    language = _get_user_language(user)
    plan = user.get("plan", PLAN_FREE)
    if _plan_rank(plan) < _plan_rank(PLAN_PLUS):
        await query.edit_message_text(
            _tr(language, "🔒 🧾 Historial\n\nDisponible para *PLUS* y *PREMIUM*.", "🔒 🧾 History\n\nAvailable for *PLUS* and *PREMIUM*."),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "💼 Planes", "💼 Plans"), callback_data="plans")],
                [InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    try:
        docs = get_history_entries_for_user(user["user_id"], user_plan=plan, limit=10)

        if not docs:
            try:
                await query.edit_message_text(
                    _tr(language, "🧾 HISTORIAL\n\nAún no tienes señales registradas en tu historial.", "🧾 HISTORY\n\nYou do not have any signals in your history yet."),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(_tr(language, "🔄 Actualizar", "🔄 Refresh"), callback_data="history_refresh")],
                        [InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")],
                    ]),
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    await query.answer(_tr(language, "✅ Actualizado", "✅ Updated"), show_alert=False)
                else:
                    raise
            return

        try:
            await query.edit_message_text(
                build_history_list_text(docs, language=language),
                reply_markup=build_history_list_keyboard(docs, language=language),
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                await query.answer(_tr(language, "✅ Actualizado", "✅ Updated"), show_alert=False)
            else:
                raise

    except Exception as e:
        logger.error(f"Error en handle_history: {e}", exc_info=True)
        await query.edit_message_text(
            _tr(language, "❌ No pude cargar tu historial ahora mismo.", "❌ I could not load your history right now."),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]),
        )

async def handle_alerts(query, user):
    language = _get_user_language(user)
    plan = user.get("plan", PLAN_FREE)

    if _plan_rank(plan) < _plan_rank(PLAN_PREMIUM):
        await query.edit_message_text(
            _tr(language, "🔒 ALERTAS PREMIUM\n\nDisponible solo para *PREMIUM*.", "🔒 PREMIUM ALERTS\n\nAvailable only for *PREMIUM*."),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "💼 Planes", "💼 Plans"), callback_data="plans")],
                [InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")],
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
                return _tr(language, "Muy alto", "Very high")
            if change >= 3:
                return _tr(language, "Alto", "High")
            if change >= 1.5:
                return _tr(language, "Medio", "Medium")
            return _tr(language, "Bajo", "Low")
        except Exception:
            pass
        try:
            score_val = float(op.get("score", 0))
            if score_val >= 95:
                return _tr(language, "Muy alto", "Very high")
            if score_val >= 85:
                return _tr(language, "Alto", "High")
            if score_val >= 75:
                return _tr(language, "Medio", "Medium")
            return _tr(language, "Bajo", "Low")
        except Exception:
            return _tr(language, "Medio", "Medium")

    try:
        opportunities = get_radar_opportunities()
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(_tr(language, "🔄 Actualizar", "🔄 Refresh"), callback_data="alerts_refresh")],
            [InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")],
        ])
        if not opportunities:
            try:
                await query.edit_message_text(
                    _tr(language, "🔔 ALERTAS\n\nNo hay oportunidades detectadas ahora mismo.", "🔔 ALERTS\n\nNo opportunities detected right now."),
                    reply_markup=keyboard,
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    await query.answer(_tr(language, "✅ Actualizado", "✅ Updated"), show_alert=False)
                else:
                    raise
            return
        lines = [_tr(language, "🔔 ALERTAS PREMIUM", "🔔 PREMIUM ALERTS"), ""]
        for i, o in enumerate(opportunities[:5], 1):
            symbol = o.get("symbol", "—")
            score = o.get("score", "—")
            momentum = _momentum_label(o)
            lines.append(f"{i}. {symbol}")
            lines.append(f"   Score: {score} | Momentum: {momentum}")
            lines.append("")
        try:
            await query.edit_message_text("\n".join(lines).strip(), reply_markup=keyboard)
        except BadRequest as e:
            if "Message is not modified" in str(e):
                await query.answer(_tr(language, "✅ Actualizado", "✅ Updated"), show_alert=False)
            else:
                raise
    except Exception as e:
        logger.error(f"Error en alerts: {e}", exc_info=True)
        await query.edit_message_text(
            _tr(language, "❌ No pude cargar alertas ahora mismo.", "❌ I could not load alerts right now."),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]),
        )

async def handle_support(query, user=None):
    language = _get_user_language(user)
    await query.edit_message_text(
        f"{_tr(language, '📩 SOPORTE', '📩 SUPPORT')}\n\n{format_whatsapp_contacts()}",
        reply_markup=back_to_menu(language=language),
    )

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
    language = _get_user_language(user)
    plan = user.get("plan", PLAN_FREE)
    admin = is_admin(user.get("user_id"))

    if _plan_rank(plan) < _plan_rank(PLAN_PLUS):
        await query.edit_message_text(
            _tr(language, "🔒 🎯 Rendimiento\n\nDisponible para *PLUS* y *PREMIUM*.\n\nActiva tu plan para ver estadísticas reales del bot.", "🔒 🎯 Performance\n\nAvailable for *PLUS* and *PREMIUM*.\n\nActivate your plan to view real bot statistics."),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "💼 Planes", "💼 Plans"), callback_data="plans")],
                [InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    snapshot = None
    try:
        if callable(get_performance_snapshot):
            snapshot = get_performance_snapshot(short_days=7, long_days=30, worst_symbols_limit=3, worst_symbols_min_resolved=3)
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
            s7 = get_weekly_stats(); s30 = get_monthly_stats()
        if callable(get_last_days_stats_by_plan):
            try: by_plan_30 = get_last_days_stats_by_plan(30)
            except Exception: by_plan_30 = None
        if callable(get_signal_activity_stats):
            act7 = get_signal_activity_stats(7); act30 = get_signal_activity_stats(30)
        if callable(get_signal_activity_stats_by_plan):
            try: act_by_plan_30 = get_signal_activity_stats_by_plan(30)
            except Exception: act_by_plan_30 = None
        if callable(get_winrate_by_score):
            try: by_score_30 = get_winrate_by_score(30)
            except Exception: by_score_30 = None

    def _fmt_ratio(value):
        if value == float("inf"):
            return "∞"
        return value

    def _fmt_stats(label_es, label_en, s):
        total=s.get('total',0); won=s.get('won',0); lost=s.get('lost',0); expired=s.get('expired',0); resolved=s.get('resolved', won+lost); winrate=s.get('winrate',0.0)
        label = label_en if language == 'en' else label_es
        return (
            f"**{label}**\n"
            f"• {_tr(language,'Evaluadas','Evaluated')}: {total}\n"
            f"• {_tr(language,'Ganadas','Won')}: {won} | {_tr(language,'Perdidas','Lost')}: {lost} | {_tr(language,'Expiradas','Expired')}: {expired}\n"
            f"• {_tr(language,'Resueltas','Resolved')}: {resolved} | Win rate: {winrate}%\n"
            f"• PF: {_fmt_ratio(s.get('profit_factor',0.0))} | Exp R: {s.get('expectancy_r',0.0)} | DD R: {s.get('max_drawdown_r',0.0)}\n"
        )

    def _plan_stats_line(title, emoji, key):
        stats=(by_plan_30 or {}).get(key,{}) if by_plan_30 else {}
        act=(act_by_plan_30 or {}).get(key,{}) if act_by_plan_30 else {}
        return [f"{emoji} {title}", f"• {_tr(language,'Evaluadas','Evaluated')}: {stats.get('total',0)}", f"• {_tr(language,'Ganadas','Won')}: {stats.get('won',0)} | {_tr(language,'Perdidas','Lost')}: {stats.get('lost',0)} | {_tr(language,'Expiradas','Expired')}: {stats.get('expired',0)}", f"• {_tr(language,'Resueltas','Resolved')}: {stats.get('resolved', stats.get('won',0)+stats.get('lost',0))} | Win rate: {stats.get('winrate',0.0)}%", f"• {_tr(language,'Señales scanner','Scanner signals')}: {act.get('signals_total',0)} | {_tr(language,'Score prom (raw)','Avg score (raw)')}: {act.get('avg_score','—')}", ""]

    parts=[f"🎯 **{_tr(language,'RENDIMIENTO DEL BOT','BOT PERFORMANCE')}**\n"]
    parts.append(_fmt_stats('Últimos 7 días','Last 7 days', s7 or {}))
    parts.append(_fmt_stats('Últimos 30 días','Last 30 days', s30 or {}))
    if diagnostics_30:
        parts.append(f"🧪 **{_tr(language,'Diagnóstico rápido (30D)','Quick diagnostics (30D)')}**")
        parts.append(f"• {_tr(language,'Evaluadas','Evaluated')}: {diagnostics_30.get('evaluated_total',0)} | {_tr(language,'Resueltas','Resolved')}: {diagnostics_30.get('resolved_total',0)} | {_tr(language,'Pendientes','Pending')}: {diagnostics_30.get('pending_to_evaluate',0)}")
        parts.append(f"• {_tr(language,'Loss rate resuelto','Resolved loss rate')}: {diagnostics_30.get('loss_rate',0.0)}% | {_tr(language,'Expiry rate total','Total expiry rate')}: {diagnostics_30.get('expiry_rate',0.0)}%")
        parts.append(f"• PF: {_fmt_ratio(diagnostics_30.get('profit_factor',0.0))} | Exp R: {diagnostics_30.get('expectancy_r',0.0)} | DD R: {diagnostics_30.get('max_drawdown_r',0.0)}")
        parts.append(f"• {_tr(language,'Tiempo prom resolución (min)','Avg resolution time (min)')}: {diagnostics_30.get('avg_resolution_minutes','—')} | {_tr(language,'Score prom resultados (raw)','Avg result score (raw)')}: {diagnostics_30.get('avg_result_score','—')}")
        parts.append(f"• {_tr(language,'Señales scanner','Scanner signals')}: {diagnostics_30.get('scanner_signals_total',0)}")
        parts.append("")
    parts.append(f"📊 **{_tr(language,'Rendimiento por plan (30D)','Performance by plan (30D)')}**")
    parts.extend(_plan_stats_line('FREE','🟢','free'))
    parts.extend(_plan_stats_line('PLUS','🟡','plus'))
    parts.extend(_plan_stats_line('PREMIUM','🔴','premium'))
    if direction_30:
        parts.append(f"🧭 **{_tr(language,'Rendimiento por dirección (30D)','Performance by direction (30D)')}**")
        for row in direction_30:
            parts.append(f"• {row.get('direction','—')}: {row.get('winrate',0.0)}% ({row.get('resolved',0)}) | {_tr(language,'Losses','Losses')}: {row.get('lost',0)} | {_tr(language,'Expiradas','Expired')}: {row.get('expired',0)}")
        parts.append("")
    if worst_symbols_30:
        parts.append(f"🚨 **{_tr(language,'Símbolos más débiles (30D)','Weakest symbols (30D)')}**")
        for row in worst_symbols_30:
            parts.append(f"• {row.get('symbol','—')}: {row.get('winrate',0.0)}% ({row.get('resolved',0)}) | {_tr(language,'Losses','Losses')}: {row.get('lost',0)} | Exp: {row.get('expired',0)}")
        parts.append("")
    if setup_groups_30:
        parts.append(f"🧩 **{_tr(language,'Rendimiento por setup group (30D)','Performance by setup group (30D)')}**")
        for row in setup_groups_30:
            parts.append(f"• {str(row.get('setup_group','—')).upper()}: {row.get('winrate',0.0)}% ({row.get('resolved',0)})")
        parts.append("")
    parts.append(f"📈 **{_tr(language,'Actividad de señales (scanner)','Signal activity (scanner)')}**")
    if act7: parts.append(f"• 7D: {act7.get('signals_total',0)} {_tr(language,'señales','signals')} | {_tr(language,'Score prom (raw)','Avg score (raw)')}: {act7.get('avg_score','—')}")
    if act30: parts.append(f"• 30D: {act30.get('signals_total',0)} {_tr(language,'señales','signals')} | {_tr(language,'Score prom (raw)','Avg score (raw)')}: {act30.get('avg_score','—')}")
    if by_score_30 and by_score_30.get('buckets',[]):
        parts.append("")
        parts.append(f"🏷️ **{_tr(language,'Win rate por raw score (30D)','Win rate by raw score (30D)')}**")
        for row in by_score_30.get('buckets',[]): parts.append(f"• {row.get('label','—')}: {row.get('winrate',0.0)}% ({row.get('n',0)})")
    if (s7 or {}).get('total',0)==0 and (act7 or {}).get('signals_total',0)>0:
        parts.append("")
        parts.append(_tr(language, 'ℹ️ Aún no hay resultados evaluados en la base de estadísticas. Las señales del scanner sí están registradas.', 'ℹ️ There are no evaluated results in the statistics database yet. Scanner signals are registered.'))
    parts.append("")
    parts.append(_tr(language, '⬅️ Usa *Volver* para regresar al menú.', '⬅️ Use *Back* to return to the menu.'))

    buttons=[]
    if admin: buttons.append([InlineKeyboardButton(_tr(language,'♻️ Restablecer todo','♻️ Reset all'), callback_data='reset_stats')])
    buttons.append([InlineKeyboardButton(_tr(language,'⬅️ Volver','⬅️ Back'), callback_data='back_menu')])
    await query.edit_message_text("\n".join(parts), reply_markup=InlineKeyboardMarkup(buttons), parse_mode='Markdown')

async def handle_reset_stats(query, user, confirmed: bool = False):
    language = _get_user_language(user)

    if not is_admin(user.get("user_id")):
        await query.answer(_tr(language, "No autorizado", "Unauthorized"), show_alert=True)
        return

    if not confirmed:
        await query.edit_message_text(
            _tr(
                language,
                "⚠️ Esta acción borrará todo el histórico de señales, señales por usuario y resultados.\n\n¿Confirmas el reinicio total?",
                "⚠️ This action will delete the full history of base signals, user signals, and results.\n\nDo you confirm the full reset?",
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "✅ Confirmar reinicio", "✅ Confirm reset"), callback_data="confirm_reset_stats")],
                [InlineKeyboardButton(_tr(language, "❌ Cancelar", "❌ Cancel"), callback_data="cancel_reset_stats")],
            ]),
        )
        return

    try:
        summary = reset_statistics(preserve_signals=False) if callable(reset_statistics) else None
        logger.warning("[ADMIN] Reset total de estadísticas ejecutado | admin=%s summary=%s", user.get("user_id"), summary)

        if isinstance(summary, dict):
            deleted_results = summary.get("deleted_results", 0)
            deleted_base = summary.get("deleted_base_signals", 0)
            deleted_user = summary.get("deleted_user_signals", 0)
            message = _tr(
                language,
                "♻️ Reinicio total ejecutado correctamente.\n\n"
                f"• Señales base borradas: {deleted_base}\n"
                f"• Señales usuario borradas: {deleted_user}\n"
                f"• Resultados borrados: {deleted_results}\n\n"
                "Se eliminó todo el histórico del módulo de señales y rendimiento.",
                "♻️ Full reset executed successfully.\n\n"
                f"• Base signals deleted: {deleted_base}\n"
                f"• User signals deleted: {deleted_user}\n"
                f"• Results deleted: {deleted_results}\n\n"
                "All history for the signals and performance module was deleted.",
            )
        else:
            message = _tr(
                language,
                "♻️ Reinicio total ejecutado correctamente.\n\nSe eliminó todo el histórico del módulo de señales y rendimiento.",
                "♻️ Full reset executed successfully.\n\nAll history for the signals and performance module was deleted.",
            )

        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "🎯 Ver rendimiento", "🎯 View performance"), callback_data="performance")],
                [InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")],
            ]),
        )
    except Exception as e:
        logger.error(f"Error reseteando estadísticas: {e}", exc_info=True)
        await query.edit_message_text(
            _tr(language, "❌ No pude restablecer las estadísticas.", "❌ I could not reset the statistics."),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "🎯 Ver rendimiento", "🎯 View performance"), callback_data="performance")],
                [InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")],
            ]),
        )

async def handle_market(query, user):
    """Estado de mercado futures: sesgo, régimen, volatilidad y lectura operativa."""
    language = _get_user_language(user)
    try:
        text, keyboard = render_market_state(plan=(user.get("plan") or PLAN_FREE), language=language)
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    except BadRequest as e:
        if "Message is not modified" in str(e):
            await query.answer(_tr(language, "✅ Actualizado", "✅ Updated"), show_alert=False)
        else:
            raise
    except Exception as e:
        logger.exception("Error construyendo estado de mercado: %s", e)
        await query.edit_message_text(_tr(language, "⚠️ No pude cargar Mercado ahora mismo. Intenta de nuevo en unos segundos.", "⚠️ I could not load Market right now. Try again in a few seconds."), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]))

async def handle_movers(query, user):
    """Muestra Top Movers 24h de Binance USDT-M Futures."""
    language = _get_user_language(user)
    try:
        movers = get_top_movers_usdtm(limit=10)
    except Exception as e:
        logger.exception("Error obteniendo movers de Binance: %s", e)
        await query.edit_message_text(_tr(language, "⚠️ No pude obtener los movers ahora mismo. Intenta de nuevo en unos segundos.", "⚠️ I could not get movers right now. Try again in a few seconds."), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]))
        return
    if not movers:
        await query.edit_message_text(_tr(language, "⚠️ No hay datos disponibles ahora mismo. Intenta de nuevo.", "⚠️ No data available right now. Try again."), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]))
        return
    lines_out = [_tr(language, "🔥 TOP MOVERS FUTURES (24h)", "🔥 TOP FUTURES MOVERS (24h)"), "", "*Binance USDT-M*", f"🕒 {_tr(language, 'Actualizado', 'Updated')}: {datetime.utcnow():%H:%M:%S} UTC", ""]
    for i, m in enumerate(movers, start=1):
        symbol = str(m.get("symbol", ""))
        try: change = float(m.get("priceChangePercent", 0.0))
        except Exception: change = 0.0
        try: qv = float(m.get("quoteVolume", 0.0))
        except Exception: qv = 0.0
        try: last = float(m.get("lastPrice", 0.0))
        except Exception: last = 0.0
        sign = "+" if change >= 0 else ""
        lines_out.append(f"{i}. *{symbol}*  —  {sign}{change:.2f}%")
        if last > 0:
            lines_out.append(_tr(language, f"   Precio: `{last}`", f"   Price: `{last}`"))
        if qv > 0:
            lines_out.append(_tr(language, f"   Volumen (USDT): `{qv:,.0f}`", f"   Volume (USDT): `{qv:,.0f}`"))
        lines_out.append("")
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "🔄 Actualizar", "🔄 Refresh"), callback_data="movers")],[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]])
    await query.edit_message_text("\n".join(lines_out).strip(), reply_markup=keyboard, parse_mode="Markdown")

async def handle_radar(query, user, plan: str):
    language = _get_user_language(user)
    try:
        opportunities = get_radar_opportunities(limit=8)
    except Exception as e:
        logging.exception("Radar error")
        text = _tr(language, "📡 RADAR FUTURES\n\n❌ No pude cargar el radar ahora mismo. Intenta de nuevo.", "📡 FUTURES RADAR\n\n❌ I could not load radar right now. Try again.")
        if user and is_admin(user.get('user_id', 0)):
            text += _tr(language, f"\n\n(Admin) Detalle: {type(e).__name__}", f"\n\n(Admin) Detail: {type(e).__name__}")
        keyboard = [[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    if not opportunities:
        text = _tr(language, "📡 RADAR FUTURES\n\nNo pude cargar oportunidades ahora mismo. Intenta de nuevo.", "📡 FUTURES RADAR\n\nI could not load opportunities right now. Try again.")
        keyboard = [[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    lines_out = [_tr(language, "📡 RADAR FUTURES", "📡 FUTURES RADAR"), "", _tr(language, "Top oportunidades (USDT-M):", "Top opportunities (USDT-M):"), ""]
    for idx, o in enumerate(opportunities, start=1):
        sym = o["symbol"]; score = o["score"]; change = o["change_pct"]; vol = o["quote_volume"]; trades = o["trades"]; direction = o["direction"]
        lines_out.append(f"{idx}️⃣ {sym} — {direction}")
        lines_out.append(f"Score: {score} | 24h: {change:+.2f}%")
        lines_out.append(_tr(language, f"Volumen 24h: ${vol:,.0f} | Trades: {trades:,.0f}", f"24h Volume: ${vol:,.0f} | Trades: {trades:,.0f}"))
        if plan == PLAN_PREMIUM:
            try:
                fr = float(get_premium_index(sym).get("lastFundingRate", 0.0))
            except Exception:
                fr = 0.0
            try:
                oi_val = float(get_open_interest(sym).get("openInterest", 0.0))
            except Exception:
                oi_val = 0.0
            lines_out.append(f"Funding: {fr:+.4f} | Open Interest: {oi_val:,.0f}")
        lines_out.append("")
    keyboard = [[InlineKeyboardButton(_tr(language, "🔄 Actualizar", "🔄 Refresh"), callback_data="radar_refresh"), InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="back_menu")]]
    try:
        await query.edit_message_text("\n".join(lines_out), reply_markup=InlineKeyboardMarkup(keyboard))
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise


async def handle_standard_menu_action(query, context, user, action: str, admin: bool) -> bool:
    user_id = user["user_id"]
    users_col = None

    if action == "view_signals":
        from app.database import users_collection
        users_col = users_collection()
        await handle_view_signals(query, user, admin, users_col)
        return True

    if action == "plans":
        await handle_plans(query, user)
        return True

    if action == "my_account":
        await handle_my_account(query, user, admin)
        return True

    if action == "performance":
        await handle_performance(query, user)
        return True

    if action == "reset_stats":
        await handle_reset_stats(query, user, confirmed=False)
        return True

    if action == "confirm_reset_stats":
        await handle_reset_stats(query, user, confirmed=True)
        return True

    if action == "cancel_reset_stats":
        await handle_performance(query, user)
        return True

    if action in {"radar", "radar_refresh"}:
        await handle_radar(query, user, user.get("plan") or PLAN_FREE)
        return True

    if action == "movers":
        await handle_movers(query, user)
        return True

    if action in {"market", "market_refresh"}:
        await handle_market(query, user)
        return True

    if action in {"alerts", "alerts_refresh"}:
        await handle_alerts(query, user)
        return True

    if action in {"history", "history_refresh"}:
        await handle_history(query, user)
        return True

    if action == "support":
        await handle_support(query, user)
        return True

    if action == "register_exchange":
        context.user_data["awaiting_exchange"] = True
        await query.edit_message_text(
            _tr(_get_user_language(user), "🌐 Envía el nombre de tu exchange (ej: Binance, CoinEx, KuCoin):", "🌐 Send the name of your exchange (e.g. Binance, CoinEx, KuCoin):")
        )
        return True

    if action == "back_menu":
        context.user_data["watchlist_active"] = False
        await query.edit_message_text(
            _tr(_get_user_language(user), "🏠 MENÚ PRINCIPAL — Selecciona una opción abajo", "🏠 MAIN MENU — Select an option below"),
            reply_markup=main_menu(language=_get_user_language(user), is_admin=admin),
        )
        return True

    if action in ["choose_plus_plan", "choose_premium_plan"]:
        target_user_id = context.user_data.get("target_user_id")
        if target_user_id:
            loop = asyncio.get_event_loop()
            if action == "choose_plus_plan":
                success = await loop.run_in_executor(None, partial(activate_plus, target_user_id))
                plan_name = "PLUS"
            else:
                success = await loop.run_in_executor(None, partial(activate_premium, target_user_id))
                plan_name = "PREMIUM"

            if success:
                await query.edit_message_text(f"✅ Plan {plan_name} activado correctamente por 30 días.")
            else:
                await query.edit_message_text(f"❌ No se pudo activar el plan {plan_name}.")

            context.user_data.pop("awaiting_plan_choice", None)
            context.user_data.pop("awaiting_plan_days", None)
            context.user_data.pop("custom_plan_type", None)
            context.user_data.pop("target_user_id", None)
        return True

    if action in ["choose_plus_plan_days", "choose_premium_plan_days"]:
        target_user_id = context.user_data.get("target_user_id")
        if not target_user_id:
            await query.edit_message_text("❌ No hay un usuario seleccionado para activar plan por días.")
            return True

        selected_plan = PLAN_PLUS if action == "choose_plus_plan_days" else PLAN_PREMIUM
        plan_name = "PLUS" if selected_plan == PLAN_PLUS else "PREMIUM"
        context.user_data["awaiting_plan_choice"] = False
        context.user_data["awaiting_plan_days"] = True
        context.user_data["custom_plan_type"] = selected_plan

        await query.edit_message_text(
            f"📅 Activación manual de PLAN {plan_name}\n\n"
            f"Envía la cantidad de días para el usuario {target_user_id}.\n"
            "Ejemplo: 7, 15, 30"
        )
        return True

    return False
