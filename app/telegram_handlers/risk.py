from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from app.analysis_ui import build_signal_analysis_keyboard, build_signal_analysis_text
from app.database import users_collection
from app.plans import PLAN_FREE, PLAN_PLUS
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
    build_default_profile_selection_keyboard,
    build_default_profile_selection_text,
    build_exchange_selection_keyboard,
    build_exchange_selection_text,
    build_risk_management_keyboard,
    build_risk_management_text,
    build_risk_result_keyboard,
    build_risk_result_text,
    build_signal_detail_keyboard,
    build_signal_detail_text,
    build_signal_profile_picker_keyboard,
    build_signal_profile_picker_text,
)
from app.signals import (
    get_signal_analysis_for_user,
    get_signal_tracking_for_user,
    get_user_signal_by_signal_id,
)
from app.telegram_handlers.common import _get_user_language, _plan_rank, _tr
from app.tracking_ui import build_signal_tracking_keyboard, build_signal_tracking_text

logger = logging.getLogger(__name__)

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

async def _show_risk_management(query, user):
    profile = get_user_risk_profile(user["user_id"])
    plan = user.get("plan", PLAN_FREE)
    language = _get_user_language(user)
    await query.edit_message_text(
        build_risk_management_text(profile, plan=plan, language=language),
        reply_markup=build_risk_management_keyboard(plan=plan, language=language),
    )

async def _show_signal_detail(query, user, signal_id: str, source: str = "live"):
    language = _get_user_language(user)
    user_signal = get_user_signal_by_signal_id(user["user_id"], signal_id)
    if not user_signal:
        await query.edit_message_text(
            _tr(language, "❌ No pude encontrar esa señal en la base de datos.", "❌ I could not find that signal in the database."),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="history" if source == "hist" else "view_signals")]]),
        )
        return

    await query.edit_message_text(
        build_signal_detail_text(user_signal, source=source, language=language),
        reply_markup=build_signal_detail_keyboard(signal_id, source=source, plan=user.get("plan", PLAN_FREE), language=language),
    )

async def _show_signal_analysis(query, user, signal_id: str, source: str = "live"):
    language = _get_user_language(user)
    profile_name = _effective_risk_profile_name(user)
    analysis = get_signal_analysis_for_user(user["user_id"], signal_id, profile_name=profile_name)
    if not analysis:
        await query.edit_message_text(
            _tr(language, "❌ No pude cargar el análisis de esa señal.", "❌ I could not load the analysis for that signal."),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="history" if source == "hist" else "view_signals")]]),
        )
        return

    plan = user.get("plan", PLAN_FREE)
    await query.edit_message_text(
        build_signal_analysis_text(analysis, plan=plan, language=language),
        reply_markup=build_signal_analysis_keyboard(signal_id, source=source, plan=plan, language=language),
    )

async def _show_signal_tracking(query, user, signal_id: str, source: str = "live"):
    language = _get_user_language(user)
    profile_name = _effective_risk_profile_name(user)
    payload = get_signal_tracking_for_user(user["user_id"], signal_id, profile_name=profile_name)
    if not payload:
        await query.edit_message_text(
            _tr(language, "❌ No pude cargar el seguimiento de esa señal.", "❌ I could not load tracking for that signal."),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="history" if source == "hist" else "view_signals")]]),
        )
        return

    await query.edit_message_text(
        build_signal_tracking_text(payload, plan=user.get("plan", PLAN_FREE), language=language),
        reply_markup=build_signal_tracking_keyboard(signal_id, source=source, plan=user.get("plan", PLAN_FREE), language=language),
    )

async def _show_risk_result(query, user, signal_id: str, source: str = "live", profile_name: str | None = None):
    language = _get_user_language(user)
    user_signal = get_user_signal_by_signal_id(user["user_id"], signal_id)
    if not user_signal:
        await query.edit_message_text(
            _tr(language, "❌ No pude cargar la señal para calcular el riesgo.", "❌ I could not load the signal to calculate risk."),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data="history" if source == "hist" else "view_signals")]]),
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
            _tr(language,
                f"⚠️ No pude calcular el riesgo todavía.\n\n{exc}\n\nConfigura primero tu perfil de riesgo.",
                f"⚠️ I could not calculate risk yet.\n\n{exc}\n\nPlease configure your risk profile first.",
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(_tr(language, "⚙️ Gestión de riesgo", "⚙️ Risk settings"), callback_data="risk_menu")],
                [InlineKeyboardButton(_tr(language, "⬅️ Volver", "⬅️ Back"), callback_data=back_cb)],
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
            _tr(language,
                f"⚠️ No pude calcular el riesgo para esta señal.\n\n{exc}\n\nLa configuración del usuario sí está cargada, pero esta señal o el perfil elegido no son coherentes para el cálculo.",
                f"⚠️ I could not calculate risk for this signal.\n\n{exc}\n\nYour risk profile is loaded, but this signal or the selected profile is inconsistent for calculation.",
            ),
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    await query.edit_message_text(
        build_risk_result_text(calc, plan=plan, language=language),
        reply_markup=build_risk_result_keyboard(signal_id, source=source, plan=plan, language=language),
    )

async def _handle_risk_dynamic_callbacks(query, context, user, action: str) -> bool:
    user_id = user["user_id"]

    if action == "risk_menu":
        await _show_risk_management(query, user)
        return True

    if action == "risk_set_capital":
        context.user_data["awaiting_risk_field"] = "capital_usdt"
        await query.edit_message_text(
            _tr(_get_user_language(user), RISK_INPUT_FIELDS["capital_usdt"]["prompt"], "💰 Send your available capital in USDT.\nExample: 500"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(_get_user_language(user), "⬅️ Volver", "⬅️ Back"), callback_data="risk_menu")]]),
        )
        return True

    if action == "risk_set_risk":
        context.user_data["awaiting_risk_field"] = "risk_percent"
        await query.edit_message_text(
            _tr(_get_user_language(user), RISK_INPUT_FIELDS["risk_percent"]["prompt"], "🎯 Send the percentage you risk per trade.\nExample: 1 or 1.5"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(_get_user_language(user), "⬅️ Volver", "⬅️ Back"), callback_data="risk_menu")]]),
        )
        return True

    if action == "risk_set_fee":
        context.user_data["awaiting_risk_field"] = "fee_percent_per_side"
        await query.edit_message_text(
            _tr(_get_user_language(user), RISK_INPUT_FIELDS["fee_percent_per_side"]["prompt"], "💸 Send the fee per side in percentage.\nExample: 0.02"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(_get_user_language(user), "⬅️ Volver", "⬅️ Back"), callback_data="risk_menu")]]),
        )
        return True

    if action == "risk_set_slippage":
        context.user_data["awaiting_risk_field"] = "slippage_percent"
        await query.edit_message_text(
            _tr(_get_user_language(user), RISK_INPUT_FIELDS["slippage_percent"]["prompt"], "📉 Send the estimated slippage in percentage.\nExample: 0.03"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(_get_user_language(user), "⬅️ Volver", "⬅️ Back"), callback_data="risk_menu")]]),
        )
        return True

    if action == "risk_set_leverage":
        context.user_data["awaiting_risk_field"] = "default_leverage"
        await query.edit_message_text(
            _tr(_get_user_language(user), RISK_INPUT_FIELDS["default_leverage"]["prompt"], "📈 Send the default leverage.\nExample: 20 or 35"),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(_get_user_language(user), "⬅️ Volver", "⬅️ Back"), callback_data="risk_menu")]]),
        )
        return True

    if action == "risk_set_exchange":
        profile = get_user_risk_profile(user_id)
        await query.edit_message_text(
            build_exchange_selection_text(profile.get("exchange"), language=_get_user_language(user)),
            reply_markup=build_exchange_selection_keyboard(profile.get("exchange"), language=_get_user_language(user)),
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
            _tr(_get_user_language(user),
                "✅ Exchange actualizado. También recargué fee y slippage estimados.\n\n",
                "✅ Exchange updated. I also reloaded the estimated fee and slippage.\n\n",
            ) + build_risk_management_text(updated, plan=user.get("plan", PLAN_FREE), language=_get_user_language(user)),
            reply_markup=build_risk_management_keyboard(plan=user.get("plan", PLAN_FREE), language=_get_user_language(user)),
        )
        return True

    if action == "risk_set_profile":
        if _risk_feature_tier(user.get("plan", PLAN_FREE)) == "basic":
            await query.edit_message_text(
                _tr(_get_user_language(user),
                    "🔒 Perfil base avanzado\n\nEn FREE la calculadora usa el perfil Moderado.\n\nSube a PLUS o PREMIUM para elegir Conservador, Moderado o Agresivo.",
                    "🔒 Advanced default profile\n\nIn FREE, the calculator uses the Moderate profile.\n\nUpgrade to PLUS or PREMIUM to choose Conservative, Moderate, or Aggressive.",
                ),
                reply_markup=_build_risk_plan_upgrade_markup("risk_menu"),
            )
            return True

        profile = get_user_risk_profile(user_id)
        await query.edit_message_text(
            build_default_profile_selection_text(profile.get("default_profile"), language=_get_user_language(user)),
            reply_markup=build_default_profile_selection_keyboard(profile.get("default_profile"), language=_get_user_language(user)),
        )
        return True

    if action.startswith("risk_pick_profile:"):
        profile_name = action.split(":", 1)[1]
        save_user_risk_profile(user_id, {"default_profile": profile_name})
        updated = get_user_risk_profile(user_id)
        await query.edit_message_text(
            _tr(_get_user_language(user),
                "✅ Perfil base actualizado.\n\n",
                "✅ Default profile updated.\n\n",
            ) + build_risk_management_text(updated, plan=user.get("plan", PLAN_FREE), language=_get_user_language(user)),
            reply_markup=build_risk_management_keyboard(plan=user.get("plan", PLAN_FREE), language=_get_user_language(user)),
        )
        return True

    if action == "risk_test":
        recent = get_recent_user_signals_for_user(user_id, limit=1, active_only=False)
        if not recent:
            await query.edit_message_text(
                _tr(_get_user_language(user), "📭 No tienes señales todavía para probar la calculadora.", "📭 You do not have any signals yet to test the calculator."),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(_get_user_language(user), "⬅️ Volver", "⬅️ Back"), callback_data="risk_menu")]]),
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
            build_signal_profile_picker_text(current.get("default_profile"), source=source, plan=user.get("plan", PLAN_FREE), language=_get_user_language(user)),
            reply_markup=build_signal_profile_picker_keyboard(signal_id, source=source, selected_profile=current.get("default_profile"), plan=user.get("plan", PLAN_FREE), language=_get_user_language(user)),
        )
        return True

    if action.startswith("risk_cp:"):
        _, source, signal_id, profile_code = action.split(":", 3)
        if _risk_feature_tier(user.get("plan", PLAN_FREE)) == "basic":
            back_cb = f"sig_detail:{signal_id}" if source == "live" else (f"hist_detail:{signal_id}" if source == "hist" else "risk_menu")
            await query.edit_message_text(
                _tr(_get_user_language(user),
                    "🔒 Cambio de perfil\n\nEn FREE la calculadora usa el perfil Moderado.\n\nSube a PLUS o PREMIUM para elegir otros perfiles.",
                    "🔒 Profile change\n\nIn FREE, the calculator uses the Moderate profile.\n\nUpgrade to PLUS or PREMIUM to choose other profiles.",
                ),
                reply_markup=_build_risk_plan_upgrade_markup(back_cb),
            )
            return True
        profile_name = PROFILE_CODE_TO_NAME.get(profile_code)
        if not profile_name:
            await query.answer(_tr(_get_user_language(user), "Perfil inválido", "Invalid profile"), show_alert=False)
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
        lang = _get_user_language(users_collection().find_one({"user_id": user_id}) or {})
        prompts_en = {
            "capital_usdt": "💰 Send your available capital in USDT.\nExample: 500",
            "risk_percent": "🎯 Send the percentage you risk per trade.\nExample: 1 or 1.5",
            "fee_percent_per_side": "💸 Send the fee per side in percentage.\nExample: 0.02",
            "slippage_percent": "📉 Send the estimated slippage in percentage.\nExample: 0.03",
            "default_leverage": "📈 Send the default leverage.\nExample: 20 or 35",
        }
        labels_en = {
            "capital_usdt": "capital",
            "risk_percent": "risk per trade",
            "fee_percent_per_side": "fee per side",
            "slippage_percent": "slippage",
            "default_leverage": "default leverage",
        }
        await update.effective_message.reply_text(
            _tr(lang,
                f"❌ Valor inválido para {RISK_INPUT_FIELDS[field]['label']}.\n\n{RISK_INPUT_FIELDS[field]['prompt']}",
                f"❌ Invalid value for {labels_en.get(field, field)}.\n\n{prompts_en.get(field, '')}",
            ),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_tr(lang, "⬅️ Volver", "⬅️ Back"), callback_data="risk_menu")]]),
        )
        return

    context.user_data["awaiting_risk_field"] = None
    try:
        profile = save_user_risk_profile(user_id, {field: value})
    except RiskConfigurationError as exc:
        lang = _get_user_language(users_collection().find_one({"user_id": user_id}) or {})
        await update.effective_message.reply_text(_tr(lang, f"❌ {exc}", f"❌ {exc}"))
        return

    lang = _get_user_language(users_collection().find_one({"user_id": user_id}) or {})
    await update.effective_message.reply_text(
        _tr(lang, "✅ Ajuste guardado.\n\n", "✅ Setting saved.\n\n") + build_risk_management_text(profile, plan=(users_collection().find_one({"user_id": user_id}) or {}).get("plan", PLAN_FREE), language=lang),
        reply_markup=build_risk_management_keyboard(plan=(users_collection().find_one({"user_id": user_id}) or {}).get("plan", PLAN_FREE), language=lang),
    )

