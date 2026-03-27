
from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.plans import PLAN_FREE
from app.risk import ENTRY_MODE_LABELS, get_risk_profile_label


PROFILE_NAME_TO_CODE = {
    "conservador": "c",
    "moderado": "m",
    "agresivo": "a",
}
PROFILE_CODE_TO_NAME = {value: key for key, value in PROFILE_NAME_TO_CODE.items()}

EXCHANGE_LABELS = {
    "binance": "Binance",
    "lbank": "LBank",
    "coinw": "CoinW",
    "weex": "WEEX",
    "coinex": "CoinEx",
    "bitunix": "Bitunix",
    "mexc": "MEXC",
    "other": "Otro",
}

ENTRY_MODE_LABELS_EN = {
    "limit_wait": "Limit waiting for price",
    "limit_fast": "Aggressive limit to enter fast",
    "limit_unknown": "Limit / not sure",
}

def _lang(language: str | None) -> str:
    return "en" if str(language or "es").lower().startswith("en") else "es"

def _t(language: str | None, es: str, en: str) -> str:
    return en if _lang(language) == "en" else es

def _profile_label(profile_name: str | None, language: str | None = "es") -> str:
    profile_name = str(profile_name or "moderado").lower()
    if _lang(language) == "en":
        mapping = {
            "conservador": "Conservative",
            "moderado": "Moderate",
            "agresivo": "Aggressive",
        }
        return mapping.get(profile_name, profile_name.title())
    return get_risk_profile_label(profile_name)

def _entry_mode_label(code: str | None, language: str | None = "es") -> str:
    if _lang(language) == "en":
        return ENTRY_MODE_LABELS_EN.get(code or "limit_wait", ENTRY_MODE_LABELS_EN["limit_wait"])
    return ENTRY_MODE_LABELS.get(code or "limit_wait", ENTRY_MODE_LABELS["limit_wait"])

def get_risk_plan_tier(plan: str | None) -> str:
    return "basic" if str(plan or PLAN_FREE).lower() == PLAN_FREE else "full"

def normalize_plan_for_risk(plan: str | None) -> str:
    value = str(plan or PLAN_FREE).lower()
    return value if value in {"free", "plus", "premium"} else PLAN_FREE

def _fmt_dt(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M UTC")
    return "—"

def _fmt_money(value) -> str:
    try:
        return f"{float(value):,.2f} USDT"
    except Exception:
        return "—"

def _fmt_pct_value(value) -> str:
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return "—"

def _fmt_fraction_as_pct(value) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "—"

def _signal_status(user_signal: Dict, language: str | None = "es") -> str:
    now = datetime.utcnow()
    telegram_valid_until = user_signal.get("telegram_valid_until")
    evaluation_valid_until = user_signal.get("evaluation_valid_until") or user_signal.get("valid_until")

    if isinstance(telegram_valid_until, datetime) and telegram_valid_until > now:
        return _t(language, "ACTIVA", "ACTIVE")
    if isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now:
        return _t(language, "CERRADA EN TELEGRAM / AÚN EVALUANDO", "CLOSED IN TELEGRAM / STILL EVALUATING")
    return _t(language, "FINALIZADA", "FINISHED")

def build_risk_management_text(profile: Dict, *, plan: str = PLAN_FREE, language: str = "es") -> str:
    plan = normalize_plan_for_risk(plan)
    tier = get_risk_plan_tier(plan)
    exchange = EXCHANGE_LABELS.get(profile.get("exchange"), str(profile.get("exchange") or "—").upper())

    lines = [
        _t(language, "⚙️ GESTIÓN DE RIESGO", "⚙️ RISK MANAGEMENT"),
        "",
        _t(language, "Tu configuración actual:", "Your current settings:"),
        f"• {_t(language, 'Capital', 'Capital')}: {_fmt_money(profile.get('capital_usdt'))}",
        f"• {_t(language, 'Riesgo por trade', 'Risk per trade')}: {_fmt_pct_value(profile.get('risk_percent'))}",
        f"• {_t(language, 'Exchange', 'Exchange')}: {exchange}",
        f"• {_t(language, 'Tipo de entrada', 'Entry mode')}: {_entry_mode_label(profile.get('entry_mode'), language)}",
        f"• {_t(language, 'Comisión por lado', 'Fee per side')}: {_fmt_pct_value(profile.get('fee_percent_per_side'))}",
        f"• {_t(language, 'Slippage estimado', 'Estimated slippage')}: {_fmt_pct_value(profile.get('slippage_percent'))}",
        f"• {_t(language, 'Apalancamiento por defecto', 'Default leverage')}: {float(profile.get('default_leverage') or 0):.0f}x",
    ]

    if tier == "basic":
        lines.append(f"• {_t(language, 'Perfil disponible en FREE', 'Profile available in FREE')}: {_profile_label('moderado', language)}")
        lines.extend([
            "",
            _t(language, "FREE incluye la calculadora básica con perfil moderado y TP1.", "FREE includes the basic calculator with the Moderate profile and TP1."),
            _t(language, "Para cambiar perfil y ver cálculo completo, sube a PLUS o PREMIUM.", "To change profile and view the full calculation, upgrade to PLUS or PREMIUM."),
        ])
    else:
        lines.append(f"• {_t(language, 'Perfil base de señal', 'Default signal profile')}: {_profile_label(profile.get('default_profile'), language)}")

    return "\n".join(lines)

def build_risk_management_keyboard(*, plan: str = PLAN_FREE, language: str = "es") -> InlineKeyboardMarkup:
    tier = get_risk_plan_tier(plan)
    rows = [
        [
            InlineKeyboardButton(_t(language, "💰 Capital", "💰 Capital"), callback_data="risk_set_capital"),
            InlineKeyboardButton(_t(language, "🎯 Riesgo %", "🎯 Risk %"), callback_data="risk_set_risk"),
        ],
        [
            InlineKeyboardButton(_t(language, "🏦 Exchange / comisión", "🏦 Exchange / fee"), callback_data="risk_set_exchange"),
            InlineKeyboardButton(_t(language, "💸 Fee manual", "💸 Manual fee"), callback_data="risk_set_fee"),
        ],
        [
            InlineKeyboardButton(_t(language, "📉 Slippage", "📉 Slippage"), callback_data="risk_set_slippage"),
            InlineKeyboardButton(_t(language, "📈 Leverage", "📈 Leverage"), callback_data="risk_set_leverage"),
        ],
    ]
    if tier == "full":
        rows.append([
            InlineKeyboardButton(_t(language, "🧭 Perfil base", "🧭 Default profile"), callback_data="risk_set_profile"),
            InlineKeyboardButton(_t(language, "🧪 Probar calculadora", "🧪 Test calculator"), callback_data="risk_test"),
        ])
    else:
        rows.append([InlineKeyboardButton(_t(language, "🧪 Probar calculadora", "🧪 Test calculator"), callback_data="risk_test")])
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver a mi cuenta", "⬅️ Back to my account"), callback_data="my_account")])
    return InlineKeyboardMarkup(rows)

def build_exchange_selection_text(current_exchange: str, *, language: str = "es") -> str:
    return (
        _t(language, "🏦 EXCHANGE / COMISIÓN", "🏦 EXCHANGE / FEE")
        + "\n\n"
        + _t(language,
             "Selecciona tu exchange principal.\nEl bot cargará una comisión estimada conservadora para futuros con orden límite.\nLuego podrás editar la fee manualmente si tu cuenta usa otra tarifa.",
             "Select your main exchange.\nThe bot will load a conservative estimated fee for futures with limit orders.\nYou can edit the fee manually later if your account uses a different rate.")
        + "\n\n"
        + f"{_t(language, 'Exchange actual', 'Current exchange')}: {EXCHANGE_LABELS.get(current_exchange, str(current_exchange).upper())}"
    )

def build_exchange_selection_keyboard(current_exchange: str, *, language: str = "es") -> InlineKeyboardMarkup:
    exchanges = ["binance", "lbank", "coinw", "weex", "coinex", "bitunix", "mexc", "other"]
    rows = []
    for idx in range(0, len(exchanges), 2):
        chunk = exchanges[idx: idx + 2]
        row = []
        for exchange in chunk:
            label = EXCHANGE_LABELS[exchange]
            if exchange == "other" and _lang(language) == "en":
                label = "Other"
            if exchange == current_exchange:
                label = f"✅ {label}"
            row.append(InlineKeyboardButton(label, callback_data=f"risk_pick_exchange:{exchange}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver a gestión de riesgo", "⬅️ Back to risk management"), callback_data="risk_menu")])
    return InlineKeyboardMarkup(rows)

def build_default_profile_selection_text(current_profile: str, *, language: str = "es") -> str:
    return (
        _t(language, "🧭 PERFIL BASE DE SEÑAL", "🧭 DEFAULT SIGNAL PROFILE")
        + "\n\n"
        + _t(language,
             "Este perfil se usará por defecto cuando calcules riesgo desde una señal.",
             "This profile will be used by default when you calculate risk from a signal.")
        + "\n\n"
        + f"{_t(language, 'Perfil actual', 'Current profile')}: {_profile_label(current_profile, language)}"
    )

def build_default_profile_selection_keyboard(current_profile: str, *, language: str = "es") -> InlineKeyboardMarkup:
    rows = []
    for profile_name in ["conservador", "moderado", "agresivo"]:
        label = _profile_label(profile_name, language)
        if profile_name == current_profile:
            label = f"✅ {label}"
        rows.append([InlineKeyboardButton(label, callback_data=f"risk_pick_profile:{profile_name}")])
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver a gestión de riesgo", "⬅️ Back to risk management"), callback_data="risk_menu")])
    return InlineKeyboardMarkup(rows)

def build_active_signals_list_text(signals: Iterable[Dict], *, language: str = "es") -> str:
    signals = list(signals)
    lines = [
        _t(language, "🚨 SEÑALES EN VIVO", "🚨 LIVE SIGNALS"),
        "",
        _t(language, "Selecciona la señal que quieres abrir:", "Select the signal you want to open:"),
        "",
    ]
    for idx, signal in enumerate(signals, 1):
        lines.append(
            f"{idx}. {signal.get('symbol', '—')} | {signal.get('direction', '—')} | "
            f"{str(signal.get('visibility') or '—').upper()} | Score {signal.get('score') or '—'}"
        )
    return "\n".join(lines).strip()

def build_active_signals_list_keyboard(signals: Iterable[Dict], *, language: str = "es") -> InlineKeyboardMarkup:
    rows = []
    for signal in signals:
        signal_id = signal.get("signal_id") or str(signal.get("_id") or "")
        label = f"{signal.get('symbol', '—')} {signal.get('direction', '—')}"
        rows.append([InlineKeyboardButton(label, callback_data=f"sig_detail:{signal_id}")])
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver al menú", "⬅️ Back to menu"), callback_data="back_menu")])
    return InlineKeyboardMarkup(rows)

def build_signal_detail_text(user_signal: Dict, *, source: str = "live", language: str = "es") -> str:
    header = _t(language, "🚨 DETALLE DE SEÑAL", "🚨 SIGNAL DETAIL") if source == "live" else _t(language, "🧾 DETALLE DE HISTORIAL", "🧾 HISTORY DETAIL")
    lines = [
        header,
        "",
        f"{_t(language, 'Par', 'Pair')}: {user_signal.get('symbol', '—')}",
        f"{_t(language, 'Dirección', 'Direction')}: {user_signal.get('direction', '—')}",
        f"{_t(language, 'Plan', 'Plan')}: {str(user_signal.get('visibility') or '—').upper()}",
        f"{_t(language, 'Estado', 'Status')}: {_signal_status(user_signal, language)}",
        f"{_t(language, 'Score raw', 'Raw score')}: {user_signal.get('score') or '—'}",
        f"{_t(language, 'Entrada base', 'Base entry')}: {user_signal.get('entry_price') or '—'}",
        f"{_t(language, 'Creada', 'Created')}: {_fmt_dt(user_signal.get('created_at'))}",
        f"{_t(language, 'Visible en Telegram hasta', 'Visible in Telegram until')}: {_fmt_dt(user_signal.get('telegram_valid_until'))}",
        f"{_t(language, 'Evaluación de mercado hasta', 'Market evaluation until')}: {_fmt_dt(user_signal.get('evaluation_valid_until') or user_signal.get('valid_until'))}",
        "",
        _t(language, "Perfiles operativos:", "Trading profiles:"),
    ]

    profiles = user_signal.get("profiles") or {}
    for profile_name in ["conservador", "moderado", "agresivo"]:
        profile = profiles.get(profile_name) or {}
        tps = profile.get("take_profits") or []
        tp1 = tps[0] if len(tps) > 0 else "—"
        tp2 = tps[1] if len(tps) > 1 else "—"
        lines.extend([
            f"• {_profile_label(profile_name, language)}",
            f"  SL: {profile.get('stop_loss', '—')}",
            f"  TP1: {tp1}",
            f"  TP2: {tp2}",
            f"  {_t(language, 'Apalancamiento sugerido', 'Suggested leverage')}: {profile.get('leverage', '—')}",
        ])

    return "\n".join(lines).strip()

def build_signal_detail_keyboard(signal_id: str, *, source: str = "live", plan: str = PLAN_FREE, language: str = "es") -> InlineKeyboardMarkup:
    back_cb = "view_signals" if source == "live" else "history"
    tier = get_risk_plan_tier(plan)

    rows = [
        [
            InlineKeyboardButton(_t(language, "📐 Calcular riesgo", "📐 Calculate risk"), callback_data=f"risk_calc:{source}:{signal_id}"),
            InlineKeyboardButton(_t(language, "📊 Ver análisis", "📊 View analysis"), callback_data=f"sig_an:{source}:{signal_id}"),
        ],
    ]

    if tier == "full":
        rows.append([
            InlineKeyboardButton(_t(language, "📍 Seguimiento", "📍 Tracking"), callback_data=f"sig_trk:{source}:{signal_id}"),
            InlineKeyboardButton(_t(language, "🧭 Elegir perfil", "🧭 Choose profile"), callback_data=f"risk_pf:{source}:{signal_id}"),
        ])
    else:
        rows.append([InlineKeyboardButton(_t(language, "📍 Seguimiento", "📍 Tracking"), callback_data=f"sig_trk:{source}:{signal_id}")])

    rows.append([InlineKeyboardButton(_t(language, "⚙️ Gestión de riesgo", "⚙️ Risk settings"), callback_data="risk_menu")])
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver", "⬅️ Back"), callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)

def build_history_list_text(docs: Iterable[Dict], *, language: str = "es") -> str:
    docs = list(docs)
    lines = [_t(language, "🧾 HISTORIAL", "🧾 HISTORY"), "", _t(language, "Selecciona una señal para ver detalle y calcular riesgo:", "Select a signal to view details and calculate risk:"), ""]
    for idx, d in enumerate(docs, 1):
        lines.append(f"{idx}. {d.get('symbol', '—')} | {d.get('direction', '—')} | {_signal_status(d, language)}")
    return "\n".join(lines).strip()

def build_history_list_keyboard(docs: Iterable[Dict], *, language: str = "es") -> InlineKeyboardMarkup:
    rows = []
    for signal in docs:
        signal_id = signal.get("signal_id") or str(signal.get("_id") or "")
        label = f"{signal.get('symbol', '—')} {signal.get('direction', '—')}"
        rows.append([InlineKeyboardButton(label, callback_data=f"hist_detail:{signal_id}")])
    rows.append([InlineKeyboardButton(_t(language, "🔄 Actualizar", "🔄 Refresh"), callback_data="history_refresh")])
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver al menú", "⬅️ Back to menu"), callback_data="back_menu")])
    return InlineKeyboardMarkup(rows)

def build_signal_profile_picker_text(selected_profile: str, *, source: str, plan: str = PLAN_FREE, language: str = "es") -> str:
    tier = get_risk_plan_tier(plan)
    source_label = {
        "live": _t(language, "señal activa", "live signal"),
        "hist": _t(language, "historial", "history"),
        "test": _t(language, "prueba rápida", "quick test"),
    }.get(source, _t(language, "señal", "signal"))
    if tier == "basic":
        return (
            _t(language, "🧭 PERFIL DE CÁLCULO", "🧭 CALCULATION PROFILE")
            + "\n\n"
            + _t(language, f"En FREE la calculadora trabaja con el perfil Moderado para esta {source_label}.", f"In FREE, the calculator uses the Moderate profile for this {source_label}.")
            + "\n\n"
            + _t(language, "Sube a PLUS o PREMIUM para elegir entre Conservador, Moderado y Agresivo.", "Upgrade to PLUS or PREMIUM to choose between Conservative, Moderate, and Aggressive.")
        )
    return (
        _t(language, "🧭 PERFIL DE CÁLCULO", "🧭 CALCULATION PROFILE")
        + "\n\n"
        + _t(language, f"Selecciona el perfil con el que quieres calcular esta {source_label}.", f"Select the profile you want to use to calculate this {source_label}.")
        + "\n"
        + f"{_t(language, 'Perfil actual por defecto', 'Current default profile')}: {_profile_label(selected_profile, language)}"
    )

def build_signal_profile_picker_keyboard(signal_id: str, *, source: str, selected_profile: str, plan: str = PLAN_FREE, language: str = "es") -> InlineKeyboardMarkup:
    tier = get_risk_plan_tier(plan)
    if source == "live":
        back_cb = f"sig_detail:{signal_id}"
    elif source == "hist":
        back_cb = f"hist_detail:{signal_id}"
    else:
        back_cb = "risk_menu"

    if tier == "basic":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(_t(language, "💼 Ver planes", "💼 View plans"), callback_data="plans")],
            [InlineKeyboardButton(_t(language, "⬅️ Volver", "⬅️ Back"), callback_data=back_cb)],
        ])

    rows = []
    for profile_name in ["conservador", "moderado", "agresivo"]:
        code = PROFILE_NAME_TO_CODE[profile_name]
        label = _profile_label(profile_name, language)
        if profile_name == selected_profile:
            label = f"✅ {label}"
        rows.append([InlineKeyboardButton(label, callback_data=f"risk_cp:{source}:{signal_id}:{code}")])

    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver", "⬅️ Back"), callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)

def build_risk_result_text(calc: Dict, *, plan: str = PLAN_FREE, language: str = "es") -> str:
    tier = get_risk_plan_tier(plan)
    diagnostics = calc.get("diagnostics") or {}
    lines = [
        f"{_t(language, '📐 GESTIÓN DE RIESGO', '📐 RISK MANAGEMENT')} — {calc.get('symbol', '—')} {calc.get('direction', '—')}",
        "",
    ]

    if tier == "basic":
        tp_results = calc.get("tp_results") or []
        tp1 = tp_results[0] if tp_results else None
        lines.extend([
            _t(language, "Versión disponible en FREE:", "Version available in FREE:"),
            f"• {_t(language, 'Perfil usado', 'Profile used')}: {calc.get('profile_label') or _profile_label('moderado', language)}",
            f"• {_t(language, 'Capital', 'Capital')}: {_fmt_money(calc.get('capital_usdt'))}",
            f"• {_t(language, 'Riesgo por trade', 'Risk per trade')}: {_fmt_pct_value(calc.get('risk_percent'))}",
            f"• {_t(language, 'Apalancamiento', 'Leverage')}: {float(calc.get('leverage') or 0):.0f}x",
            "",
            _t(language, "Resultado básico:", "Basic result:"),
            f"• {_t(language, 'Riesgo máximo', 'Maximum risk')}: {_fmt_money(calc.get('risk_amount_usdt'))}",
            f"• {_t(language, 'Tamaño nocional recomendado', 'Recommended notional size')}: {_fmt_money(calc.get('position_notional_usdt'))}",
            f"• {_t(language, 'Pérdida estimada en SL', 'Estimated loss at SL')}: {_fmt_money(calc.get('loss_at_stop_usdt'))}",
        ])
        if tp1:
            lines.append(f"• TP1: {tp1.get('price')} | {_t(language, 'Ganancia neta', 'Net profit')} {_fmt_money(tp1.get('net_profit_usdt'))} | R:R {float(tp1.get('rr_net') or 0):.2f}")
        else:
            lines.append("• TP1: —")
        lines.extend([
            "",
            _t(language, "PLUS y PREMIUM desbloquean:", "PLUS and PREMIUM unlock:"),
            _t(language, "• cambio de perfil (conservador / moderado / agresivo)", "• profile change (conservative / moderate / aggressive)"),
            _t(language, "• TP2 y cálculo completo", "• TP2 and full calculation"),
            _t(language, "• diagnóstico y advertencias avanzadas", "• advanced diagnostics and warnings"),
        ])
        if not calc.get("signal_active_for_entry", True):
            lines.append("")
            lines.append(_t(language, "⚠️ Esta señal ya no está activa para una entrada nueva. El cálculo es solo informativo.", "⚠️ This signal is no longer active for a new entry. The calculation is informational only."))
        return "\n".join(lines).strip()

    lines.extend([
        _t(language, "Configuración usada:", "Configuration used:"),
        f"• {_t(language, 'Perfil de señal', 'Signal profile')}: {calc.get('profile_label', '—')}",
        f"• {_t(language, 'Capital', 'Capital')}: {_fmt_money(calc.get('capital_usdt'))}",
        f"• {_t(language, 'Riesgo por trade', 'Risk per trade')}: {_fmt_pct_value(calc.get('risk_percent'))}",
        f"• {_t(language, 'Exchange', 'Exchange')}: {EXCHANGE_LABELS.get(calc.get('exchange'), str(calc.get('exchange') or '—').upper())}",
        f"• {_t(language, 'Tipo de entrada', 'Entry mode')}: {calc.get('entry_mode_label', '—')}",
        f"• {_t(language, 'Comisión por lado', 'Fee per side')}: {_fmt_pct_value(calc.get('fee_percent_per_side'))}",
        f"• {_t(language, 'Slippage estimado', 'Estimated slippage')}: {_fmt_pct_value(calc.get('slippage_percent'))}",
        f"• {_t(language, 'Apalancamiento', 'Leverage')}: {float(calc.get('leverage') or 0):.0f}x",
        "",
        _t(language, "Resultado:", "Result:"),
        f"• {_t(language, 'Riesgo máximo', 'Maximum risk')}: {_fmt_money(calc.get('risk_amount_usdt'))}",
        f"• {_t(language, 'Distancia al SL', 'Distance to SL')}: {_fmt_fraction_as_pct(calc.get('stop_distance_pct'))}",
        f"• {_t(language, 'Riesgo efectivo total', 'Total effective risk')}: {_fmt_fraction_as_pct(calc.get('effective_loss_pct'))}",
        f"• {_t(language, 'Tamaño nocional recomendado', 'Recommended notional size')}: {_fmt_money(calc.get('position_notional_usdt'))}",
        f"• {_t(language, 'Margen estimado requerido', 'Estimated required margin')}: {_fmt_money(calc.get('required_margin_usdt'))}",
        f"• {_t(language, 'Pérdida estimada en SL', 'Estimated loss at SL')}: {_fmt_money(calc.get('loss_at_stop_usdt'))}",
    ])

    tp_results = calc.get("tp_results") or []
    if tp_results:
        lines.append("")
        lines.append(_t(language, "Take profits:", "Take profits:"))
        for tp in tp_results:
            lines.append(
                f"• {tp.get('name')}: {tp.get('price')} | {_t(language, 'Ganancia neta', 'Net profit')} {_fmt_money(tp.get('net_profit_usdt'))} | R:R {float(tp.get('rr_net') or 0):.2f}"
            )

    if diagnostics:
        lines.append("")
        lines.append(_t(language, "Diagnóstico:", "Diagnostics:"))
        lines.append(f"• {_t(language, 'Uso de margen', 'Margin usage')}: {_fmt_pct_value(diagnostics.get('margin_usage_pct'))}")
        lines.append(f"• {_t(language, 'Colchón de capital', 'Capital buffer')}: {_fmt_money(diagnostics.get('capital_buffer_usdt'))}")
        lines.append(f"• {_t(language, 'Mejor R:R neto', 'Best net R:R')}: {float(diagnostics.get('best_rr_net') or 0):.2f}")
        lines.append(f"• {_t(language, 'Banda de riesgo', 'Risk band')}: {str(diagnostics.get('risk_band') or 'normal').upper()}")

    if not calc.get("signal_active_for_entry", True):
        lines.append("")
        lines.append(_t(language, "⚠️ Esta señal ya no está activa para una entrada nueva. El cálculo es solo informativo.", "⚠️ This signal is no longer active for a new entry. The calculation is informational only."))

    warnings = calc.get("warnings") or []
    if warnings:
        lines.append("")
        lines.append(_t(language, "Advertencias:", "Warnings:"))
        for warning in warnings:
            lines.append(f"• {warning}")

    if not calc.get("is_operable", False):
        lines.append("")
        lines.append(_t(language, "⛔ Con la configuración actual, la operación no es operable de forma sana.", "⛔ With the current configuration, the trade is not safely operable."))

    return "\n".join(lines).strip()

def build_risk_result_keyboard(signal_id: str, *, source: str = "live", plan: str = PLAN_FREE, language: str = "es") -> InlineKeyboardMarkup:
    tier = get_risk_plan_tier(plan)
    if source == "live":
        back_cb = f"sig_detail:{signal_id}"
    elif source == "hist":
        back_cb = f"hist_detail:{signal_id}"
    else:
        back_cb = "risk_menu"

    rows = []
    if signal_id:
        if tier == "full":
            rows.append([
                InlineKeyboardButton(_t(language, "📍 Seguimiento", "📍 Tracking"), callback_data=f"sig_trk:{source}:{signal_id}"),
                InlineKeyboardButton(_t(language, "🧭 Cambiar perfil", "🧭 Change profile"), callback_data=f"risk_pf:{source}:{signal_id}"),
            ])
        else:
            rows.append([
                InlineKeyboardButton(_t(language, "📍 Seguimiento", "📍 Tracking"), callback_data=f"sig_trk:{source}:{signal_id}"),
            ])
            rows.append([InlineKeyboardButton(_t(language, "💼 Ver planes", "💼 View plans"), callback_data="plans")])
        rows.append([InlineKeyboardButton(_t(language, "⚙️ Ajustes", "⚙️ Settings"), callback_data="risk_menu")])
    else:
        rows.append([InlineKeyboardButton(_t(language, "⚙️ Ajustes", "⚙️ Settings"), callback_data="risk_menu")])
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver", "⬅️ Back"), callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)
