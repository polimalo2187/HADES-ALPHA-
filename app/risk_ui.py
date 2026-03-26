from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

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


def _signal_status(user_signal: Dict) -> str:
    now = datetime.utcnow()
    telegram_valid_until = user_signal.get("telegram_valid_until")
    evaluation_valid_until = user_signal.get("evaluation_valid_until") or user_signal.get("valid_until")

    if isinstance(telegram_valid_until, datetime) and telegram_valid_until > now:
        return "ACTIVA"
    if isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now:
        return "CERRADA EN TELEGRAM / AÚN EVALUANDO"
    return "FINALIZADA"


def build_risk_management_text(profile: Dict) -> str:
    exchange = EXCHANGE_LABELS.get(profile.get("exchange"), str(profile.get("exchange") or "—").upper())
    return (
        "⚙️ GESTIÓN DE RIESGO\n\n"
        "Tu configuración actual:\n"
        f"• Capital: {_fmt_money(profile.get('capital_usdt'))}\n"
        f"• Riesgo por trade: {_fmt_pct_value(profile.get('risk_percent'))}\n"
        f"• Exchange: {exchange}\n"
        f"• Tipo de entrada: {ENTRY_MODE_LABELS.get(profile.get('entry_mode'), 'Límite esperando precio')}\n"
        f"• Comisión por lado: {_fmt_pct_value(profile.get('fee_percent_per_side'))}\n"
        f"• Slippage estimado: {_fmt_pct_value(profile.get('slippage_percent'))}\n"
        f"• Apalancamiento por defecto: {float(profile.get('default_leverage') or 0):.0f}x\n"
        f"• Perfil base de señal: {get_risk_profile_label(profile.get('default_profile'))}\n"
    )


def build_risk_management_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💰 Capital", callback_data="risk_set_capital"),
            InlineKeyboardButton("🎯 Riesgo %", callback_data="risk_set_risk"),
        ],
        [
            InlineKeyboardButton("🏦 Exchange / comisión", callback_data="risk_set_exchange"),
            InlineKeyboardButton("💸 Fee manual", callback_data="risk_set_fee"),
        ],
        [
            InlineKeyboardButton("📉 Slippage", callback_data="risk_set_slippage"),
            InlineKeyboardButton("📈 Leverage", callback_data="risk_set_leverage"),
        ],
        [
            InlineKeyboardButton("🧭 Perfil base", callback_data="risk_set_profile"),
            InlineKeyboardButton("🧪 Probar calculadora", callback_data="risk_test"),
        ],
        [InlineKeyboardButton("⬅️ Volver a mi cuenta", callback_data="my_account")],
    ])


def build_exchange_selection_text(current_exchange: str) -> str:
    return (
        "🏦 EXCHANGE / COMISIÓN\n\n"
        "Selecciona tu exchange principal.\n"
        "El bot cargará una comisión estimada conservadora para futuros con orden límite.\n"
        "Luego podrás editar la fee manualmente si tu cuenta usa otra tarifa.\n\n"
        f"Exchange actual: {EXCHANGE_LABELS.get(current_exchange, str(current_exchange).upper())}"
    )


def build_exchange_selection_keyboard(current_exchange: str) -> InlineKeyboardMarkup:
    exchanges = ["binance", "lbank", "coinw", "weex", "coinex", "bitunix", "mexc", "other"]
    rows = []
    for idx in range(0, len(exchanges), 2):
        chunk = exchanges[idx: idx + 2]
        row = []
        for exchange in chunk:
            label = EXCHANGE_LABELS[exchange]
            if exchange == current_exchange:
                label = f"✅ {label}"
            row.append(InlineKeyboardButton(label, callback_data=f"risk_pick_exchange:{exchange}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Volver a gestión de riesgo", callback_data="risk_menu")])
    return InlineKeyboardMarkup(rows)


def build_default_profile_selection_text(current_profile: str) -> str:
    return (
        "🧭 PERFIL BASE DE SEÑAL\n\n"
        "Este perfil se usará por defecto cuando calcules riesgo desde una señal.\n\n"
        f"Perfil actual: {get_risk_profile_label(current_profile)}"
    )


def build_default_profile_selection_keyboard(current_profile: str) -> InlineKeyboardMarkup:
    rows = []
    for profile_name in ["conservador", "moderado", "agresivo"]:
        label = get_risk_profile_label(profile_name)
        if profile_name == current_profile:
            label = f"✅ {label}"
        rows.append([InlineKeyboardButton(label, callback_data=f"risk_pick_profile:{profile_name}")])
    rows.append([InlineKeyboardButton("⬅️ Volver a gestión de riesgo", callback_data="risk_menu")])
    return InlineKeyboardMarkup(rows)


def build_active_signals_list_text(signals: Iterable[Dict]) -> str:
    signals = list(signals)
    lines = ["🚨 SEÑALES EN VIVO", "", "Selecciona la señal que quieres abrir:", ""]
    for idx, signal in enumerate(signals, 1):
        lines.append(
            f"{idx}. {signal.get('symbol', '—')} | {signal.get('direction', '—')} | "
            f"{str(signal.get('visibility') or '—').upper()} | Score {signal.get('score') or '—'}"
        )
    return "\n".join(lines).strip()


def build_active_signals_list_keyboard(signals: Iterable[Dict]) -> InlineKeyboardMarkup:
    rows = []
    for signal in signals:
        signal_id = signal.get("signal_id") or str(signal.get("_id") or "")
        label = f"{signal.get('symbol', '—')} {signal.get('direction', '—')}"
        rows.append([InlineKeyboardButton(label, callback_data=f"sig_detail:{signal_id}")])
    rows.append([InlineKeyboardButton("⬅️ Volver al menú", callback_data="back_menu")])
    return InlineKeyboardMarkup(rows)


def build_signal_detail_text(user_signal: Dict, *, source: str = "live") -> str:
    header = "🚨 DETALLE DE SEÑAL" if source == "live" else "🧾 DETALLE DE HISTORIAL"
    lines = [
        header,
        "",
        f"Par: {user_signal.get('symbol', '—')}",
        f"Dirección: {user_signal.get('direction', '—')}",
        f"Plan: {str(user_signal.get('visibility') or '—').upper()}",
        f"Estado: {_signal_status(user_signal)}",
        f"Score raw: {user_signal.get('score') or '—'}",
        f"Entrada base: {user_signal.get('entry_price') or '—'}",
        f"Creada: {_fmt_dt(user_signal.get('created_at'))}",
        f"Visible en Telegram hasta: {_fmt_dt(user_signal.get('telegram_valid_until'))}",
        f"Evaluación de mercado hasta: {_fmt_dt(user_signal.get('evaluation_valid_until') or user_signal.get('valid_until'))}",
        "",
        "Perfiles operativos:",
    ]

    profiles = user_signal.get("profiles") or {}
    for profile_name in ["conservador", "moderado", "agresivo"]:
        profile = profiles.get(profile_name) or {}
        tps = profile.get("take_profits") or []
        tp1 = tps[0] if len(tps) > 0 else "—"
        tp2 = tps[1] if len(tps) > 1 else "—"
        lines.extend([
            f"• {get_risk_profile_label(profile_name)}",
            f"  SL: {profile.get('stop_loss', '—')}",
            f"  TP1: {tp1}",
            f"  TP2: {tp2}",
            f"  Apalancamiento sugerido: {profile.get('leverage', '—')}",
        ])

    return "\n".join(lines).strip()


def build_signal_detail_keyboard(signal_id: str, *, source: str = "live") -> InlineKeyboardMarkup:
    back_cb = "view_signals" if source == "live" else "history"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📐 Calcular riesgo", callback_data=f"risk_calc:{source}:{signal_id}"),
            InlineKeyboardButton("📊 Ver análisis", callback_data=f"sig_an:{source}:{signal_id}"),
        ],
        [InlineKeyboardButton("🧭 Elegir perfil", callback_data=f"risk_pf:{source}:{signal_id}")],
        [InlineKeyboardButton("⚙️ Gestión de riesgo", callback_data="risk_menu")],
        [InlineKeyboardButton("⬅️ Volver", callback_data=back_cb)],
    ])


def build_history_list_text(docs: Iterable[Dict]) -> str:
    docs = list(docs)
    lines = ["🧾 HISTORIAL", "", "Selecciona una señal para ver detalle y calcular riesgo:", ""]
    for idx, d in enumerate(docs, 1):
        lines.append(f"{idx}. {d.get('symbol', '—')} | {d.get('direction', '—')} | {_signal_status(d)}")
    return "\n".join(lines).strip()


def build_history_list_keyboard(docs: Iterable[Dict]) -> InlineKeyboardMarkup:
    rows = []
    for signal in docs:
        signal_id = signal.get("signal_id") or str(signal.get("_id") or "")
        label = f"{signal.get('symbol', '—')} {signal.get('direction', '—')}"
        rows.append([InlineKeyboardButton(label, callback_data=f"hist_detail:{signal_id}")])
    rows.append([InlineKeyboardButton("🔄 Actualizar", callback_data="history_refresh")])
    rows.append([InlineKeyboardButton("⬅️ Volver al menú", callback_data="back_menu")])
    return InlineKeyboardMarkup(rows)


def build_signal_profile_picker_text(selected_profile: str, *, source: str) -> str:
    source_label = {"live": "señal activa", "hist": "historial", "test": "prueba rápida"}.get(source, "señal")
    return (
        f"🧭 PERFIL DE CÁLCULO\n\n"
        f"Selecciona el perfil con el que quieres calcular esta {source_label}.\n"
        f"Perfil actual por defecto: {get_risk_profile_label(selected_profile)}"
    )


def build_signal_profile_picker_keyboard(signal_id: str, *, source: str, selected_profile: str) -> InlineKeyboardMarkup:
    rows = []
    for profile_name in ["conservador", "moderado", "agresivo"]:
        code = PROFILE_NAME_TO_CODE[profile_name]
        label = get_risk_profile_label(profile_name)
        if profile_name == selected_profile:
            label = f"✅ {label}"
        rows.append([InlineKeyboardButton(label, callback_data=f"risk_cp:{source}:{signal_id}:{code}")])

    if source == "live":
        back_cb = f"sig_detail:{signal_id}"
    elif source == "hist":
        back_cb = f"hist_detail:{signal_id}"
    else:
        back_cb = "risk_menu"

    rows.append([InlineKeyboardButton("⬅️ Volver", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)


def build_risk_result_text(calc: Dict) -> str:
    diagnostics = calc.get("diagnostics") or {}
    lines = [
        f"📐 GESTIÓN DE RIESGO — {calc.get('symbol', '—')} {calc.get('direction', '—')}",
        "",
        "Configuración usada:",
        f"• Perfil de señal: {calc.get('profile_label', '—')}",
        f"• Capital: {_fmt_money(calc.get('capital_usdt'))}",
        f"• Riesgo por trade: {_fmt_pct_value(calc.get('risk_percent'))}",
        f"• Exchange: {EXCHANGE_LABELS.get(calc.get('exchange'), str(calc.get('exchange') or '—').upper())}",
        f"• Tipo de entrada: {calc.get('entry_mode_label', '—')}",
        f"• Comisión por lado: {_fmt_pct_value(calc.get('fee_percent_per_side'))}",
        f"• Slippage estimado: {_fmt_pct_value(calc.get('slippage_percent'))}",
        f"• Apalancamiento: {float(calc.get('leverage') or 0):.0f}x",
        "",
        "Resultado:",
        f"• Riesgo máximo: {_fmt_money(calc.get('risk_amount_usdt'))}",
        f"• Distancia al SL: {_fmt_fraction_as_pct(calc.get('stop_distance_pct'))}",
        f"• Riesgo efectivo total: {_fmt_fraction_as_pct(calc.get('effective_loss_pct'))}",
        f"• Tamaño nocional recomendado: {_fmt_money(calc.get('position_notional_usdt'))}",
        f"• Margen estimado requerido: {_fmt_money(calc.get('required_margin_usdt'))}",
        f"• Pérdida estimada en SL: {_fmt_money(calc.get('loss_at_stop_usdt'))}",
    ]

    tp_results = calc.get("tp_results") or []
    if tp_results:
        lines.append("")
        lines.append("Take profits:")
        for tp in tp_results:
            lines.append(
                f"• {tp.get('name')}: {tp.get('price')} | Ganancia neta {_fmt_money(tp.get('net_profit_usdt'))} | R:R {float(tp.get('rr_net') or 0):.2f}"
            )

    if diagnostics:
        lines.append("")
        lines.append("Diagnóstico:")
        lines.append(f"• Uso de margen: {_fmt_pct_value(diagnostics.get('margin_usage_pct'))}")
        lines.append(f"• Colchón de capital: {_fmt_money(diagnostics.get('capital_buffer_usdt'))}")
        lines.append(f"• Mejor R:R neto: {float(diagnostics.get('best_rr_net') or 0):.2f}")
        lines.append(f"• Banda de riesgo: {str(diagnostics.get('risk_band') or 'normal').upper()}")

    if not calc.get("signal_active_for_entry", True):
        lines.append("")
        lines.append("⚠️ Esta señal ya no está activa para una entrada nueva. El cálculo es solo informativo.")

    warnings = calc.get("warnings") or []
    if warnings:
        lines.append("")
        lines.append("Advertencias:")
        for warning in warnings:
            lines.append(f"• {warning}")

    if not calc.get("is_operable", False):
        lines.append("")
        lines.append("⛔ Con la configuración actual, la operación no es operable de forma sana.")

    return "\n".join(lines).strip()


def build_risk_result_keyboard(signal_id: str, *, source: str = "live") -> InlineKeyboardMarkup:
    if source == "live":
        back_cb = f"sig_detail:{signal_id}"
    elif source == "hist":
        back_cb = f"hist_detail:{signal_id}"
    else:
        back_cb = "risk_menu"

    rows = []
    if signal_id:
        rows.append([
            InlineKeyboardButton("🧭 Cambiar perfil", callback_data=f"risk_pf:{source}:{signal_id}"),
            InlineKeyboardButton("⚙️ Ajustes", callback_data="risk_menu"),
        ])
    else:
        rows.append([InlineKeyboardButton("⚙️ Ajustes", callback_data="risk_menu")])
    rows.append([InlineKeyboardButton("⬅️ Volver", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)
