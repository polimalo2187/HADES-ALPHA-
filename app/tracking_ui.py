from __future__ import annotations

from datetime import datetime
from typing import Dict

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.risk import get_risk_profile_label


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


def _fmt_dt(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M UTC")
    return "—"


def _fmt_num(value, digits: int = 4) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "—"


def _fmt_pct_fraction(value) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "—"


def _fmt_pct_percent(value) -> str:
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return "—"


def _tracking_feature_tier(plan: str) -> str:
    plan_value = str(plan or "free").lower()
    if plan_value == "premium":
        return "advanced"
    if plan_value == "plus":
        return "full"
    return "basic"


def _yes_no(value: bool, language: str | None = "es") -> str:
    return _t(language, "Sí", "Yes") if value else _t(language, "No", "No")


def build_signal_tracking_text(payload: Dict, *, plan: str = "free", language: str = "es") -> str:
    tier = _tracking_feature_tier(plan)
    take_profits = payload.get("take_profits") or []
    tp1 = _fmt_num(take_profits[0]) if len(take_profits) > 0 else "—"
    tp2 = _fmt_num(take_profits[1]) if len(take_profits) > 1 else "—"

    if tier == "basic":
        lines = [
            f"{_t(language, '📍 SEGUIMIENTO', '📍 TRACKING')} — {payload.get('symbol', '—')} {payload.get('direction', '—')}",
            "",
            f"{_t(language, 'Estado', 'Status')}: {payload.get('state_label', '—')}",
            f"{_t(language, 'Operabilidad', 'Operability')}: {_t(language, 'Activa ahora', 'Tradable now') if payload.get('is_operable_now') else _t(language, 'No recomendable ahora', 'Not recommended now')}",
            f"{_t(language, 'Resultado final', 'Final result')}: {payload.get('result_label', _t(language, 'Aún sin cierre final', 'No final close yet'))}",
            "",
            _t(language, "Lectura rápida:", "Quick reading:"),
            f"• {_t(language, 'Precio actual', 'Current price')}: {_fmt_num(payload.get('current_price'))}",
            f"• {_t(language, 'Entrada base', 'Base entry')}: {_fmt_num(payload.get('entry_price'))}",
            f"• SL: {_fmt_num(payload.get('stop_loss'))}",
            f"• TP1: {tp1}",
            f"• {_t(language, 'Distancia a entrada', 'Distance to entry')}: {_fmt_pct_fraction(payload.get('distance_to_entry_pct'))}",
            f"• {_t(language, 'Distancia a SL', 'Distance to SL')}: {_fmt_pct_fraction(payload.get('stop_distance_pct'))}",
            "",
            _t(language, "Recomendación:", "Recommendation:"),
            f"• {payload.get('recommendation', '—')}",
            "",
            _t(language, "🔒 Seguimiento completo disponible en PLUS y PREMIUM.", "🔒 Full tracking available in PLUS and PREMIUM."),
        ]
        return "\n".join(lines)

    lines = [
        f"{_t(language, '📍 SEGUIMIENTO', '📍 TRACKING')} — {payload.get('symbol', '—')} {payload.get('direction', '—')}",
        "",
        f"{_t(language, 'Estado general', 'General status')}: {payload.get('state_label', '—')}",
        f"{_t(language, 'Estado operativo', 'Operational status')}: {payload.get('entry_state_label', '—')}",
        f"{_t(language, 'Perfil seguido', 'Tracked profile')}: {_profile_label(payload.get('selected_profile'), language)}",
        f"{_t(language, 'Resultado final', 'Final result')}: {payload.get('result_label', _t(language, 'Aún sin cierre final', 'No final close yet'))}",
        "",
        _t(language, "Snapshot actual:", "Current snapshot:"),
        f"• {_t(language, 'Precio actual', 'Current price')}: {_fmt_num(payload.get('current_price'))}",
        f"• {_t(language, 'Entrada base', 'Base entry')}: {_fmt_num(payload.get('entry_price'))}",
        f"• {_t(language, 'Distancia actual a entrada', 'Current distance to entry')}: {_fmt_pct_fraction(payload.get('distance_to_entry_pct'))}",
        "",
        _t(language, "Perfil operativo actual:", "Current operating profile:"),
        f"• SL: {_fmt_num(payload.get('stop_loss'))}",
        f"• TP1: {tp1}",
        f"• TP2: {tp2}",
        f"• {_t(language, 'Distancia a SL', 'Distance to SL')}: {_fmt_pct_fraction(payload.get('stop_distance_pct'))}",
        f"• {_t(language, 'Distancia a TP1', 'Distance to TP1')}: {_fmt_pct_fraction(payload.get('tp1_distance_pct'))}",
        "",
        _t(language, "Lectura rápida:", "Quick reading:"),
        f"• {_t(language, 'En zona de entrada', 'In entry zone')}: {_yes_no(bool(payload.get('in_entry_zone')), language)}",
        f"• {_t(language, 'TP1 alcanzado ahora', 'TP1 hit now')}: {_yes_no(bool(payload.get('tp1_hit_now')), language)}",
        f"• {_t(language, 'Operable ahora', 'Tradable now')}: {_yes_no(bool(payload.get('is_operable_now')), language)}",
        "",
        _t(language, "Recomendación:", "Recommendation:"),
        f"• {payload.get('recommendation', '—')}",
    ]

    warnings = payload.get("warnings") or []
    progress = payload.get("progress_to_tp1_pct")

    if tier == "full":
        if progress is not None:
            lines.insert(lines.index(_t(language, "Recomendación:", "Recommendation:")), f"• {_t(language, 'Progreso hacia TP1', 'Progress to TP1')}: {_fmt_pct_percent(progress)}")
        lines.extend([
            "",
            _t(language, "Ventanas:", "Windows:"),
            f"• {_t(language, 'Visible en Telegram hasta', 'Visible in Telegram until')}: {_fmt_dt(payload.get('telegram_valid_until'))}",
            f"• {_t(language, 'Evaluación de mercado hasta', 'Market evaluation until')}: {_fmt_dt(payload.get('evaluation_valid_until') or payload.get('valid_until'))}",
        ])
        if warnings:
            lines.extend(["", _t(language, "Notas:", "Notes:")])
            for warning in warnings[:3]:
                lines.append(f"• {warning}")
        return "\n".join(lines)

    lines = [
        f"{_t(language, '📍 SEGUIMIENTO', '📍 TRACKING')} — {payload.get('symbol', '—')} {payload.get('direction', '—')}",
        "",
        f"{_t(language, 'Estado general', 'General status')}: {payload.get('state_label', '—')}",
        f"{_t(language, 'Estado operativo', 'Operational status')}: {payload.get('entry_state_label', '—')}",
        f"{_t(language, 'Perfil seguido', 'Tracked profile')}: {_profile_label(payload.get('selected_profile'), language)}",
        f"{_t(language, 'Resultado final', 'Final result')}: {payload.get('result_label', _t(language, 'Aún sin cierre final', 'No final close yet'))}",
        "",
        _t(language, "Snapshot actual:", "Current snapshot:"),
        f"• {_t(language, 'Precio actual', 'Current price')}: {_fmt_num(payload.get('current_price'))}",
        f"• {_t(language, 'Entrada base', 'Base entry')}: {_fmt_num(payload.get('entry_price'))}",
        f"• {_t(language, 'Zona de entrada', 'Entry zone')}: {_fmt_num(payload.get('entry_zone_low'))} → {_fmt_num(payload.get('entry_zone_high'))}",
        f"• {_t(language, 'Distancia actual a entrada', 'Current distance to entry')}: {_fmt_pct_fraction(payload.get('distance_to_entry_pct'))}",
        f"• {_t(language, 'Movimiento vs entrada', 'Move vs entry')}: {_fmt_pct_fraction(payload.get('current_move_pct'))}",
        "",
        _t(language, "Perfil operativo actual:", "Current operating profile:"),
        f"• SL: {_fmt_num(payload.get('stop_loss'))}",
        f"• TP1: {tp1}",
        f"• TP2: {tp2}",
        f"• {_t(language, 'Distancia a SL', 'Distance to SL')}: {_fmt_pct_fraction(payload.get('stop_distance_pct'))}",
        f"• {_t(language, 'Distancia a TP1', 'Distance to TP1')}: {_fmt_pct_fraction(payload.get('tp1_distance_pct'))}",
        f"• {_t(language, 'Distancia a TP2', 'Distance to TP2')}: {_fmt_pct_fraction(payload.get('tp2_distance_pct'))}",
        "",
        _t(language, "Lectura avanzada:", "Advanced reading:"),
        f"• {_t(language, 'En zona de entrada', 'In entry zone')}: {_yes_no(bool(payload.get('in_entry_zone')), language)}",
        f"• {_t(language, 'TP1 ya alcanzado en precio actual', 'TP1 already reached at current price')}: {_yes_no(bool(payload.get('tp1_hit_now')), language)}",
        f"• {_t(language, 'TP2 ya alcanzado en precio actual', 'TP2 already reached at current price')}: {_yes_no(bool(payload.get('tp2_hit_now')), language)}",
        f"• {_t(language, 'SL roto en precio actual', 'SL broken at current price')}: {_yes_no(bool(payload.get('stop_hit_now')), language)}",
        f"• {_t(language, 'Operable ahora', 'Tradable now')}: {_yes_no(bool(payload.get('is_operable_now')), language)}",
    ]
    if progress is not None:
        lines.append(f"• {_t(language, 'Progreso hacia TP1', 'Progress to TP1')}: {_fmt_pct_percent(progress)}")
    lines.extend([
        "",
        _t(language, "Recomendación:", "Recommendation:"),
        f"• {payload.get('recommendation', '—')}",
        "",
        _t(language, "Ventanas:", "Windows:"),
        f"• {_t(language, 'Creada', 'Created')}: {_fmt_dt(payload.get('created_at'))}",
        f"• {_t(language, 'Visible en Telegram hasta', 'Visible in Telegram until')}: {_fmt_dt(payload.get('telegram_valid_until'))}",
        f"• {_t(language, 'Evaluación de mercado hasta', 'Market evaluation until')}: {_fmt_dt(payload.get('evaluation_valid_until') or payload.get('valid_until'))}",
    ])
    if warnings:
        lines.extend(["", _t(language, "Notas avanzadas:", "Advanced notes:")])
        for warning in warnings[:5]:
            lines.append(f"• {warning}")
    return "\n".join(lines)


def build_signal_tracking_keyboard(signal_id: str, *, source: str = "live", plan: str = "free", language: str = "es") -> InlineKeyboardMarkup:
    if source == "live":
        back_cb = f"sig_detail:{signal_id}"
    elif source == "hist":
        back_cb = f"hist_detail:{signal_id}"
    else:
        back_cb = "risk_menu"

    tier = _tracking_feature_tier(plan)
    rows = []
    if signal_id:
        rows.append([
            InlineKeyboardButton(_t(language, "🔄 Actualizar", "🔄 Refresh"), callback_data=f"sig_trk:{source}:{signal_id}"),
            InlineKeyboardButton(_t(language, "📊 Ver análisis", "📊 View analysis"), callback_data=f"sig_an:{source}:{signal_id}"),
        ])
        if tier == "basic":
            rows.append([
                InlineKeyboardButton(_t(language, "📐 Calcular riesgo", "📐 Calculate risk"), callback_data=f"risk_calc:{source}:{signal_id}"),
                InlineKeyboardButton(_t(language, "💼 Ver planes", "💼 View plans"), callback_data="plans"),
            ])
        else:
            rows.append([
                InlineKeyboardButton(_t(language, "📐 Calcular riesgo", "📐 Calculate risk"), callback_data=f"risk_calc:{source}:{signal_id}"),
                InlineKeyboardButton(_t(language, "🧭 Elegir perfil", "🧭 Choose profile"), callback_data=f"risk_pf:{source}:{signal_id}"),
            ])
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver", "⬅️ Back"), callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)
