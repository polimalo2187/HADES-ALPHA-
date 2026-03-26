from __future__ import annotations

from datetime import datetime
from typing import Dict

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
from app.risk import get_risk_profile_label


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
    plan_value = str(plan or PLAN_FREE).lower()
    if plan_value == PLAN_PREMIUM:
        return "advanced"
    if plan_value == PLAN_PLUS:
        return "full"
    return "basic"


def build_signal_tracking_text(payload: Dict, *, plan: str = PLAN_FREE) -> str:
    tier = _tracking_feature_tier(plan)
    take_profits = payload.get("take_profits") or []
    tp1 = _fmt_num(take_profits[0]) if len(take_profits) > 0 else "—"
    tp2 = _fmt_num(take_profits[1]) if len(take_profits) > 1 else "—"

    if tier == "basic":
        lines = [
            f"📍 SEGUIMIENTO — {payload.get('symbol', '—')} {payload.get('direction', '—')}",
            "",
            f"Estado: {payload.get('state_label', '—')}",
            f"Operabilidad: {'Activa ahora' if payload.get('is_operable_now') else 'No recomendable ahora'}",
            f"Resultado final: {payload.get('result_label', 'Aún sin cierre final')}",
            "",
            "Lectura rápida:",
            f"• Precio actual: {_fmt_num(payload.get('current_price'))}",
            f"• Entrada base: {_fmt_num(payload.get('entry_price'))}",
            f"• SL: {_fmt_num(payload.get('stop_loss'))}",
            f"• TP1: {tp1}",
            f"• Distancia a entrada: {_fmt_pct_fraction(payload.get('distance_to_entry_pct'))}",
            f"• Distancia a SL: {_fmt_pct_fraction(payload.get('stop_distance_pct'))}",
            "",
            "Recomendación:",
            f"• {payload.get('recommendation', '—')}",
            "",
            "🔒 Seguimiento completo disponible en PLUS y PREMIUM.",
        ]
        return "\n".join(lines)

    lines = [
        f"📍 SEGUIMIENTO — {payload.get('symbol', '—')} {payload.get('direction', '—')}",
        "",
        f"Estado general: {payload.get('state_label', '—')}",
        f"Estado operativo: {payload.get('entry_state_label', '—')}",
        f"Perfil seguido: {get_risk_profile_label(payload.get('selected_profile'))}",
        f"Resultado final: {payload.get('result_label', 'Aún sin cierre final')}",
        "",
        "Snapshot actual:",
        f"• Precio actual: {_fmt_num(payload.get('current_price'))}",
        f"• Entrada base: {_fmt_num(payload.get('entry_price'))}",
        f"• Distancia actual a entrada: {_fmt_pct_fraction(payload.get('distance_to_entry_pct'))}",
        "",
        "Perfil operativo actual:",
        f"• SL: {_fmt_num(payload.get('stop_loss'))}",
        f"• TP1: {tp1}",
        f"• TP2: {tp2}",
        f"• Distancia a SL: {_fmt_pct_fraction(payload.get('stop_distance_pct'))}",
        f"• Distancia a TP1: {_fmt_pct_fraction(payload.get('tp1_distance_pct'))}",
        "",
        "Lectura rápida:",
        f"• En zona de entrada: {'Sí' if payload.get('in_entry_zone') else 'No'}",
        f"• TP1 alcanzado ahora: {'Sí' if payload.get('tp1_hit_now') else 'No'}",
        f"• Operable ahora: {'Sí' if payload.get('is_operable_now') else 'No'}",
        "",
        "Recomendación:",
        f"• {payload.get('recommendation', '—')}",
    ]

    warnings = payload.get("warnings") or []
    progress = payload.get("progress_to_tp1_pct")

    if tier == "full":
        if progress is not None:
            lines.insert(lines.index("Recomendación:"), f"• Progreso hacia TP1: {_fmt_pct_percent(progress)}")
        lines.extend([
            "",
            "Ventanas:",
            f"• Visible en Telegram hasta: {_fmt_dt(payload.get('telegram_valid_until'))}",
            f"• Evaluación de mercado hasta: {_fmt_dt(payload.get('evaluation_valid_until') or payload.get('valid_until'))}",
        ])
        if warnings:
            lines.extend(["", "Notas:"])
            for warning in warnings[:3]:
                lines.append(f"• {warning}")
        return "\n".join(lines)

    lines = [
        f"📍 SEGUIMIENTO — {payload.get('symbol', '—')} {payload.get('direction', '—')}",
        "",
        f"Estado general: {payload.get('state_label', '—')}",
        f"Estado operativo: {payload.get('entry_state_label', '—')}",
        f"Perfil seguido: {get_risk_profile_label(payload.get('selected_profile'))}",
        f"Resultado final: {payload.get('result_label', 'Aún sin cierre final')}",
        "",
        "Snapshot actual:",
        f"• Precio actual: {_fmt_num(payload.get('current_price'))}",
        f"• Entrada base: {_fmt_num(payload.get('entry_price'))}",
        f"• Zona de entrada: {_fmt_num(payload.get('entry_zone_low'))} → {_fmt_num(payload.get('entry_zone_high'))}",
        f"• Distancia actual a entrada: {_fmt_pct_fraction(payload.get('distance_to_entry_pct'))}",
        f"• Movimiento vs entrada: {_fmt_pct_fraction(payload.get('current_move_pct'))}",
        "",
        "Perfil operativo actual:",
        f"• SL: {_fmt_num(payload.get('stop_loss'))}",
        f"• TP1: {tp1}",
        f"• TP2: {tp2}",
        f"• Distancia a SL: {_fmt_pct_fraction(payload.get('stop_distance_pct'))}",
        f"• Distancia a TP1: {_fmt_pct_fraction(payload.get('tp1_distance_pct'))}",
        f"• Distancia a TP2: {_fmt_pct_fraction(payload.get('tp2_distance_pct'))}",
        "",
        "Lectura avanzada:",
        f"• En zona de entrada: {'Sí' if payload.get('in_entry_zone') else 'No'}",
        f"• TP1 ya alcanzado en precio actual: {'Sí' if payload.get('tp1_hit_now') else 'No'}",
        f"• TP2 ya alcanzado en precio actual: {'Sí' if payload.get('tp2_hit_now') else 'No'}",
        f"• SL roto en precio actual: {'Sí' if payload.get('stop_hit_now') else 'No'}",
        f"• Operable ahora: {'Sí' if payload.get('is_operable_now') else 'No'}",
    ]
    if progress is not None:
        lines.append(f"• Progreso hacia TP1: {_fmt_pct_percent(progress)}")
    lines.extend([
        "",
        "Recomendación:",
        f"• {payload.get('recommendation', '—')}",
        "",
        "Ventanas:",
        f"• Creada: {_fmt_dt(payload.get('created_at'))}",
        f"• Visible en Telegram hasta: {_fmt_dt(payload.get('telegram_valid_until'))}",
        f"• Evaluación de mercado hasta: {_fmt_dt(payload.get('evaluation_valid_until') or payload.get('valid_until'))}",
    ])
    if warnings:
        lines.extend(["", "Notas avanzadas:"])
        for warning in warnings[:5]:
            lines.append(f"• {warning}")
    return "\n".join(lines)


def build_signal_tracking_keyboard(signal_id: str, *, source: str = "live", plan: str = PLAN_FREE) -> InlineKeyboardMarkup:
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
            InlineKeyboardButton("🔄 Actualizar", callback_data=f"sig_trk:{source}:{signal_id}"),
            InlineKeyboardButton("📊 Ver análisis", callback_data=f"sig_an:{source}:{signal_id}"),
        ])
        if tier == "basic":
            rows.append([
                InlineKeyboardButton("📐 Calcular riesgo", callback_data=f"risk_calc:{source}:{signal_id}"),
                InlineKeyboardButton("💼 Ver planes", callback_data="plans"),
            ])
        else:
            rows.append([
                InlineKeyboardButton("📐 Calcular riesgo", callback_data=f"risk_calc:{source}:{signal_id}"),
                InlineKeyboardButton("🧭 Elegir perfil", callback_data=f"risk_pf:{source}:{signal_id}"),
            ])
    rows.append([InlineKeyboardButton("⬅️ Volver", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)
