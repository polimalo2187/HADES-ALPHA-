from __future__ import annotations

from datetime import datetime
from typing import Dict, List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
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


def _fmt_fraction_pct(value) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "—"


def _signal_status(analysis: Dict) -> str:
    now = datetime.utcnow()
    telegram_valid_until = analysis.get("telegram_valid_until")
    evaluation_valid_until = analysis.get("evaluation_valid_until") or analysis.get("valid_until")
    if isinstance(telegram_valid_until, datetime) and telegram_valid_until > now:
        return "ACTIVA"
    if isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now:
        return "CERRADA EN TELEGRAM / AÚN EVALUANDO"
    return "FINALIZADA"


def _safe_list(value) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]
    return [str(value)]


def _analysis_feature_tier(plan: str) -> str:
    plan = str(plan or PLAN_FREE)
    if plan == PLAN_PREMIUM:
        return "advanced"
    if plan == PLAN_PLUS:
        return "full"
    return "basic"


def build_signal_analysis_text(analysis: Dict, *, plan: str = PLAN_FREE) -> str:
    tier = _analysis_feature_tier(plan)
    selected_profile = str(analysis.get("selected_profile") or "moderado")
    profile_payload = analysis.get("selected_profile_payload") or {}
    take_profits = profile_payload.get("take_profits") or []
    components = _safe_list(analysis.get("components"))
    raw_components = _safe_list(analysis.get("raw_components"))
    normalized_components = _safe_list(analysis.get("normalized_components"))
    warnings = _safe_list(analysis.get("warnings"))

    if tier == "basic":
        lines = [
            "📊 ANÁLISIS DE LA SEÑAL",
            "",
            f"Par: {analysis.get('symbol', '—')}",
            f"Dirección: {analysis.get('direction', '—')}",
            f"Plan: {str(analysis.get('visibility') or '—').upper()}",
            f"Estado: {_signal_status(analysis)}",
            "",
            "Resumen rápido:",
            f"• Score: {analysis.get('normalized_score') if analysis.get('normalized_score') is not None else (analysis.get('score') if analysis.get('score') is not None else '—')}",
            f"• Timeframes: {' / '.join(analysis.get('timeframes') or []) or '—'}",
            f"• Perfil usado: {get_risk_profile_label(selected_profile)}",
            f"• Entrada: {_fmt_num(analysis.get('entry_price'))}",
            f"• SL: {_fmt_num(profile_payload.get('stop_loss'))}",
            f"• TP1: {_fmt_num(take_profits[0]) if len(take_profits) > 0 else '—'}",
            f"• Distancia a SL: {_fmt_fraction_pct(analysis.get('selected_stop_distance_pct'))}",
            f"• ATR %: {_fmt_fraction_pct(analysis.get('atr_pct'))}",
            "",
            "Lectura general:",
            f"• Setup: {str(analysis.get('setup_group') or 'legacy').upper()}",
            "• Esta es la versión resumida del análisis para tu plan.",
        ]
        if warnings:
            lines.extend(["", "Notas:"])
            for item in warnings[:2]:
                lines.append(f"• {item}")
        lines.extend([
            "",
            "💼 Sube a Plus para ver el análisis completo de la señal.",
        ])
        return "\n".join(lines).strip()

    lines = [
        "📊 ANÁLISIS DE LA SEÑAL",
        "",
        f"Par: {analysis.get('symbol', '—')}",
        f"Dirección: {analysis.get('direction', '—')}",
        f"Plan: {str(analysis.get('visibility') or '—').upper()}",
        f"Estado: {_signal_status(analysis)}",
        f"Setup group: {str(analysis.get('setup_group') or 'legacy / no disponible').upper()}",
        "",
        "Calidad de la señal:",
        f"• Score raw: {analysis.get('score') if analysis.get('score') is not None else '—'}",
        f"• Score normalizado: {analysis.get('normalized_score') if analysis.get('normalized_score') is not None else '—'}",
        f"• ATR %: {_fmt_fraction_pct(analysis.get('atr_pct'))}",
        f"• Timeframes: {' / '.join(analysis.get('timeframes') or []) or '—'}",
        "",
        "Estructura operativa:",
        f"• Perfil seleccionado: {get_risk_profile_label(selected_profile)}",
        f"• Entrada base: {_fmt_num(analysis.get('entry_price'))}",
        f"• SL: {_fmt_num(profile_payload.get('stop_loss'))}",
        f"• TP1: {_fmt_num(take_profits[0]) if len(take_profits) > 0 else '—'}",
        f"• TP2: {_fmt_num(take_profits[1]) if len(take_profits) > 1 else '—'}",
        f"• Leverage sugerido: {profile_payload.get('leverage', '—')}",
        f"• Distancia a SL: {_fmt_fraction_pct(analysis.get('selected_stop_distance_pct'))}",
        f"• Distancia a TP1: {_fmt_fraction_pct(analysis.get('selected_tp1_distance_pct'))}",
        f"• Distancia a TP2: {_fmt_fraction_pct(analysis.get('selected_tp2_distance_pct'))}",
        "",
        "Ventanas de la señal:",
        f"• Creada: {_fmt_dt(analysis.get('created_at'))}",
        f"• Visible en Telegram hasta: {_fmt_dt(analysis.get('telegram_valid_until'))}",
        f"• Evaluación de mercado hasta: {_fmt_dt(analysis.get('evaluation_valid_until') or analysis.get('valid_until'))}",
        f"• Minutos de mercado: {analysis.get('market_validity_minutes') or '—'}",
    ]

    if tier == "advanced":
        lines.insert(7, f"Perfil de scoring: {str(analysis.get('score_profile') or '—').upper()}")
        lines.insert(8, f"Calibración: {analysis.get('score_calibration') or '—'}")

    if components:
        lines.extend(["", "Lectura principal:"])
        for item in components[:6]:
            lines.append(f"• {item}")

    if tier == "advanced":
        if raw_components:
            lines.extend(["", "Componentes raw:"])
            for item in raw_components[:6]:
                lines.append(f"• {item}")
        if normalized_components:
            lines.extend(["", "Componentes normalizados:"])
            for item in normalized_components[:6]:
                lines.append(f"• {item}")

    if warnings:
        lines.extend(["", "Notas:"])
        max_warn = 4 if tier == "advanced" else 2
        for item in warnings[:max_warn]:
            lines.append(f"• {item}")

    lines.extend([
        "",
        "Invalidación práctica:",
        "• La idea pierde validez si el precio rompe el SL del perfil elegido.",
        "• Si la señal ya no está activa en mercado, este análisis se usa como referencia.",
    ])

    if tier == "full":
        lines.extend([
            "",
            "👑 Premium añade desglose interno completo del scoring y componentes avanzados.",
        ])

    return "\n".join(lines).strip()


def build_signal_analysis_keyboard(signal_id: str, *, source: str = "live", plan: str = PLAN_FREE) -> InlineKeyboardMarkup:
    if source == "live":
        back_cb = f"sig_detail:{signal_id}"
    elif source == "hist":
        back_cb = f"hist_detail:{signal_id}"
    else:
        back_cb = "risk_menu"

    tier = _analysis_feature_tier(plan)
    rows = []
    if signal_id:
        rows.append([
            InlineKeyboardButton("📐 Calcular riesgo", callback_data=f"risk_calc:{source}:{signal_id}"),
            InlineKeyboardButton("📍 Seguimiento", callback_data=f"sig_trk:{source}:{signal_id}"),
        ])
        if tier == "basic":
            rows.append([InlineKeyboardButton("💼 Ver planes", callback_data="plans")])
        else:
            rows.append([InlineKeyboardButton("🧭 Elegir perfil", callback_data=f"risk_pf:{source}:{signal_id}")])
    rows.append([InlineKeyboardButton("⬅️ Volver", callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)
