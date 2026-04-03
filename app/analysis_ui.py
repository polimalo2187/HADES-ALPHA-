from __future__ import annotations

from datetime import datetime
from typing import Dict, List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM
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


def _fmt_fraction_pct(value) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "—"


def _signal_status(analysis: Dict, language: str | None = "es") -> str:
    now = datetime.utcnow()
    telegram_valid_until = analysis.get("telegram_valid_until")
    evaluation_valid_until = analysis.get("evaluation_valid_until") or analysis.get("valid_until")
    if isinstance(telegram_valid_until, datetime) and telegram_valid_until > now:
        return _t(language, "ACTIVA", "ACTIVE")
    if isinstance(evaluation_valid_until, datetime) and evaluation_valid_until > now:
        return _t(language, "CERRADA EN TELEGRAM / AÚN EVALUANDO", "CLOSED IN TELEGRAM / STILL EVALUATING")
    return _t(language, "FINALIZADA", "FINISHED")


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


def _fmt_component_row(item: object) -> str:
    if isinstance(item, dict):
        label = item.get("label") or item.get("name") or "—"
        raw_value = item.get("points")
        if raw_value is None:
            raw_value = item.get("score")
        try:
            return f"{label}: {float(raw_value):.2f}"
        except Exception:
            return str(label)
    if isinstance(item, (list, tuple)) and item:
        label = item[0]
        raw_value = item[1] if len(item) > 1 else None
        try:
            return f"{label}: {float(raw_value):.2f}"
        except Exception:
            return str(label)
    return str(item)


def build_signal_analysis_text(analysis: Dict, *, plan: str = PLAN_FREE, language: str = "es") -> str:
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
            _t(language, "📊 ANÁLISIS DE LA SEÑAL", "📊 SIGNAL ANALYSIS"),
            "",
            f"{_t(language, 'Par', 'Pair')}: {analysis.get('symbol', '—')}",
            f"{_t(language, 'Dirección', 'Direction')}: {analysis.get('direction', '—')}",
            f"{_t(language, 'Plan', 'Plan')}: {str(analysis.get('visibility') or '—').upper()}",
            f"{_t(language, 'Estado', 'Status')}: {_signal_status(analysis, language)}",
            "",
            _t(language, "Resumen rápido:", "Quick summary:"),
            f"• {_t(language, 'Score', 'Score')}: {analysis.get('normalized_score') if analysis.get('normalized_score') is not None else (analysis.get('score') if analysis.get('score') is not None else '—')}",
            f"• {_t(language, 'Timeframes', 'Timeframes')}: {' / '.join(analysis.get('timeframes') or []) or '—'}",
            f"• {_t(language, 'Perfil usado', 'Selected profile')}: {_profile_label(selected_profile, language)}",
            f"• {_t(language, 'Entrada', 'Entry')}: {_fmt_num(analysis.get('entry_price'))}",
            f"• SL: {_fmt_num(profile_payload.get('stop_loss'))}",
            f"• TP1: {_fmt_num(take_profits[0]) if len(take_profits) > 0 else '—'}",
            f"• {_t(language, 'Distancia a SL', 'Distance to SL')}: {_fmt_fraction_pct(analysis.get('selected_stop_distance_pct'))}",
            f"• ATR %: {_fmt_fraction_pct(analysis.get('atr_pct'))}",
            "",
            _t(language, "Lectura general:", "General reading:"),
            f"• {_t(language, 'Setup', 'Setup')}: {str(analysis.get('setup_group') or 'legacy').upper()}",
            f"• {_t(language, 'Esta es la versión resumida del análisis para tu plan.', 'This is the summarized analysis version for your plan.')}",
        ]
        if warnings:
            lines.extend(["", _t(language, "Notas:", "Notes:")])
            for item in warnings[:2]:
                lines.append(f"• {_fmt_component_row(item)}")
        lines.extend([
            "",
            _t(language, "💼 Sube a Plus para ver el análisis completo de la señal.", "💼 Upgrade to Plus to view the full signal analysis."),
        ])
        return "\n".join(lines).strip()

    lines = [
        _t(language, "📊 ANÁLISIS DE LA SEÑAL", "📊 SIGNAL ANALYSIS"),
        "",
        f"{_t(language, 'Par', 'Pair')}: {analysis.get('symbol', '—')}",
        f"{_t(language, 'Dirección', 'Direction')}: {analysis.get('direction', '—')}",
        f"{_t(language, 'Plan', 'Plan')}: {str(analysis.get('visibility') or '—').upper()}",
        f"{_t(language, 'Estado', 'Status')}: {_signal_status(analysis, language)}",
        f"{_t(language, 'Setup group', 'Setup group')}: {str(analysis.get('setup_group') or _t(language, 'legacy / no disponible', 'legacy / unavailable')).upper()}",
        "",
        _t(language, "Calidad de la señal:", "Signal quality:"),
        f"• {_t(language, 'Score raw', 'Raw score')}: {analysis.get('score') if analysis.get('score') is not None else '—'}",
        f"• {_t(language, 'Score normalizado', 'Normalized score')}: {analysis.get('normalized_score') if analysis.get('normalized_score') is not None else '—'}",
        f"• ATR %: {_fmt_fraction_pct(analysis.get('atr_pct'))}",
        f"• {_t(language, 'Timeframes', 'Timeframes')}: {' / '.join(analysis.get('timeframes') or []) or '—'}",
        "",
        _t(language, "Estructura operativa:", "Operational structure:"),
        f"• {_t(language, 'Perfil seleccionado', 'Selected profile')}: {_profile_label(selected_profile, language)}",
        f"• {_t(language, 'Entrada base', 'Base entry')}: {_fmt_num(analysis.get('entry_price'))}",
        f"• SL: {_fmt_num(profile_payload.get('stop_loss'))}",
        f"• TP1: {_fmt_num(take_profits[0]) if len(take_profits) > 0 else '—'}",
        f"• TP2: {_fmt_num(take_profits[1]) if len(take_profits) > 1 else '—'}",
        f"• {_t(language, 'Leverage sugerido', 'Suggested leverage')}: {profile_payload.get('leverage', '—')}",
        f"• {_t(language, 'Distancia a SL', 'Distance to SL')}: {_fmt_fraction_pct(analysis.get('selected_stop_distance_pct'))}",
        f"• {_t(language, 'Distancia a TP1', 'Distance to TP1')}: {_fmt_fraction_pct(analysis.get('selected_tp1_distance_pct'))}",
        f"• {_t(language, 'Distancia a TP2', 'Distance to TP2')}: {_fmt_fraction_pct(analysis.get('selected_tp2_distance_pct'))}",
        "",
        _t(language, "Ventanas de la señal:", "Signal windows:"),
        f"• {_t(language, 'Creada', 'Created')}: {_fmt_dt(analysis.get('created_at'))}",
        f"• {_t(language, 'Visible en Telegram hasta', 'Visible in Telegram until')}: {_fmt_dt(analysis.get('telegram_valid_until'))}",
        f"• {_t(language, 'Evaluación de mercado hasta', 'Market evaluation until')}: {_fmt_dt(analysis.get('evaluation_valid_until') or analysis.get('valid_until'))}",
        f"• {_t(language, 'Minutos de mercado', 'Market minutes')}: {analysis.get('market_validity_minutes') or '—'}",
    ]

    if tier == "advanced":
        lines.insert(7, f"{_t(language, 'Perfil de scoring', 'Scoring profile')}: {str(analysis.get('score_profile') or '—').upper()}")
        lines.insert(8, f"{_t(language, 'Calibración', 'Calibration')}: {analysis.get('score_calibration') or '—'}")

    if components:
        lines.extend(["", _t(language, "Lectura principal:", "Main reading:")])
        for item in components[:6]:
            lines.append(f"• {_fmt_component_row(item)}")

    if tier == "advanced":
        if raw_components:
            lines.extend(["", _t(language, "Componentes raw:", "Raw components:")])
            for item in raw_components[:6]:
                lines.append(f"• {_fmt_component_row(item)}")
        if normalized_components:
            lines.extend(["", _t(language, "Componentes normalizados:", "Normalized components:")])
            for item in normalized_components[:6]:
                lines.append(f"• {_fmt_component_row(item)}")

    if warnings:
        lines.extend(["", _t(language, "Notas:", "Notes:")])
        max_warn = 4 if tier == "advanced" else 2
        for item in warnings[:max_warn]:
            lines.append(f"• {_fmt_component_row(item)}")

    lines.extend([
        "",
        _t(language, "Invalidación práctica:", "Practical invalidation:"),
        f"• {_t(language, 'La idea pierde validez si el precio rompe el SL del perfil elegido.', 'The idea loses validity if price breaks the selected profile SL.')}",
        f"• {_t(language, 'Si la señal ya no está activa en mercado, este análisis se usa como referencia.', 'If the signal is no longer active in market, this analysis is for reference only.')}",
    ])

    if tier == "full":
        lines.extend([
            "",
            _t(language, "👑 Premium añade desglose interno completo del scoring y componentes avanzados.", "👑 Premium adds full internal scoring breakdown and advanced components."),
        ])

    return "\n".join(lines).strip()


def build_signal_analysis_keyboard(signal_id: str, *, source: str = "live", plan: str = PLAN_FREE, language: str = "es") -> InlineKeyboardMarkup:
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
            InlineKeyboardButton(_t(language, "📐 Calcular riesgo", "📐 Calculate risk"), callback_data=f"risk_calc:{source}:{signal_id}"),
            InlineKeyboardButton(_t(language, "📍 Seguimiento", "📍 Tracking"), callback_data=f"sig_trk:{source}:{signal_id}"),
        ])
        if tier == "basic":
            rows.append([InlineKeyboardButton(_t(language, "💼 Ver planes", "💼 View plans"), callback_data="plans")])
        else:
            rows.append([InlineKeyboardButton(_t(language, "🧭 Elegir perfil", "🧭 Choose profile"), callback_data=f"risk_pf:{source}:{signal_id}")])
    rows.append([InlineKeyboardButton(_t(language, "⬅️ Volver", "⬅️ Back"), callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)
