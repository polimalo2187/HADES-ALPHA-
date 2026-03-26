from __future__ import annotations

import math
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.database import users_collection


SUPPORTED_PROFILE_NAMES = ("conservador", "moderado", "agresivo")
SUPPORTED_ENTRY_MODES = ("limit_wait", "limit_fast", "limit_unknown")
SUPPORTED_EXCHANGES = (
    "binance",
    "lbank",
    "coinw",
    "weex",
    "coinex",
    "bitunix",
    "mexc",
    "other",
)

# Valores pensados para futuros USDT y para órdenes límite.
# Son presets editables, no verdades absolutas. En varios exchanges pueden
# cambiar por región, nivel VIP o campañas temporales.
EXCHANGE_FEE_PRESETS: Dict[str, Dict[str, float]] = {
    "binance": {"maker": 0.02, "taker": 0.05},
    "lbank": {"maker": 0.02, "taker": 0.06},
    "coinw": {"maker": 0.01, "taker": 0.06},
    "weex": {"maker": 0.02, "taker": 0.08},
    "coinex": {"maker": 0.03, "taker": 0.05},
    "bitunix": {"maker": 0.02, "taker": 0.06},
    "mexc": {"maker": 0.02, "taker": 0.06},
    "other": {"maker": 0.02, "taker": 0.06},
}

EXCHANGE_ALIASES: Dict[str, str] = {
    "binance": "binance",
    "coinw": "coinw",
    "coin ex": "coinex",
    "coinex": "coinex",
    "coin net": "coinex",
    "coinnet": "coinex",
    "weex": "weex",
    "web": "weex",
    "lbank": "lbank",
    "elbank": "lbank",
    "elebank": "lbank",
    "bitunix": "bitunix",
    "bituni": "bitunix",
    "mexc": "mexc",
    "med": "mexc",
    "otro": "other",
    "other": "other",
}

ENTRY_MODE_LABELS: Dict[str, str] = {
    "limit_wait": "Límite esperando precio",
    "limit_fast": "Límite agresiva para entrar rápido",
    "limit_unknown": "Límite / no seguro",
}

ENTRY_MODE_TO_FEE_KIND: Dict[str, str] = {
    "limit_wait": "maker",
    "limit_fast": "taker",
    "limit_unknown": "taker",
}

DEFAULT_RISK_PROFILE: Dict[str, Any] = {
    "capital_usdt": 0.0,
    "risk_percent": 1.0,
    "exchange": "binance",
    "fee_percent_per_side": 0.02,
    "slippage_percent": 0.03,
    "default_profile": "moderado",
    "default_leverage": 35.0,
    "entry_mode": "limit_wait",
    "updated_at": None,
}


class RiskConfigurationError(ValueError):
    """Configuración inválida para cálculo de riesgo."""


class SignalProfileError(ValueError):
    """Perfil de señal inválido o incompleto."""


class SignalRiskError(ValueError):
    """La señal no se puede calcular con los parámetros dados."""


def normalize_exchange_name(value: Optional[str]) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return DEFAULT_RISK_PROFILE["exchange"]
    return EXCHANGE_ALIASES.get(raw, raw if raw in SUPPORTED_EXCHANGES else "other")


def normalize_entry_mode(value: Optional[str]) -> str:
    raw = (value or "").strip().lower()
    if raw in SUPPORTED_ENTRY_MODES:
        return raw
    return DEFAULT_RISK_PROFILE["entry_mode"]


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    if isinstance(value, bool):
        raise RiskConfigurationError("Valor booleano inválido donde se esperaba número")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().lower().replace("usdt", "").replace("x", "")
        cleaned = cleaned.replace("%", "").replace(",", ".")
        if not cleaned:
            return float(default)
        try:
            return float(cleaned)
        except ValueError as exc:
            raise RiskConfigurationError(f"No pude convertir '{value}' a número") from exc
    raise RiskConfigurationError(f"Tipo de valor no soportado: {type(value)!r}")


def parse_leverage_hint(value: Any, fallback: float = 35.0) -> float:
    if value is None:
        return float(fallback)
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed > 0 else float(fallback)
    if not isinstance(value, str):
        return float(fallback)

    text = value.strip().lower().replace(" ", "")
    if not text:
        return float(fallback)
    if "-" in text:
        left, right = text.split("-", 1)
        try:
            low = _to_float(left, default=fallback)
            high = _to_float(right, default=fallback)
            if low > 0 and high > 0:
                return round((low + high) / 2.0, 2)
        except Exception:
            return float(fallback)
    try:
        parsed = _to_float(text, default=fallback)
        return parsed if parsed > 0 else float(fallback)
    except Exception:
        return float(fallback)


def get_exchange_fee_preset(exchange: Optional[str], entry_mode: Optional[str]) -> Dict[str, float]:
    exchange_key = normalize_exchange_name(exchange)
    mode_key = normalize_entry_mode(entry_mode)
    preset = EXCHANGE_FEE_PRESETS.get(exchange_key, EXCHANGE_FEE_PRESETS["other"])
    fee_kind = ENTRY_MODE_TO_FEE_KIND.get(mode_key, "maker")
    fee_percent = float(preset[fee_kind])

    if mode_key == "limit_wait":
        slippage = 0.03
    elif mode_key == "limit_fast":
        slippage = 0.06
    else:
        slippage = 0.07

    return {
        "exchange": exchange_key,
        "entry_mode": mode_key,
        "fee_percent_per_side": round(fee_percent, 4),
        "slippage_percent": round(slippage, 4),
        "fee_kind": fee_kind,
    }


def build_default_risk_profile(
    *,
    exchange: Optional[str] = None,
    entry_mode: Optional[str] = None,
) -> Dict[str, Any]:
    profile = deepcopy(DEFAULT_RISK_PROFILE)
    preset = get_exchange_fee_preset(exchange or profile["exchange"], entry_mode or profile["entry_mode"])
    profile.update(
        exchange=preset["exchange"],
        entry_mode=preset["entry_mode"],
        fee_percent_per_side=preset["fee_percent_per_side"],
        slippage_percent=preset["slippage_percent"],
    )
    return profile


def normalize_risk_profile(profile: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = build_default_risk_profile(
        exchange=(profile or {}).get("exchange"),
        entry_mode=(profile or {}).get("entry_mode"),
    )

    if not profile:
        return base

    normalized = deepcopy(base)
    normalized["capital_usdt"] = round(max(0.0, _to_float(profile.get("capital_usdt"), default=0.0)), 4)
    normalized["risk_percent"] = round(max(0.01, _to_float(profile.get("risk_percent"), default=1.0)), 4)
    normalized["fee_percent_per_side"] = round(max(0.0, _to_float(profile.get("fee_percent_per_side"), default=base["fee_percent_per_side"])), 4)
    normalized["slippage_percent"] = round(max(0.0, _to_float(profile.get("slippage_percent"), default=base["slippage_percent"])), 4)
    normalized["default_leverage"] = round(max(1.0, _to_float(profile.get("default_leverage"), default=35.0)), 2)

    default_profile = str(profile.get("default_profile") or base["default_profile"]).strip().lower()
    normalized["default_profile"] = default_profile if default_profile in SUPPORTED_PROFILE_NAMES else base["default_profile"]
    normalized["exchange"] = normalize_exchange_name(profile.get("exchange") or base["exchange"])
    normalized["entry_mode"] = normalize_entry_mode(profile.get("entry_mode") or base["entry_mode"])
    normalized["updated_at"] = profile.get("updated_at")
    return normalized


def ensure_risk_profile_ready(profile: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    normalized = normalize_risk_profile(profile)
    capital = float(normalized.get("capital_usdt") or 0.0)
    risk_percent = float(normalized.get("risk_percent") or 0.0)
    leverage = float(normalized.get("default_leverage") or 0.0)

    if capital <= 0:
        raise RiskConfigurationError("Debes configurar un capital mayor que cero en Gestión de riesgo")
    if risk_percent <= 0:
        raise RiskConfigurationError("Debes configurar un riesgo por trade mayor que cero")
    if leverage <= 0:
        raise RiskConfigurationError("Debes configurar un apalancamiento por defecto mayor que cero")
    return normalized


def get_user_risk_profile(user_id: int) -> Dict[str, Any]:
    user_doc = users_collection().find_one({"user_id": int(user_id)}, {"risk_profile": 1}) or {}
    return normalize_risk_profile(user_doc.get("risk_profile"))


def save_user_risk_profile(user_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(patch, dict):
        raise RiskConfigurationError("El patch del perfil de riesgo debe ser un diccionario")

    current = get_user_risk_profile(int(user_id))
    merged = deepcopy(current)
    merged.update(patch)
    normalized = normalize_risk_profile(merged)
    normalized["updated_at"] = datetime.utcnow()

    users_collection().update_one(
        {"user_id": int(user_id)},
        {"$set": {"risk_profile": normalized}},
        upsert=False,
    )
    return normalized


def get_risk_profile_label(profile_name: str) -> str:
    profile_name = (profile_name or "").strip().lower()
    if profile_name == "conservador":
        return "Conservador"
    if profile_name == "moderado":
        return "Moderado"
    if profile_name == "agresivo":
        return "Agresivo"
    return "Desconocido"


def _extract_profile_dict(user_signal: Dict[str, Any], profile_name: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    profiles = user_signal.get("profiles") or {}
    if not isinstance(profiles, dict) or not profiles:
        raise SignalProfileError("La señal no contiene perfiles operativos")

    selected_name = (profile_name or "").strip().lower() or ""
    if selected_name not in SUPPORTED_PROFILE_NAMES:
        fallback = user_signal.get("risk_profile_selected") or user_signal.get("default_profile") or "moderado"
        selected_name = fallback if fallback in profiles else "moderado"
    if selected_name not in profiles:
        for candidate in SUPPORTED_PROFILE_NAMES:
            if candidate in profiles:
                selected_name = candidate
                break
        else:
            raise SignalProfileError("No encontré un perfil válido dentro de la señal")

    signal_profile = profiles[selected_name] or {}
    if not isinstance(signal_profile, dict):
        raise SignalProfileError("Perfil de señal mal formado")
    return selected_name, signal_profile


def extract_signal_trade_params(
    user_signal: Dict[str, Any],
    *,
    profile_name: Optional[str] = None,
    override_leverage: Optional[float] = None,
) -> Dict[str, Any]:
    if not isinstance(user_signal, dict):
        raise SignalProfileError("La señal debe ser un diccionario")

    selected_name, signal_profile = _extract_profile_dict(user_signal, profile_name)

    try:
        entry_price = float(user_signal["entry_price"])
        stop_loss = float(signal_profile["stop_loss"])
    except Exception as exc:
        raise SignalProfileError("La señal no tiene entry/SL válidos") from exc

    raw_take_profits = signal_profile.get("take_profits") or []
    take_profits = [round(float(tp), 8) for tp in raw_take_profits if tp is not None]
    if not take_profits:
        raise SignalProfileError("La señal no tiene take profits válidos")

    direction = str(user_signal.get("direction") or "").upper().strip()
    if direction not in {"LONG", "SHORT"}:
        raise SignalProfileError("Dirección de señal inválida")

    signal_leverage_hint = signal_profile.get("leverage") or user_signal.get("leverage")
    signal_leverage_default = parse_leverage_hint(signal_leverage_hint, fallback=35.0)
    leverage = float(override_leverage) if override_leverage else signal_leverage_default
    leverage = max(1.0, leverage)

    return {
        "signal_id": user_signal.get("signal_id") or str(user_signal.get("_id") or ""),
        "symbol": str(user_signal.get("symbol") or "").upper(),
        "direction": direction,
        "entry_price": round(entry_price, 8),
        "stop_loss": round(stop_loss, 8),
        "take_profits": take_profits,
        "profile_name": selected_name,
        "profile_label": get_risk_profile_label(selected_name),
        "signal_leverage_hint": signal_leverage_hint,
        "signal_leverage_default": round(signal_leverage_default, 2),
        "leverage": round(leverage, 2),
        "created_at": user_signal.get("created_at"),
        "telegram_valid_until": user_signal.get("telegram_valid_until"),
        "evaluation_valid_until": user_signal.get("evaluation_valid_until") or user_signal.get("valid_until"),
        "visibility": user_signal.get("visibility"),
        "timeframes": list(user_signal.get("timeframes") or []),
        "score": user_signal.get("score"),
    }


def _validate_signal_geometry(direction: str, entry_price: float, stop_loss: float) -> None:
    if entry_price <= 0 or stop_loss <= 0:
        raise SignalRiskError("Entry y stop loss deben ser mayores que cero")
    if direction == "LONG" and stop_loss >= entry_price:
        raise SignalRiskError("En LONG el stop loss debe estar por debajo de la entrada")
    if direction == "SHORT" and stop_loss <= entry_price:
        raise SignalRiskError("En SHORT el stop loss debe estar por encima de la entrada")


def _distance_pct(direction: str, start_price: float, end_price: float) -> float:
    if start_price <= 0 or end_price <= 0:
        return 0.0
    if direction == "LONG":
        return (end_price - start_price) / start_price
    return (start_price - end_price) / start_price


def _round_money(value: float) -> float:
    return round(float(value), 4)


def _round_pct_decimal(value: float) -> float:
    return round(float(value), 8)


def calculate_signal_risk(
    *,
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profits: List[float],
    capital_usdt: float,
    risk_percent: float,
    leverage: float,
    fee_percent_per_side: float,
    slippage_percent: float,
) -> Dict[str, Any]:
    direction = str(direction or "").upper().strip()
    if direction not in {"LONG", "SHORT"}:
        raise SignalRiskError("Dirección inválida para cálculo de riesgo")

    entry_price = float(entry_price)
    stop_loss = float(stop_loss)
    leverage = float(leverage)
    capital_usdt = float(capital_usdt)
    risk_percent = float(risk_percent)
    fee_percent_per_side = float(fee_percent_per_side)
    slippage_percent = float(slippage_percent)

    if capital_usdt <= 0:
        raise SignalRiskError("El capital debe ser mayor que cero")
    if risk_percent <= 0:
        raise SignalRiskError("El riesgo % debe ser mayor que cero")
    if leverage <= 0:
        raise SignalRiskError("El apalancamiento debe ser mayor que cero")
    if not take_profits:
        raise SignalRiskError("Debes indicar al menos un take profit")

    _validate_signal_geometry(direction, entry_price, stop_loss)

    risk_amount_usdt = capital_usdt * (risk_percent / 100.0)
    stop_distance_pct = abs(entry_price - stop_loss) / entry_price
    fee_roundtrip_pct = (fee_percent_per_side * 2.0) / 100.0
    slippage_decimal = slippage_percent / 100.0
    effective_loss_pct = stop_distance_pct + fee_roundtrip_pct + slippage_decimal

    if effective_loss_pct <= 0:
        raise SignalRiskError("El riesgo efectivo calculado es inválido")

    position_notional_usdt = risk_amount_usdt / effective_loss_pct
    required_margin_usdt = position_notional_usdt / leverage
    quantity_estimate = position_notional_usdt / entry_price

    warnings: List[str] = []
    if required_margin_usdt > capital_usdt:
        warnings.append("El margen estimado requerido supera el capital configurado.")
    if stop_distance_pct >= 0.03:
        warnings.append("La distancia al stop es amplia; revisa si el setup sigue teniendo sentido.")
    if effective_loss_pct >= 0.025:
        warnings.append("El riesgo efectivo total es alto para una entrada estándar.")

    tp_results: List[Dict[str, Any]] = []
    for idx, tp in enumerate(take_profits, start=1):
        tp_price = float(tp)
        move_pct = _distance_pct(direction, entry_price, tp_price)
        if move_pct <= 0:
            continue
        net_profit_pct = move_pct - fee_roundtrip_pct - slippage_decimal
        gross_profit_usdt = position_notional_usdt * move_pct
        net_profit_usdt = position_notional_usdt * max(0.0, net_profit_pct)
        rr_net = net_profit_usdt / risk_amount_usdt if risk_amount_usdt > 0 else 0.0

        tp_results.append(
            {
                "name": f"TP{idx}",
                "price": round(tp_price, 8),
                "distance_pct": _round_pct_decimal(move_pct),
                "gross_profit_usdt": _round_money(gross_profit_usdt),
                "net_profit_usdt": _round_money(net_profit_usdt),
                "rr_net": round(rr_net, 4),
            }
        )

    if not tp_results:
        warnings.append("No se pudo calcular beneficio neto porque los TP no son coherentes con la dirección.")

    result = {
        "direction": direction,
        "entry_price": round(entry_price, 8),
        "stop_loss": round(stop_loss, 8),
        "take_profits": [round(float(tp), 8) for tp in take_profits],
        "capital_usdt": _round_money(capital_usdt),
        "risk_percent": round(risk_percent, 4),
        "risk_amount_usdt": _round_money(risk_amount_usdt),
        "leverage": round(leverage, 2),
        "fee_percent_per_side": round(fee_percent_per_side, 4),
        "slippage_percent": round(slippage_percent, 4),
        "stop_distance_pct": _round_pct_decimal(stop_distance_pct),
        "fee_roundtrip_pct": _round_pct_decimal(fee_roundtrip_pct),
        "slippage_decimal": _round_pct_decimal(slippage_decimal),
        "effective_loss_pct": _round_pct_decimal(effective_loss_pct),
        "position_notional_usdt": _round_money(position_notional_usdt),
        "required_margin_usdt": _round_money(required_margin_usdt),
        "quantity_estimate": round(quantity_estimate, 8),
        "loss_at_stop_usdt": _round_money(risk_amount_usdt),
        "tp_results": tp_results,
        "warnings": warnings,
        "is_operable": required_margin_usdt <= capital_usdt and len(tp_results) > 0,
    }
    return result


def _ensure_trade_params_calculable(trade_params: Dict[str, Any]) -> None:
    direction = str(trade_params.get("direction") or "").upper().strip()
    entry_price = float(trade_params.get("entry_price") or 0.0)
    stop_loss = float(trade_params.get("stop_loss") or 0.0)
    take_profits = list(trade_params.get("take_profits") or [])

    _validate_signal_geometry(direction, entry_price, stop_loss)

    coherent_tps = [tp for tp in take_profits if _distance_pct(direction, entry_price, float(tp)) > 0]
    if not coherent_tps:
        raise SignalRiskError("El perfil no tiene take profits coherentes con la dirección de la señal")


def _resolve_best_trade_params(
    user_signal: Dict[str, Any],
    *,
    requested_profile_name: Optional[str],
    override_leverage: Optional[float],
) -> Tuple[Dict[str, Any], str, List[Dict[str, str]]]:
    requested = (requested_profile_name or "").strip().lower() or ""
    ordered_candidates: List[str] = []

    if requested in SUPPORTED_PROFILE_NAMES:
        ordered_candidates.append(requested)

    fallback = str(user_signal.get("risk_profile_selected") or user_signal.get("default_profile") or "").strip().lower()
    if fallback in SUPPORTED_PROFILE_NAMES and fallback not in ordered_candidates:
        ordered_candidates.append(fallback)

    for candidate in SUPPORTED_PROFILE_NAMES:
        if candidate not in ordered_candidates:
            ordered_candidates.append(candidate)

    errors: List[Dict[str, str]] = []
    for candidate in ordered_candidates:
        try:
            trade_params = extract_signal_trade_params(
                user_signal,
                profile_name=candidate,
                override_leverage=override_leverage,
            )
            _ensure_trade_params_calculable(trade_params)
            return trade_params, candidate, errors
        except (SignalProfileError, SignalRiskError) as exc:
            errors.append({
                "profile_name": candidate,
                "profile_label": get_risk_profile_label(candidate),
                "error": str(exc),
            })

    details = "; ".join(f"{item['profile_label']}: {item['error']}" for item in errors[:3])
    if details:
        raise SignalRiskError(f"La señal tiene perfiles inconsistentes para calcular riesgo. {details}")
    raise SignalRiskError("La señal no tiene un perfil operativo coherente para calcular riesgo")


def calculate_signal_risk_from_user_signal(
    user_signal: Dict[str, Any],
    *,
    risk_profile: Optional[Dict[str, Any]] = None,
    profile_name: Optional[str] = None,
    override_leverage: Optional[float] = None,
) -> Dict[str, Any]:
    normalized_risk_profile = ensure_risk_profile_ready(risk_profile)
    requested_profile = (profile_name or normalized_risk_profile["default_profile"] or "moderado").strip().lower()

    trade_params, resolved_profile_name, fallback_errors = _resolve_best_trade_params(
        user_signal,
        requested_profile_name=requested_profile,
        override_leverage=override_leverage or normalized_risk_profile["default_leverage"],
    )

    calc = calculate_signal_risk(
        direction=trade_params["direction"],
        entry_price=trade_params["entry_price"],
        stop_loss=trade_params["stop_loss"],
        take_profits=trade_params["take_profits"],
        capital_usdt=normalized_risk_profile["capital_usdt"],
        risk_percent=normalized_risk_profile["risk_percent"],
        leverage=trade_params["leverage"],
        fee_percent_per_side=normalized_risk_profile["fee_percent_per_side"],
        slippage_percent=normalized_risk_profile["slippage_percent"],
    )

    calc["exchange"] = normalized_risk_profile["exchange"]
    calc["entry_mode"] = normalized_risk_profile["entry_mode"]
    calc["entry_mode_label"] = ENTRY_MODE_LABELS.get(normalized_risk_profile["entry_mode"], ENTRY_MODE_LABELS["limit_wait"])
    calc["profile_name"] = trade_params["profile_name"]
    calc["profile_label"] = trade_params["profile_label"]
    calc["signal_id"] = trade_params["signal_id"]
    calc["symbol"] = trade_params["symbol"]
    calc["visibility"] = trade_params["visibility"]
    calc["timeframes"] = trade_params["timeframes"]
    calc["score"] = trade_params["score"]
    calc["signal_leverage_default"] = trade_params["signal_leverage_default"]
    calc["signal_leverage_hint"] = trade_params["signal_leverage_hint"]
    calc["created_at"] = trade_params["created_at"]
    calc["telegram_valid_until"] = trade_params["telegram_valid_until"]
    calc["evaluation_valid_until"] = trade_params["evaluation_valid_until"]
    calc["risk_profile"] = normalized_risk_profile
    calc["requested_profile_name"] = requested_profile
    calc["requested_profile_label"] = get_risk_profile_label(requested_profile)
    calc["profile_fallback_used"] = resolved_profile_name != requested_profile
    calc["profile_resolution_errors"] = fallback_errors

    if calc["profile_fallback_used"]:
        calc.setdefault("warnings", [])
        calc["warnings"].insert(
            0,
            f"El perfil {get_risk_profile_label(requested_profile)} no era calculable en esta señal; se usó {trade_params['profile_label']}."
        )

    return calc


def is_signal_active_for_entry(user_signal: Dict[str, Any], *, now: Optional[datetime] = None) -> bool:
    now = now or datetime.utcnow()
    telegram_valid_until = user_signal.get("telegram_valid_until")
    if isinstance(telegram_valid_until, datetime):
        return telegram_valid_until > now
    return False


def build_risk_diagnostics(calc: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics = {
        "margin_usage_pct": 0.0,
        "capital_buffer_usdt": 0.0,
        "tp_count": len(calc.get("tp_results") or []),
        "best_rr_net": 0.0,
        "risk_band": "normal",
    }

    capital = float(calc.get("capital_usdt") or 0.0)
    margin = float(calc.get("required_margin_usdt") or 0.0)
    if capital > 0:
        diagnostics["margin_usage_pct"] = round((margin / capital) * 100.0, 4)
        diagnostics["capital_buffer_usdt"] = _round_money(capital - margin)

    tp_results = calc.get("tp_results") or []
    if tp_results:
        diagnostics["best_rr_net"] = round(max(float(tp.get("rr_net") or 0.0) for tp in tp_results), 4)

    effective_loss_pct = float(calc.get("effective_loss_pct") or 0.0)
    if effective_loss_pct >= 0.03:
        diagnostics["risk_band"] = "alto"
    elif effective_loss_pct >= 0.02:
        diagnostics["risk_band"] = "medio"
    else:
        diagnostics["risk_band"] = "normal"

    return diagnostics


def build_risk_preview_from_user_signal(
    user_signal: Dict[str, Any],
    *,
    risk_profile: Optional[Dict[str, Any]] = None,
    profile_name: Optional[str] = None,
    override_leverage: Optional[float] = None,
) -> Dict[str, Any]:
    calc = calculate_signal_risk_from_user_signal(
        user_signal,
        risk_profile=risk_profile,
        profile_name=profile_name,
        override_leverage=override_leverage,
    )
    calc["diagnostics"] = build_risk_diagnostics(calc)
    calc["signal_active_for_entry"] = is_signal_active_for_entry(user_signal)
    return calc


__all__ = [
    "DEFAULT_RISK_PROFILE",
    "ENTRY_MODE_LABELS",
    "EXCHANGE_FEE_PRESETS",
    "RiskConfigurationError",
    "SignalProfileError",
    "SignalRiskError",
    "build_default_risk_profile",
    "ensure_risk_profile_ready",
    "build_risk_diagnostics",
    "build_risk_preview_from_user_signal",
    "calculate_signal_risk",
    "calculate_signal_risk_from_user_signal",
    "extract_signal_trade_params",
    "get_exchange_fee_preset",
    "get_risk_profile_label",
    "get_user_risk_profile",
    "is_signal_active_for_entry",
    "normalize_entry_mode",
    "normalize_exchange_name",
    "normalize_risk_profile",
    "parse_leverage_hint",
    "save_user_risk_profile",
]
