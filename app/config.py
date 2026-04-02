# app/config.py

import os
from typing import List
from urllib.parse import urlparse


DEFAULT_BOT_DISPLAY_NAME = "HADES ALPHA V2"
DEFAULT_BOT_USERNAME = "HADES_ALPHA_bot"
_ALLOWED_RUNTIME_ROLES = {"web", "bot", "bot_ui", "signal_worker", "scheduler"}


# ======================================================
# BOT / MARCA
# ======================================================

def get_bot_display_name() -> str:
    return os.getenv("BOT_DISPLAY_NAME", DEFAULT_BOT_DISPLAY_NAME).strip() or DEFAULT_BOT_DISPLAY_NAME



def get_bot_username() -> str:
    value = os.getenv("BOT_USERNAME", DEFAULT_BOT_USERNAME).strip()
    return value.lstrip("@") or DEFAULT_BOT_USERNAME


# ======================================================
# ENTORNO / RUNTIME
# ======================================================

def get_environment_name() -> str:
    return os.getenv("ENVIRONMENT", "production").strip().lower() or "production"



def is_development_environment() -> bool:
    return get_environment_name() in {"dev", "development", "local", "test", "testing", "staging"}



def get_runtime_role() -> str:
    raw = os.getenv("APP_RUNTIME_ROLE", "").strip().lower()
    aliases = {
        "miniapp": "web",
        "api": "web",
        "telegram": "bot",
        "polling": "bot",
        "telegram_ui": "bot_ui",
        "worker": "signal_worker",
        "scanner": "signal_worker",
        "cron": "scheduler",
    }
    normalized = aliases.get(raw, raw)
    if normalized in _ALLOWED_RUNTIME_ROLES:
        return normalized
    return "web" if is_mini_app_enabled() else "bot"

def get_bot_token() -> str:
    return os.getenv("BOT_TOKEN", "").strip()


def get_mongodb_uri() -> str:
    return os.getenv("MONGODB_URI", "").strip()


def get_database_name() -> str:
    return os.getenv("DATABASE_NAME", "").strip()


def get_runtime_required_env_vars(role: str | None = None) -> List[str]:
    runtime_role = role or get_runtime_role()
    required = ["MONGODB_URI", "DATABASE_NAME"]

    if runtime_role in {"bot", "bot_ui", "signal_worker"}:
        required.append("BOT_TOKEN")

    if runtime_role == "web":
        required.append("BOT_TOKEN")

    return required


def get_runtime_configuration_errors(role: str | None = None) -> List[str]:
    runtime_role = role or get_runtime_role()
    errors: List[str] = []

    values = {
        "BOT_TOKEN": get_bot_token(),
        "MONGODB_URI": get_mongodb_uri(),
        "DATABASE_NAME": get_database_name(),
        "MINI_APP_URL": get_mini_app_url(),
        "MINI_APP_SESSION_SECRET": os.getenv("MINI_APP_SESSION_SECRET", "").strip(),
    }

    missing = [name for name in get_runtime_required_env_vars(runtime_role) if not values.get(name)]
    if missing:
        errors.append(
            f"runtime_role={runtime_role}: faltan variables requeridas: {', '.join(missing)}"
        )

    if runtime_role == "web" and not get_mini_app_url() and not is_development_environment():
        errors.append("runtime_role=web: MINI_APP_URL es obligatoria fuera de desarrollo")

    if runtime_role == "web" and not values["MINI_APP_SESSION_SECRET"] and not is_development_environment():
        errors.append(
            "runtime_role=web: MINI_APP_SESSION_SECRET debe configurarse explícitamente fuera de desarrollo"
        )

    if runtime_role == "web" and is_mini_app_dev_auth_enabled() and get_mini_app_dev_user_id() is None:
        errors.append("runtime_role=web: MINI_APP_DEV_USER_ID es obligatorio cuando MINI_APP_ALLOW_DEV_AUTH=true")

    return errors


def validate_runtime_configuration(role: str | None = None) -> None:
    errors = get_runtime_configuration_errors(role=role)
    if errors:
        raise RuntimeError(" | ".join(errors))


# ======================================================
# ADMINISTRADORES DEL SISTEMA (TELEGRAM USER_ID)
# ======================================================

def _parse_admin_user_ids() -> List[int]:
    admin_ids: List[int] = []

    raw_csv = os.getenv("ADMIN_USER_IDS", "").strip()
    if raw_csv:
        for chunk in raw_csv.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                admin_id = int(chunk)
            except ValueError:
                continue
            if admin_id > 0 and admin_id not in admin_ids:
                admin_ids.append(admin_id)

    for env_name in ("ADMIN_USER_ID_1", "ADMIN_USER_ID_2"):
        raw_value = os.getenv(env_name, "").strip()
        if not raw_value:
            continue
        try:
            admin_id = int(raw_value)
        except ValueError:
            continue
        if admin_id > 0 and admin_id not in admin_ids:
            admin_ids.append(admin_id)

    return admin_ids


ADMIN_USER_IDS: List[int] = _parse_admin_user_ids()



def is_admin(user_id: int) -> bool:
    """
    Verifica si un user_id es administrador.
    """
    return user_id in ADMIN_USER_IDS


# ======================================================
# CONTACTOS DE WHATSAPP PARA ACTIVAR PLANES
# ======================================================

def _parse_admin_whatsapps() -> List[str]:
    values: List[str] = []

    raw_csv = os.getenv("ADMIN_WHATSAPPS", "").strip()
    if raw_csv:
        for item in raw_csv.split(","):
            item = item.strip()
            if item and item not in values:
                values.append(item)

    for env_name in ("ADMIN_WHATSAPP_1", "ADMIN_WHATSAPP_2"):
        value = os.getenv(env_name, "").strip()
        if value and value not in values:
            values.append(value)

    return values


ADMIN_WHATSAPPS: List[str] = _parse_admin_whatsapps()



def get_admin_whatsapps() -> List[str]:
    """
    Retorna la lista de WhatsApps válidos (no vacíos).
    """
    return [w for w in ADMIN_WHATSAPPS if w]


# ======================================================
# PAGOS AUTOMÁTICOS USDT BEP-20
# ======================================================

def get_payment_network() -> str:
    return os.getenv("PAYMENT_NETWORK", "bep20").strip().lower() or "bep20"



def get_payment_token_symbol() -> str:
    return os.getenv("PAYMENT_TOKEN_SYMBOL", "USDT").strip().upper() or "USDT"



def get_payment_token_contract() -> str:
    return os.getenv("PAYMENT_TOKEN_CONTRACT", "").strip().lower()



def get_payment_receiver_address() -> str:
    return os.getenv("PAYMENT_RECEIVER_ADDRESS", "").strip().lower()



def get_bsc_rpc_http_url() -> str:
    return os.getenv("BSC_RPC_HTTP_URL", "").strip()



def get_payment_min_confirmations() -> int:
    try:
        return max(int(os.getenv("PAYMENT_MIN_CONFIRMATIONS", "3")), 1)
    except Exception:
        return 3



def get_payment_order_ttl_minutes() -> int:
    try:
        return max(int(os.getenv("PAYMENT_ORDER_TTL_MINUTES", "30")), 5)
    except Exception:
        return 30


def get_payment_unique_max_delta() -> float:
    try:
        value = float(os.getenv("PAYMENT_UNIQUE_MAX_DELTA", "0.150"))
    except Exception:
        return 0.150
    return max(0.001, min(value, 0.150))



def get_payment_token_decimals() -> int:
    try:
        return max(int(os.getenv("PAYMENT_TOKEN_DECIMALS", "18")), 0)
    except Exception:
        return 18



def get_payment_lookback_blocks() -> int:
    try:
        return max(int(os.getenv("PAYMENT_LOOKBACK_BLOCKS", "2500")), 100)
    except Exception:
        return 2500



def get_payment_configuration_status() -> dict:
    checks = [
        {
            "key": "BSC_RPC_HTTP_URL",
            "label": "RPC BSC",
            "value_present": bool(get_bsc_rpc_http_url()),
        },
        {
            "key": "PAYMENT_TOKEN_CONTRACT",
            "label": "Contrato del token",
            "value_present": bool(get_payment_token_contract()),
        },
        {
            "key": "PAYMENT_RECEIVER_ADDRESS",
            "label": "Wallet receptora",
            "value_present": bool(get_payment_receiver_address()),
        },
    ]
    missing = [item["key"] for item in checks if not item["value_present"]]
    return {
        "ready": not missing,
        "checks": checks,
        "missing_keys": missing,
    }



def is_payment_configuration_ready() -> bool:
    return bool(get_payment_configuration_status().get("ready"))


# ======================================================
# MINI APP / WEB APP
# ======================================================

def get_mini_app_url() -> str:
    return os.getenv("MINI_APP_URL", "").strip()



def is_mini_app_enabled() -> bool:
    value = os.getenv("ENABLE_MINI_APP_SERVER", "false").strip().lower()
    return value in {"1", "true", "yes", "on"}



def get_mini_app_session_secret() -> str:
    value = os.getenv("MINI_APP_SESSION_SECRET", "").strip()
    if value:
        return value
    return get_bot_token()



def get_mini_app_session_ttl_seconds() -> int:
    try:
        return max(int(os.getenv("MINI_APP_SESSION_TTL_SECONDS", "43200")), 900)
    except Exception:
        return 43200



def get_mini_app_dev_user_id() -> int | None:
    raw = os.getenv("MINI_APP_DEV_USER_ID", "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except Exception:
        return None
    return value if value > 0 else None



def is_mini_app_dev_auth_enabled() -> bool:
    raw = os.getenv("MINI_APP_ALLOW_DEV_AUTH", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return is_development_environment()
    return False



def _normalize_origin(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw == "*":
        return "*"
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return ""
    return raw.rstrip("/")



def get_mini_app_cors_origins() -> List[str]:
    explicit = os.getenv("MINI_APP_CORS_ORIGINS", "").strip()
    origins: List[str] = []

    if explicit:
        for item in explicit.split(","):
            normalized = _normalize_origin(item)
            if normalized and normalized not in origins:
                origins.append(normalized)
        return origins

    mini_app_url = _normalize_origin(get_mini_app_url())
    if mini_app_url and mini_app_url not in origins:
        origins.append(mini_app_url)

    if is_mini_app_dev_auth_enabled():
        for local_origin in (
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ):
            if local_origin not in origins:
                origins.append(local_origin)

    return origins
