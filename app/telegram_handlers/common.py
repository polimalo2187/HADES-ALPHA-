import logging
from urllib.parse import quote

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import get_admin_whatsapps
from app.i18n import language_label, normalize_language, tr, tr_pair
from app.onboarding_ui import (
    ONBOARDING_VERSION,
    build_language_selector_keyboard,
    build_language_selector_text,
    build_onboarding_keyboard,
    build_onboarding_text,
)
from app.plans import PLAN_FREE, PLAN_PLUS, PLAN_PREMIUM

logger = logging.getLogger(__name__)


def _get_user_language(user: dict | None) -> str:
    return normalize_language((user or {}).get("language"))



def _tr(language: str | None, es: str, en: str, **kwargs) -> str:
    return tr_pair(_get_user_language({"language": language}), es, en, **kwargs)



def _needs_onboarding(user: dict | None) -> bool:
    if not user:
        return True
    language = normalize_language(user.get("language"))
    version = int(user.get("onboarding_version") or 0)
    completed = bool(user.get("onboarding_completed"))
    return language not in {"es", "en"} or version < ONBOARDING_VERSION or not completed



def _banned_message(language: str | None) -> str:
    return tr(language, "common.access_revoked")



def _admin_panel_keyboard(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_tr(language, "➕ Activar plan", "➕ Activate plan"), callback_data="admin_activate_plan")],
        [InlineKeyboardButton(_tr(language, "🗑 Eliminar usuario", "🗑 Delete user"), callback_data="admin_delete_user")],
        [InlineKeyboardButton(tr(language, "common.back"), callback_data="back_menu")],
    ])



def build_language_settings_keyboard(language: str | None, include_back: bool = True) -> InlineKeyboardMarkup:
    current = _get_user_language({"language": language})
    es_label = language_label(language, "es") + (" ✅" if current == "es" else "")
    en_label = language_label(language, "en") + (" ✅" if current == "en" else "")
    rows = [[
        InlineKeyboardButton(es_label, callback_data="set_lang:es"),
        InlineKeyboardButton(en_label, callback_data="set_lang:en"),
    ]]
    if include_back:
        rows.append([InlineKeyboardButton(tr(language, "common.back"), callback_data="my_account")])
    return InlineKeyboardMarkup(rows)



async def _show_language_selector_message(message, user_id: int):
    await message.reply_text(
        build_language_selector_text(),
        reply_markup=build_language_selector_keyboard(user_id=user_id),
    )


async def _show_onboarding_screen(query, user: dict, screen: str):
    language = _get_user_language(user)
    await query.edit_message_text(
        build_onboarding_text(screen, language),
        reply_markup=build_onboarding_keyboard(screen, language),
    )


async def _show_onboarding_screen_message(message, user: dict, screen: str):
    language = _get_user_language(user)
    await message.reply_text(
        build_onboarding_text(screen, language),
        reply_markup=build_onboarding_keyboard(screen, language),
    )



def format_whatsapp_contacts():
    whatsapps = get_admin_whatsapps()
    if not whatsapps:
        return "WhatsApp: (no configurado)"
    if len(whatsapps) == 1:
        return f"WhatsApp: {whatsapps[0]}"
    return "WhatsApps:\n- " + "\n- ".join(whatsapps)



def _wa_link(phone: str, message: str) -> str:
    phone_str = str(phone)
    if phone_str.startswith("http://") or phone_str.startswith("https://"):
        if "text=" in phone_str:
            return phone_str
        sep = "&" if "?" in phone_str else "?"
        return f"{phone_str}{sep}text={quote(message)}"
    clean = "".join(ch for ch in phone_str if ch.isdigit() or ch == "+")
    if clean.startswith("+"):
        clean = clean[1:]
    return f"https://wa.me/{clean}?text={quote(message)}"



def parse_ref_code(start_param: str) -> int | None:
    if not start_param:
        return None
    if start_param.startswith("ref_"):
        try:
            return int(start_param.split("_")[1])
        except ValueError:
            return None
    return None



def _plan_rank(plan: str) -> int:
    if plan == PLAN_PREMIUM:
        return 3
    if plan == PLAN_PLUS:
        return 2
    return 1
