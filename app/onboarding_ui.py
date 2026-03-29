from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.i18n import normalize_language, tr

ONBOARDING_VERSION = 2
SUPPORTED_LANGUAGES = {"es", "en"}


def build_language_selector_text() -> str:
    return tr("es", "common.language_selector_text")



def build_language_selector_keyboard(user_id: int | None = None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(tr("es", "common.language_es"), callback_data="lang:es"),
            InlineKeyboardButton(tr("es", "common.language_en"), callback_data="lang:en"),
        ]
    ])



def build_onboarding_text(screen: str, language: str) -> str:
    language = normalize_language(language)
    return tr(language, f"onboarding.{screen}", default=tr(language, "onboarding.home"))



def _btn(label: str, cb: str):
    return InlineKeyboardButton(label, callback_data=cb)



def build_onboarding_keyboard(screen: str, language: str) -> InlineKeyboardMarkup:
    language = normalize_language(language)
    labels = {
        "plans": tr(language, "onboarding.btn_plans"),
        "back": tr(language, "common.back"),
        "start": tr(language, "onboarding.btn_start"),
        "how": tr(language, "onboarding.btn_how"),
        "risk": tr(language, "onboarding.btn_risk"),
        "analysis": tr(language, "onboarding.btn_analysis"),
        "tracking": tr(language, "onboarding.btn_tracking"),
        "market": tr(language, "onboarding.btn_market"),
        "free": tr(language, "onboarding.btn_free"),
        "plus": tr(language, "onboarding.btn_plus"),
        "premium": tr(language, "onboarding.btn_premium"),
        "choose_plus": tr(language, "onboarding.btn_choose_plus"),
        "choose_premium": tr(language, "onboarding.btn_choose_premium"),
        "menu": tr(language, "onboarding.btn_menu"),
    }

    mapping = {
        "home": [
            [_btn(labels["start"], "ob:start")],
            [_btn(labels["plans"], "ob:plans"), _btn(labels["how"], "ob:how")],
        ],
        "how": [
            [_btn(labels["risk"], "ob:risk"), _btn(labels["analysis"], "ob:analysis")],
            [_btn(labels["tracking"], "ob:tracking"), _btn(labels["market"], "ob:market")],
            [_btn(labels["plans"], "ob:plans"), _btn(labels["back"], "ob:back:home")],
        ],
        "risk": [[_btn(labels["plans"], "ob:plans")], [_btn(labels["back"], "ob:back:how")]],
        "analysis": [[_btn(labels["plans"], "ob:plans")], [_btn(labels["back"], "ob:back:how")]],
        "tracking": [[_btn(labels["plans"], "ob:plans")], [_btn(labels["back"], "ob:back:how")]],
        "market": [[_btn(labels["plans"], "ob:plans")], [_btn(labels["back"], "ob:back:how")]],
        "plans": [
            [_btn(labels["free"], "ob:free")],
            [_btn(labels["plus"], "ob:plus"), _btn(labels["premium"], "ob:premium")],
            [_btn(labels["back"], "ob:back:home")],
        ],
        "plus": [
            [_btn(labels["choose_plus"], "plans")],
            [_btn(labels["premium"], "ob:premium")],
            [_btn(labels["back"], "ob:back:plans")],
        ],
        "premium": [
            [_btn(labels["choose_premium"], "plans")],
            [_btn(labels["plus"], "ob:plus")],
            [_btn(labels["back"], "ob:back:plans")],
        ],
        "free": [
            [_btn(labels["menu"], "ob:menu")],
            [_btn(labels["plans"], "ob:plans")],
            [_btn(labels["back"], "ob:back:plans")],
        ],
        "guide": [
            [_btn(labels["menu"], "ob:menu")],
            [_btn(labels["plans"], "ob:plans")],
            [_btn(labels["back"], "ob:back:home")],
        ],
    }
    return InlineKeyboardMarkup(mapping.get(screen, mapping["home"]))
