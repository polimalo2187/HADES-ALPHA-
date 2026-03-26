from telegram import InlineKeyboardButton, InlineKeyboardMarkup

ONBOARDING_VERSION = 2
SUPPORTED_LANGUAGES = {"es", "en"}


def normalize_language(value: str | None) -> str:
    value = (value or "es").strip().lower()
    if value.startswith("en"):
        return "en"
    return "es"


def build_language_selector_text() -> str:
    return (
        "🌐 Elige tu idioma / Choose your language\n\n"
        "Selecciona cómo quieres leer el onboarding del bot.\n"
        "Choose how you want to read the bot onboarding."
    )


def build_language_selector_keyboard(user_id: int | None = None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇪🇸 Español", callback_data="lang:es"),
            InlineKeyboardButton("🇺🇸 English", callback_data="lang:en"),
        ]
    ])


ONBOARDING_TEXTS = {
    "es": {
        "home": "🔥 Bienvenido a HADES Alpha V2\n\nBot de señales de futuros dentro de Telegram con:\n\n• señales por plan\n• análisis de cada señal\n• calculadora de riesgo\n• seguimiento operativo\n• estado de mercado\n\nAquí no solo recibes una señal.\nTambién puedes entenderla, medirla y seguirla.",
        "how": "ℹ️ Cómo funciona HADES Alpha V2\n\nDentro del bot puedes hacer 4 cosas principales:\n\n📐 Calcular riesgo\nPara estimar cuánto arriesgar antes de entrar.\n\n📊 Ver análisis\nPara entender la estructura detrás de la señal.\n\n📍 Hacer seguimiento\nPara revisar si la señal sigue operable o no.\n\n🌍 Ver el mercado\nPara leer el contexto general antes de operar.\n\nTodo funciona directamente dentro de Telegram.",
        "risk": "📐 Calculadora de riesgo\n\nEsta herramienta te ayuda a estimar:\n\n• cuánto arriesgas\n• tamaño recomendado\n• pérdida estimada en SL\n• beneficio estimado en TP\n• relación riesgo/beneficio\n\nEstá pensada para que no operes sin estructura.",
        "analysis": "📊 Análisis de señal\n\nEl análisis te muestra más contexto sobre la operación:\n\n• dirección\n• score\n• timeframes\n• entry, SL y TP\n• estructura general de la señal\n• lectura del setup\n\nSirve para entender mejor por qué apareció esa oportunidad.",
        "tracking": "📍 Seguimiento de señal\n\nEl seguimiento te permite revisar:\n\n• si la señal sigue activa\n• si todavía es operable\n• si ya se alejó de entrada\n• cómo va evolucionando\n• recomendación operativa general\n\nSirve para acompañar la operación después de emitida.",
        "market": "🌍 Estado de mercado\n\nEsta herramienta te da una lectura del contexto general:\n\n• sesgo\n• régimen\n• volatilidad\n• entorno\n• lado favorecido\n\nSirve para entender si el mercado está más favorable para operar o si conviene actuar con más cautela.",
        "plans": "💼 Planes disponibles\n\n🟢 FREE\nIdeal para conocer el ecosistema del bot.\n\nIncluye:\n• señales Free\n• riesgo básico\n• análisis resumido\n• seguimiento básico\n• mercado resumido\n\n🟡 PLUS\nEl plan más sólido para operar con estructura.\n\nIncluye:\n• señales Plus\n• riesgo completo\n• análisis completo\n• seguimiento completo\n• mercado completo\n\n🔴 PREMIUM\nLa experiencia más profunda del bot.\n\nIncluye:\n• señales Premium\n• riesgo completo\n• análisis avanzado\n• seguimiento avanzado\n• mercado extendido",
        "plus": "🟡 Plan Plus\n\nPensado para usuarios que quieren una experiencia operativa completa.\n\nIncluye:\n• señales Plus\n• calculadora de riesgo completa\n• análisis completo\n• seguimiento estándar\n• estado de mercado completo\n\nEs el plan recomendado para la mayoría de usuarios.",
        "premium": "🔴 Plan Premium\n\nPensado para usuarios que quieren la experiencia más completa del bot.\n\nIncluye:\n• señales Premium\n• herramientas completas\n• análisis avanzado\n• seguimiento avanzado\n• lectura de mercado más profunda\n\nEs el plan para quien busca el máximo nivel dentro del ecosistema.",
        "free": "🟢 Plan Free\n\nEl plan Free te permite entrar al ecosistema del bot y entender su estructura base.\n\nIncluye:\n• señales Free\n• calculadora básica\n• análisis resumido\n• seguimiento básico\n• mercado resumido\n\nEs la mejor forma de conocer el bot antes de subir de nivel.",
        "guide": "📌 Cómo aprovechar mejor el bot\n\nRecomendación de uso:\n\n1. revisa la señal\n2. abre el análisis\n3. calcula el riesgo antes de entrar\n4. consulta el estado del mercado\n5. usa el seguimiento para acompañar la operación\n\n⚠️ Ninguna señal elimina el riesgo.\nUsa siempre gestión de capital.",
    },
    "en": {
        "home": "🔥 Welcome to HADES Alpha V2\n\nA futures signals bot inside Telegram with:\n\n• signals by plan\n• signal analysis\n• risk calculator\n• signal tracking\n• market context\n\nHere you do not just receive a signal.\nYou can also understand it, measure the risk, and follow it.",
        "how": "ℹ️ How HADES Alpha V2 works\n\nInside the bot, you can do 4 main things:\n\n📐 Risk Calculator\nEstimate how much to risk before entering.\n\n📊 Signal Analysis\nUnderstand the structure behind the signal.\n\n📍 Signal Tracking\nCheck whether the signal is still valid or already too extended.\n\n🌍 Market State\nRead the overall market context before trading.\n\nEverything works directly inside Telegram.",
        "risk": "📐 Risk Calculator\n\nThis tool helps you estimate:\n\n• how much you are risking\n• recommended size\n• estimated loss at stop loss\n• estimated profit at take profit\n• risk/reward ratio\n\nIt is designed so you do not trade without structure.",
        "analysis": "📊 Signal Analysis\n\nThe analysis screen gives you more context about the trade:\n\n• direction\n• score\n• timeframes\n• entry, SL and TP\n• overall signal structure\n• setup reading\n\nIt helps you understand why that opportunity appeared.",
        "tracking": "📍 Signal Tracking\n\nTracking lets you review:\n\n• whether the signal is still active\n• whether it is still tradable\n• whether price is already too far from entry\n• how the setup is evolving\n• general operational guidance\n\nIt is built to help you follow the trade after the signal is sent.",
        "market": "🌍 Market State\n\nThis tool gives you a broader reading of the market:\n\n• bias\n• regime\n• volatility\n• environment\n• favored side\n\nIt helps you understand whether market conditions are supportive or whether extra caution is needed.",
        "plans": "💼 Available Plans\n\n🟢 FREE\nA good way to explore the bot ecosystem.\n\nIncludes:\n• Free signals\n• basic risk calculator\n• summarized analysis\n• basic tracking\n• summarized market view\n\n🟡 PLUS\nThe strongest plan for traders who want a structured experience.\n\nIncludes:\n• Plus signals\n• full risk calculator\n• full analysis\n• full tracking\n• full market view\n\n🔴 PREMIUM\nThe deepest experience inside the bot.\n\nIncludes:\n• Premium signals\n• full risk tools\n• advanced analysis\n• advanced tracking\n• extended market view",
        "plus": "🟡 Plus Plan\n\nBuilt for users who want a complete operational experience.\n\nIncludes:\n• Plus signals\n• full risk calculator\n• full signal analysis\n• standard tracking\n• full market state\n\nThis is the recommended plan for most users.",
        "premium": "🔴 Premium Plan\n\nBuilt for users who want the most complete experience inside the bot.\n\nIncludes:\n• Premium signals\n• full tools\n• advanced analysis\n• advanced tracking\n• deeper market reading\n\nThis is the plan for users who want the highest level inside the ecosystem.",
        "free": "🟢 Free Plan\n\nThe Free plan lets you enter the bot ecosystem and understand the core structure.\n\nIncludes:\n• Free signals\n• basic risk calculator\n• summarized analysis\n• basic tracking\n• summarized market state\n\nIt is the best way to explore the bot before upgrading.",
        "guide": "📌 How to get the most out of the bot\n\nRecommended flow:\n\n1. review the signal\n2. open the analysis\n3. calculate the risk before entering\n4. check the market state\n5. use tracking to follow the trade\n\n⚠️ No signal removes risk.\nAlways use proper capital management.",
    },
}


def build_onboarding_text(screen: str, language: str) -> str:
    language = normalize_language(language)
    return ONBOARDING_TEXTS.get(language, ONBOARDING_TEXTS["es"]).get(screen, ONBOARDING_TEXTS[language]["home"])


def _btn(label: str, cb: str):
    return InlineKeyboardButton(label, callback_data=cb)


def build_onboarding_keyboard(screen: str, language: str) -> InlineKeyboardMarkup:
    language = normalize_language(language)
    es = language == "es"
    labels = {
        "plans": "💼 Ver planes" if es else "💼 View plans",
        "back": "⬅️ Volver" if es else "⬅️ Back",
        "start": "🚀 Empezar" if es else "🚀 Start",
        "how": "ℹ️ Cómo funciona" if es else "ℹ️ How it works",
        "risk": "📐 Riesgo" if es else "📐 Risk",
        "analysis": "📊 Análisis" if es else "📊 Analysis",
        "tracking": "📍 Seguimiento" if es else "📍 Tracking",
        "market": "🌍 Mercado" if es else "🌍 Market",
        "free": "🟢 Probar Free" if es else "🟢 Try Free",
        "plus": "🟡 Ver Plus" if es else "🟡 View Plus",
        "premium": "🔴 Ver Premium" if es else "🔴 View Premium",
        "choose_plus": "✅ Elegir Plus" if es else "✅ Choose Plus",
        "choose_premium": "✅ Elegir Premium" if es else "✅ Choose Premium",
        "menu": "🚀 Ir al menú" if es else "🚀 Go to menu",
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
