from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

SUPPORTED_LANGUAGES = {"es", "en"}
DEFAULT_LANGUAGE = "es"

MESSAGES: Dict[str, Dict[str, Any]] = {
    "es": {
        "common": {
            "back": "⬅️ Volver",
            "back_menu": "⬅️ Volver al menú",
            "refresh": "🔄 Actualizar",
            "cancel": "❌ Cancelar",
            "confirm": "✅ Confirmar",
            "language": "🌐 Idioma",
            "language_changed": "✅ Idioma actualizado correctamente.",
            "current_language": "Idioma actual",
            "language_select_title": "🌐 Selecciona tu idioma\n\nElige cómo quieres ver el bot a partir de ahora.",
            "language_selector_text": (
                "🌐 Elige tu idioma / Choose your language\n\n"
                "Selecciona cómo quieres leer el onboarding del bot.\n"
                "Choose how you want to read the bot onboarding."
            ),
            "language_es": "🇪🇸 Español",
            "language_en": "🇺🇸 English",
            "access_revoked": "🚫 Tu acceso al bot ha sido revocado.",
            "main_menu_short": "🚀 HADES MiniApp",
            "main_menu_full": (
                "🔥 {bot_name}\n\n"
                "Bienvenido a HADES, tu plataforma operativa de señales.\n\n"
                "Desde la MiniApp puedes acceder a:\n"
                "• señales en vivo\n"
                "• radar y mercado\n"
                "• watchlist profesional\n"
                "• historial y rendimiento\n"
                "• gestión de riesgo\n"
                "• planes, pagos y cuenta\n\n"
                "Telegram ahora funciona como canal de notificaciones y acceso rápido. "
                "Toda la operativa principal vive dentro de la MiniApp.\n\n"
                "Pulsa el botón de abajo para entrar."
            ),
            "admin_menu_short": "🛠 HADES Admin",
            "admin_menu_full": (
                "🔥 {bot_name}\n\n"
                "Bienvenido a HADES, tu plataforma operativa de señales.\n\n"
                "Desde la MiniApp puedes acceder a todo el ecosistema:\n"
                "• señales en vivo\n"
                "• radar y mercado\n"
                "• watchlist profesional\n"
                "• historial y rendimiento\n"
                "• gestión de riesgo\n"
                "• planes, pagos y cuenta\n\n"
                "Telegram ahora funciona como canal de notificaciones y acceso rápido. "
                "Toda la operativa principal vive dentro de la MiniApp.\n\n"
                "Pulsa el botón de abajo para entrar."
            ),
        },
        "menu": {
            "signals": "🚨 Señales",
            "radar": "📡 Radar",
            "performance": "🎯 Rendimiento",
            "movers": "🔥 Movers",
            "market": "📊 Mercado",
            "watchlist": "⭐ Watchlist",
            "alerts": "🔔 Alertas",
            "history": "🧾 Historial",
            "plans": "💼 Planes",
            "referrals": "👥 Referidos",
            "account": "👤 Mi cuenta",
            "support": "📩 Soporte",
            "open_app": "🚀 Abrir MiniApp",
            "admin_panel": "🛠 Panel Admin",
            "risk_menu": "⚙️ Gestión de riesgo",
            "admin_activate_plus": "➕ Activar plan PLUS",
            "admin_activate_premium": "👑 Activar plan PREMIUM",
            "admin_extend_plan": "⏳ Extender plan actual",
            "admin_stats": "📊 Estadísticas",
        },
        "onboarding": {
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
            "btn_plans": "💼 Ver planes",
            "btn_start": "🚀 Empezar",
            "btn_how": "ℹ️ Cómo funciona",
            "btn_risk": "📐 Riesgo",
            "btn_analysis": "📊 Análisis",
            "btn_tracking": "📍 Seguimiento",
            "btn_market": "🌍 Mercado",
            "btn_free": "🟢 Probar Free",
            "btn_plus": "🟡 Ver Plus",
            "btn_premium": "🔴 Ver Premium",
            "btn_choose_plus": "✅ Elegir Plus",
            "btn_choose_premium": "✅ Elegir Premium",
            "btn_menu": "🚀 Ir al menú",
            "screen_not_available": "Pantalla no disponible.",
        },
        "watchlist": {
            "empty_title": "⭐ Watchlist vacía.",
            "empty_hint": "Escribe un símbolo para añadir.",
            "examples": "Ejemplos válidos: BTCUSDT, ETHUSDT, SOLUSDT",
            "title": "⭐ WATCHLIST",
            "tip": "Tip: puedes escribir varios separados por coma. Ej: BTC, ETH, SOL",
            "pro_title": "📊 Watchlist PRO (Futures):",
            "load_error": "ℹ️ No pude cargar datos de mercado ahora mismo.",
            "updated_at": "🕒 Actualizado",
            "price": "Precio",
            "volume_24h": "Volumen 24h",
            "funding": "Funding",
            "open_interest": "Open Interest",
            "trend": "Tendencia",
            "momentum": "Momentum",
            "bullish": "Alcista",
            "bearish": "Bajista",
            "mixed": "Mixta",
            "strong": "Fuerte",
            "medium": "Medio",
            "weak": "Débil",
        },
        "account": {
            "title": "👤 MI CUENTA",
            "id": "ID",
            "plan": "Plan",
            "days_left": "📅 Días restantes",
            "expires": "⏳ Expira",
            "risk_settings": "Configuración de riesgo",
            "capital": "Capital",
            "risk_trade": "Riesgo/trade",
            "default_profile": "Perfil base",
            "exchange": "Exchange",
        },
    },
    "en": {
        "common": {
            "back": "⬅️ Back",
            "back_menu": "⬅️ Back to menu",
            "refresh": "🔄 Refresh",
            "cancel": "❌ Cancel",
            "confirm": "✅ Confirm",
            "language": "🌐 Language",
            "language_changed": "✅ Language updated successfully.",
            "current_language": "Current language",
            "language_select_title": "🌐 Select your language\n\nChoose how you want to see the bot from now on.",
            "language_selector_text": (
                "🌐 Elige tu idioma / Choose your language\n\n"
                "Selecciona cómo quieres leer el onboarding del bot.\n"
                "Choose how you want to read the bot onboarding."
            ),
            "language_es": "🇪🇸 Spanish",
            "language_en": "🇺🇸 English",
            "access_revoked": "🚫 Your access to the bot has been revoked.",
            "main_menu_short": "🚀 HADES MiniApp",
            "main_menu_full": (
                "🔥 {bot_name}\n\n"
                "Welcome to HADES, your operational signals platform.\n\n"
                "Inside the MiniApp you can access:\n"
                "• live signals\n"
                "• radar and market\n"
                "• pro watchlist\n"
                "• history and performance\n"
                "• risk management\n"
                "• plans, payments, and account\n\n"
                "Telegram now works as your notification channel and quick entry point. "
                "All core operations now live inside the MiniApp.\n\n"
                "Tap the button below to enter."
            ),
            "admin_menu_short": "🛠 HADES Admin",
            "admin_menu_full": (
                "🛠 {bot_name} | Admin access\n\n"
                "The product now operates from the MiniApp.\n\n"
                "Inside the MiniApp you also have:\n"
                "• admin panel\n"
                "• manual plan activation\n"
                "• tier metrics\n"
                "• operational health\n"
                "• user moderation\n\n"
                "Telegram remains only as a quick entry point and notification channel.\n\n"
                "Tap the button below to open the MiniApp."
            ),
        },
        "menu": {
            "signals": "🚨 Signals",
            "radar": "📡 Radar",
            "performance": "🎯 Performance",
            "movers": "🔥 Movers",
            "market": "📊 Market",
            "watchlist": "⭐ Watchlist",
            "alerts": "🔔 Alerts",
            "history": "🧾 History",
            "plans": "💼 Plans",
            "referrals": "👥 Referrals",
            "account": "👤 My account",
            "support": "📩 Support",
            "open_app": "🚀 Open MiniApp",
            "admin_panel": "🛠 Admin Panel",
            "risk_menu": "⚙️ Risk management",
            "admin_activate_plus": "➕ Activate PLUS plan",
            "admin_activate_premium": "👑 Activate PREMIUM plan",
            "admin_extend_plan": "⏳ Extend current plan",
            "admin_stats": "📊 Statistics",
        },
        "onboarding": {
            "home": "🔥 Welcome to HADES Alpha V2\n\nA futures signals bot inside Telegram with:\n\n• signals by plan\n• signal analysis\n• risk calculator\n• signal tracking\n• market context\n\nHere you do not just receive a signal.\nYou can also understand it, measure it, and follow it.",
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
            "btn_plans": "💼 View plans",
            "btn_start": "🚀 Start",
            "btn_how": "ℹ️ How it works",
            "btn_risk": "📐 Risk",
            "btn_analysis": "📊 Analysis",
            "btn_tracking": "📍 Tracking",
            "btn_market": "🌍 Market",
            "btn_free": "🟢 Try Free",
            "btn_plus": "🟡 View Plus",
            "btn_premium": "🔴 View Premium",
            "btn_choose_plus": "✅ Choose Plus",
            "btn_choose_premium": "✅ Choose Premium",
            "btn_menu": "🚀 Go to menu",
            "screen_not_available": "Screen not available.",
        },
        "watchlist": {
            "empty_title": "⭐ Empty watchlist.",
            "empty_hint": "Send a symbol to add it.",
            "examples": "Valid examples: BTCUSDT, ETHUSDT, SOLUSDT",
            "title": "⭐ WATCHLIST",
            "tip": "Tip: you can write several separated by commas. Example: BTC, ETH, SOL",
            "pro_title": "📊 PRO Watchlist (Futures):",
            "load_error": "ℹ️ I couldn't load market data right now.",
            "updated_at": "🕒 Updated",
            "price": "Price",
            "volume_24h": "24h Volume",
            "funding": "Funding",
            "open_interest": "Open Interest",
            "trend": "Trend",
            "momentum": "Momentum",
            "bullish": "Bullish",
            "bearish": "Bearish",
            "mixed": "Mixed",
            "strong": "Strong",
            "medium": "Medium",
            "weak": "Weak",
        },
        "account": {
            "title": "👤 MY ACCOUNT",
            "id": "ID",
            "plan": "Plan",
            "days_left": "📅 Days remaining",
            "expires": "⏳ Expires",
            "risk_settings": "Risk configuration",
            "capital": "Capital",
            "risk_trade": "Risk/trade",
            "default_profile": "Default profile",
            "exchange": "Exchange",
        },
    },
}


def normalize_language(value: str | None) -> str:
    value = (value or DEFAULT_LANGUAGE).strip().lower()
    if value.startswith("en"):
        return "en"
    return DEFAULT_LANGUAGE


def _resolve_path(tree: Dict[str, Any], dotted_key: str) -> Any:
    current: Any = tree
    for part in dotted_key.split('.'):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def tr(language: str | None, key: str, default: str | None = None, **kwargs) -> str:
    lang = normalize_language(language)
    value = _resolve_path(MESSAGES.get(lang, MESSAGES[DEFAULT_LANGUAGE]), key)
    if value is None:
        value = _resolve_path(MESSAGES[DEFAULT_LANGUAGE], key)
    if value is None:
        value = default if default is not None else key
    if isinstance(value, str) and kwargs:
        return value.format(**kwargs)
    return str(value)


def tr_pair(language: str | None, es: str, en: str, **kwargs) -> str:
    template = en if normalize_language(language) == "en" else es
    return template.format(**kwargs) if kwargs else template


def get_catalog(language: str | None) -> Dict[str, Any]:
    return deepcopy(MESSAGES.get(normalize_language(language), MESSAGES[DEFAULT_LANGUAGE]))


def language_label(language: str | None, target_language: str | None = None) -> str:
    lang = normalize_language(language)
    target = normalize_language(target_language) if target_language else lang
    key = "common.language_en" if target == "en" else "common.language_es"
    return tr(lang, key)
