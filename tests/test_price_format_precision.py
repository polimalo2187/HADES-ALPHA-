import sys
import types

import tests._bootstrap  # noqa: F401

if 'telegram' not in sys.modules:
    telegram = types.ModuleType('telegram')

    class InlineKeyboardButton:
        def __init__(self, *args, **kwargs):
            pass

    class InlineKeyboardMarkup:
        def __init__(self, *args, **kwargs):
            pass

    telegram.InlineKeyboardButton = InlineKeyboardButton
    telegram.InlineKeyboardMarkup = InlineKeyboardMarkup
    sys.modules['telegram'] = telegram

from app.signals import calculate_entry_zone
from app.tracking_ui import build_signal_tracking_text


def test_calculate_entry_zone_keeps_precision_for_low_price_assets():
    low, high = calculate_entry_zone(0.00856321)
    assert low != high
    assert round(low, 8) == low
    assert round(high, 8) == high
    assert abs(high - low) > 0


def test_tracking_text_uses_dynamic_price_precision_for_small_prices():
    payload = {
        "symbol": "TESTUSDT",
        "direction": "SHORT",
        "state_label": "ACTIVA",
        "entry_state_label": "ESPERANDO ENTRADA",
        "selected_profile": "moderado",
        "result_label": "Aún sin cierre final",
        "current_price": 0.00856321,
        "entry_price": 0.00851234,
        "stop_loss": 0.00869876,
        "take_profits": [0.00840123, 0.00830123],
        "distance_to_entry_pct": 0.001,
        "stop_distance_pct": 0.02,
        "tp1_distance_pct": 0.03,
        "recommendation": "Prueba",
        "is_operable_now": True,
    }
    text = build_signal_tracking_text(payload, plan="premium", language="es")
    assert "0.00856321" in text
    assert "0.00851234" in text
    assert "0.00869876" in text
    assert "0.00840123" in text
