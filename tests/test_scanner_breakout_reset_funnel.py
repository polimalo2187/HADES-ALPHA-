import tests._bootstrap
import sys
import types

if 'telegram' not in sys.modules:
    telegram = types.ModuleType('telegram')
    class Bot: ...
    telegram.Bot = Bot
    sys.modules['telegram'] = telegram

import pymongo
errors_mod = sys.modules.get('pymongo.errors')
if errors_mod is not None and not hasattr(errors_mod, 'BulkWriteError'):
    class BulkWriteError(Exception):
        pass
    errors_mod.BulkWriteError = BulkWriteError
if not hasattr(pymongo, 'UpdateOne'):
    class UpdateOne:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
    pymongo.UpdateOne = UpdateOne

from app.scanner import _build_breakout_reset_funnel


def test_breakout_reset_funnel_maps_debug_lifecycle_counts():
    funnel = _build_breakout_reset_funnel(
        reject_totals={
            "router_risk_off_breakout_reset_override": 2,
            "router_symbol_continuation_breakout_override": 3,
            "strategy_router_try_breakout_reset_fallback": 1,
            "router_allowed_breakout_total": 6,
            "router_allowed_breakout_direct": 1,
            "router_allowed_breakout_override": 4,
            "router_allowed_breakout_fallback": 1,
            "symbol_regime_block_symbol_sweep_chop": 4,
            "symbol_regime_symbol_continuation_clean": 8,
            "breakout_setup_armed_waiting_reset": 5,
            "breakout_stateful_setup_loaded": 2,
            "breakout_stateful_extension_wait": 6,
            "breakout_waiting_live_reset": 11,
            "breakout_reset_rebounded_before_publish": 12,
            "breakout_reset_late": 13,
            "breakout_setup_expired": 1,
            "stateful_setup_back_inside_range": 2,
            "soft_gate_adx_strength": 3,
            "score_floor_free": 4,
            "atr_pct_hard": 7,
        },
        candidate_pool_by_strategy={"breakout_reset": 9},
        selected_by_strategy={"breakout_reset": 1},
        rejected_by_strategy={"breakout_reset": 30},
        attempts_by_strategy={"breakout_reset": 50},
    )

    assert funnel["router_allowed_attempts"] == 50
    assert funnel["router_allowed_direct"] == 1
    assert funnel["router_allowed_override"] == 4
    assert funnel["router_allowed_fallback"] == 1
    assert funnel["router_risk_off_overrides"] == 2
    assert funnel["router_symbol_continuation_overrides"] == 3
    assert funnel["router_sweep_fallbacks"] == 1
    assert funnel["blocked_symbol_regime"] == 4
    assert funnel["symbol_continuation_clean"] == 8
    assert funnel["setup_armed_waiting_reset"] == 5
    assert funnel["setup_loaded_waiting_reset"] == 2
    assert funnel["reset_extension_wait"] == 6
    assert funnel["waiting_live_reset"] == 11
    assert funnel["reset_rebounded_before_publish"] == 12
    assert funnel["reset_late_or_lost"] == 13
    assert funnel["reset_touched_candidates"] == 9
    assert funnel["published_selected"] == 1
    assert funnel["expired_no_reset"] == 1
    assert funnel["invalidated"] == 2
    assert funnel["soft_gate_hits"] == 3
    assert funnel["score_floor_rejected"] == 4
    assert funnel["hard_gate_rejected"] == 7
    assert funnel["hard_gate_atr_pct"] == 7
