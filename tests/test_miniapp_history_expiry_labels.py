import tests._bootstrap

from app.miniapp.service import _serialize_history


def test_history_serializer_exposes_expiry_reason_labels():
    payload = _serialize_history({
        "signal_id": "sig-1",
        "symbol": "BTCUSDT",
        "direction": "SHORT",
        "result": "expired",
        "resolution": "expired_no_fill",
        "entry_touched": False,
        "tp1_progress_max_pct": 0.0,
    })

    assert payload["expiry_type"] == "no_fill"
    assert payload["expiry_label"] == "Expirada: no llegó al entry"
