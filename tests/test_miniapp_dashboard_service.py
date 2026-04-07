import tests._bootstrap  # noqa: F401

import unittest
from unittest.mock import patch

from app.miniapp.service import build_dashboard_payload


class MiniAppDashboardServiceTests(unittest.TestCase):
    def test_dashboard_payload_tolerates_bad_signal_serialization(self):
        user = {"user_id": 123, "plan": "premium"}
        with patch("app.miniapp.service.get_performance_snapshot", return_value={}), \
             patch("app.miniapp.service.user_signals_collection") as user_signals, \
             patch("app.miniapp.service.get_history_entries_for_user", return_value=[]), \
             patch("app.miniapp.service.get_active_payment_order_for_user", return_value=None), \
             patch("app.miniapp.service.watchlists_collection") as watchlists, \
             patch("app.miniapp.service._serialize_signal", side_effect=ValueError("bad row")):
            user_signals.return_value.find.return_value.sort.return_value.limit.side_effect = [[{"signal_id": "a1"}], [{"signal_id": "a1"}]]
            user_signals.return_value.count_documents.return_value = 1
            watchlists.return_value.find_one.return_value = {"symbols": []}
            payload = build_dashboard_payload(user)
        self.assertEqual(payload["recent_signals"], [])
        self.assertEqual(payload["active_signals_count"], 1)

    def test_dashboard_payload_tolerates_bad_order_serialization(self):
        user = {"user_id": 123, "plan": "premium"}
        with patch("app.miniapp.service.get_performance_snapshot", return_value={}), \
             patch("app.miniapp.service.user_signals_collection") as user_signals, \
             patch("app.miniapp.service.get_history_entries_for_user", return_value=[]), \
             patch("app.miniapp.service.get_active_payment_order_for_user", return_value={"status": object()}), \
             patch("app.miniapp.service.watchlists_collection") as watchlists, \
             patch("app.miniapp.service.serialize_order_public", side_effect=ValueError("bad order")):
            user_signals.return_value.find.return_value.sort.return_value.limit.side_effect = [[], []]
            user_signals.return_value.count_documents.return_value = 0
            watchlists.return_value.find_one.return_value = {"symbols": []}
            payload = build_dashboard_payload(user)
        self.assertIsNone(payload["active_payment_order"])


if __name__ == "__main__":
    unittest.main()
